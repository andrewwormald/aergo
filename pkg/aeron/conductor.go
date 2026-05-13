package aeron

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"
)

// Conductor manages the lifecycle of publications and subscriptions
// by communicating with the Aeron media driver via shared memory.
type Conductor struct {
	cnc              *MappedCnc
	proxy            *DriverProxy
	broadcastRecv    *CopyBroadcastReceiver
	clientID         int64
	driverTimeoutNs  int64
	keepaliveInterNs int64
	lastKeepaliveNs  int64

	heartbeatCounterId int32

	mu           sync.Mutex
	publications map[int64]*publicationState
	subscriptions map[int64]*subscriptionState
	errors       []error
}

type publicationState struct {
	correlationID    int64
	registrationID   int64
	channel          string
	streamID         int32
	logBuffersPath   string
	sessionID        int32
	posLimitCounterID int32
	channelStatusID  int32
	ready            bool
	logBuffers       *LogBuffers
}

type subscriptionState struct {
	correlationID int64
	channel       string
	streamID      int32
	channelStatusID int32
	ready         bool
	images        []*Image
}

// Image represents a subscription's connection to a publication.
type Image struct {
	SessionID          int32
	CorrelationID      int64
	LogBuffers         *LogBuffers
	SubscriberPos      int32 // counter ID for position tracking
	JoinPosition       int64
	SourceIdentity     string
	subscriberPosition int64 // current read position (internal)
}

// Context holds configuration for the client conductor.
type Context struct {
	AeronDir         string
	DriverTimeoutMs  int64
	KeepaliveInterMs int64
}

// DefaultContext returns sensible defaults.
func DefaultContext() Context {
	return Context{
		DriverTimeoutMs:  10000,
		KeepaliveInterMs: 500,
	}
}

// NewConductor creates a new conductor connected to the media driver.
func NewConductor(cfg Context) (*Conductor, error) {
	cnc, err := MapCnc(cfg.AeronDir)
	if err != nil {
		return nil, fmt.Errorf("map cnc: %w", err)
	}

	toDriverRB, err := NewManyToOneRingBuffer(cnc.ToDriverBuffer)
	if err != nil {
		return nil, fmt.Errorf("to-driver ring buffer: %w", err)
	}

	clientID := toDriverRB.NextCorrelationID()

	broadcastReceiver := NewBroadcastReceiver(cnc.ToClientsBuffer)
	copyReceiver := NewCopyBroadcastReceiver(broadcastReceiver)

	proxy := NewDriverProxy(toDriverRB, clientID)

	c := &Conductor{
		cnc:                cnc,
		proxy:              proxy,
		broadcastRecv:      copyReceiver,
		clientID:           clientID,
		driverTimeoutNs:    cfg.DriverTimeoutMs * 1_000_000,
		keepaliveInterNs:   cfg.KeepaliveInterMs * 1_000_000,
		heartbeatCounterId: -1,
		publications:       make(map[int64]*publicationState),
		subscriptions:      make(map[int64]*subscriptionState),
	}

	// Enable broadcast receiver debug logging
	copyReceiver.SetDebugLog(func(msgTypeID, length, recordOffset int32) {
		log.Printf("conductor: broadcast recv msgTypeID=0x%04x length=%d recordOffset=%d",
			msgTypeID, length, recordOffset)
	})

	return c, nil
}

// ClientID returns the unique client identifier.
func (c *Conductor) ClientID() int64 { return c.clientID }

// Close shuts down the conductor and releases resources.
func (c *Conductor) Close() error {
	c.proxy.ClientClose()

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pub := range c.publications {
		if pub.logBuffers != nil {
			pub.logBuffers.Close()
		}
	}
	for _, sub := range c.subscriptions {
		for _, img := range sub.images {
			if img.LogBuffers != nil {
				img.LogBuffers.Close()
			}
		}
	}

	return c.cnc.Close()
}

// AddPublication requests a new publication from the media driver.
// Returns a correlationID to track the request.
func (c *Conductor) AddPublication(channel string, streamID int32) int64 {
	corrID := c.proxy.AddPublication(channel, streamID)
	if corrID < 0 {
		return corrID
	}

	c.mu.Lock()
	c.publications[corrID] = &publicationState{
		correlationID: corrID,
		channel:       channel,
		streamID:      streamID,
	}
	c.mu.Unlock()

	return corrID
}

// AddSubscription requests a new subscription from the media driver.
func (c *Conductor) AddSubscription(channel string, streamID int32) int64 {
	rb := c.proxy.rb
	headBefore := rb.HeadPosition()

	corrID := c.proxy.AddSubscription(channel, streamID)
	if corrID < 0 {
		return corrID
	}

	log.Printf("conductor: AddSubscription corrID=%d channel=%q streamID=%d rbHead=%d->check",
		corrID, channel, streamID, headBefore)

	c.mu.Lock()
	c.subscriptions[corrID] = &subscriptionState{
		correlationID: corrID,
		channel:       channel,
		streamID:      streamID,
	}
	c.mu.Unlock()

	return corrID
}

// FindPublication returns the publication state for a correlationID.
// Returns nil if not ready yet.
func (c *Conductor) FindPublication(corrID int64) *publicationState {
	c.mu.Lock()
	defer c.mu.Unlock()
	pub := c.publications[corrID]
	if pub != nil && pub.ready {
		return pub
	}
	return nil
}

// FindSubscription returns the subscription state for a correlationID.
func (c *Conductor) FindSubscription(corrID int64) *subscriptionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	sub := c.subscriptions[corrID]
	if sub != nil && sub.ready {
		return sub
	}
	return nil
}

// DoWork processes driver responses and sends keepalives.
// Returns the number of work items processed.
func (c *Conductor) DoWork() int {
	workCount := c.broadcastRecv.Receive(c.onDriverMessage, 10)

	now := time.Now().UnixNano()
	if now-c.lastKeepaliveNs > c.keepaliveInterNs {
		rbHead := c.proxy.rb.HeadPosition()
		rbTail := c.proxy.rb.TailPosition()
		log.Printf("conductor: keepalive tick rbHead=%d rbTail=%d pending=%d heartbeatCounter=%d",
			rbHead, rbTail, rbTail-rbHead, c.heartbeatCounterId)
		c.proxy.SendClientKeepalive()

		// Find and update heartbeat counter (driver uses this for client liveness)
		if c.heartbeatCounterId < 0 {
			c.heartbeatCounterId = FindHeartbeatCounter(
				c.cnc.CounterMetadata, c.cnc.CounterValues, c.clientID)
			if c.heartbeatCounterId >= 0 {
				log.Printf("conductor: found heartbeat counter ID=%d for clientID=%d",
					c.heartbeatCounterId, c.clientID)
			}
		}
		if c.heartbeatCounterId >= 0 {
			UpdateHeartbeatCounter(c.cnc.CounterValues, c.heartbeatCounterId)
		}

		c.lastKeepaliveNs = now
	}

	return workCount
}

func (c *Conductor) onDriverMessage(msgTypeID int32, buffer []byte, offset, length int32) {
	log.Printf("conductor: onDriverMessage msgTypeID=0x%04x offset=%d length=%d bufLen=%d",
		msgTypeID, offset, length, len(buffer))

	switch msgTypeID {
	case RespOnPublication, RespOnExclusivePublication:
		c.onNewPublication(buffer[offset : offset+length])
	case RespOnSubscription:
		log.Printf("conductor: dispatching onSubscriptionReady (0x%04x)", msgTypeID)
		c.onSubscriptionReady(buffer[offset : offset+length])
	case RespOnError:
		c.onError(buffer[offset : offset+length])
	case RespOnAvailableImage:
		c.onAvailableImage(buffer[offset : offset+length])
	case RespOnUnavailableImage:
		c.onUnavailableImage(buffer[offset : offset+length])
	case RespOnCounter:
		log.Printf("conductor: counter ready (0x%04x)", msgTypeID)
	case RespOnUnavailableCounter:
		log.Printf("conductor: unavailable counter (0x%04x)", msgTypeID)
	case RespOnOperationSuccess:
		log.Printf("conductor: operation success (ignored)")
	case RespOnClientTimeout:
		log.Printf("conductor: client timeout (0x%04x)", msgTypeID)
	default:
		log.Printf("conductor: unknown msgTypeID=0x%04x", msgTypeID)
	}
}

func (c *Conductor) onNewPublication(msg []byte) {
	if len(msg) < 36 {
		log.Printf("conductor: onNewPublication too short: %d bytes", len(msg))
		return
	}
	// Java PublicationBuffersReadyFlyweight layout:
	//   offset 0:  correlationID              int64
	//   offset 8:  registrationID             int64
	//   offset 16: sessionID                  int32
	//   offset 20: streamID                   int32
	//   offset 24: publicationLimitCounterID  int32
	//   offset 28: channelStatusIndicatorID   int32
	//   offset 32: logFileLength              int32 (putStringAscii prefix)
	//   offset 36: logFileName                ASCII bytes
	corrID := int64(binary.LittleEndian.Uint64(msg[0:]))
	regID := int64(binary.LittleEndian.Uint64(msg[8:]))
	sessionID := int32(binary.LittleEndian.Uint32(msg[16:]))
	posLimitID := int32(binary.LittleEndian.Uint32(msg[24:]))
	channelStatusID := int32(binary.LittleEndian.Uint32(msg[28:]))
	logFileLen := int32(binary.LittleEndian.Uint32(msg[32:]))

	logFile := ""
	if logFileLen > 0 && len(msg) >= 36+int(logFileLen) {
		logFile = string(msg[36 : 36+logFileLen])
	}

	log.Printf("conductor: PUB_READY corrID=%d regID=%d sessionID=%d posLimit=%d channelStatus=%d logFileLen=%d logFile=%q msgLen=%d",
		corrID, regID, sessionID, posLimitID, channelStatusID, logFileLen, logFile, len(msg))

	c.mu.Lock()
	pub := c.publications[corrID]
	if pub != nil {
		pub.registrationID = regID
		pub.sessionID = sessionID
		pub.posLimitCounterID = posLimitID
		pub.channelStatusID = channelStatusID
		pub.logBuffersPath = logFile
		pub.ready = true
	}
	c.mu.Unlock()

	if logFile != "" {
		lb, err := MapLogBuffers(logFile)
		if err != nil {
			log.Printf("native: failed to map log buffers %s: %v", logFile, err)
		} else if pub != nil {
			c.mu.Lock()
			pub.logBuffers = lb
			c.mu.Unlock()
		}
	}
}

func (c *Conductor) onSubscriptionReady(msg []byte) {
	if len(msg) < 12 {
		return
	}
	corrID := int64(binary.LittleEndian.Uint64(msg[0:]))
	channelStatusID := int32(binary.LittleEndian.Uint32(msg[8:]))
	log.Printf("conductor: SUB_READY corrID=%d channelStatusID=%d msgLen=%d", corrID, channelStatusID, len(msg))

	c.mu.Lock()
	sub := c.subscriptions[corrID]
	if sub != nil {
		sub.channelStatusID = channelStatusID
		sub.ready = true
		log.Printf("conductor: SUB_READY matched! corrID=%d", corrID)
	} else {
		log.Printf("conductor: SUB_READY no match for corrID=%d (have %d subs)", corrID, len(c.subscriptions))
	}
	c.mu.Unlock()
}

func (c *Conductor) onAvailableImage(msg []byte) {
	if len(msg) < 40 {
		return
	}
	// Response layout:
	//   offset 0:  correlationID       int64
	//   offset 8:  sessionID           int32
	//   offset 12: subscriberPosID     int32
	//   offset 16: subscriptionRegID   int64
	//   offset 24: joinPosition        int64
	//   offset 32: logFileLength       int32
	//   offset 36: logFile             string
	//   after logFile: sourceIdentityLength int32 + sourceIdentity string
	corrID := int64(binary.LittleEndian.Uint64(msg[0:]))
	sessionID := int32(binary.LittleEndian.Uint32(msg[8:]))
	subPosID := int32(binary.LittleEndian.Uint32(msg[12:]))
	subRegID := int64(binary.LittleEndian.Uint64(msg[16:]))
	joinPos := int64(binary.LittleEndian.Uint64(msg[24:]))
	logFileLen := int32(binary.LittleEndian.Uint32(msg[32:]))
	logFile := ""
	if len(msg) >= 36+int(logFileLen) {
		logFile = string(msg[36 : 36+logFileLen])
	}
	_ = corrID

	lb, err := MapLogBuffers(logFile)
	if err != nil {
		log.Printf("native: failed to map image log buffers %s: %v", logFile, err)
		return
	}

	img := &Image{
		SessionID:     sessionID,
		CorrelationID: corrID,
		LogBuffers:    lb,
		SubscriberPos: subPosID,
		JoinPosition:  joinPos,
	}

	c.mu.Lock()
	sub := c.subscriptions[subRegID]
	if sub != nil {
		sub.images = append(sub.images, img)
	}
	c.mu.Unlock()
}

func (c *Conductor) onUnavailableImage(msg []byte) {
	if len(msg) < 24 {
		return
	}
	corrID := int64(binary.LittleEndian.Uint64(msg[0:]))
	subRegID := int64(binary.LittleEndian.Uint64(msg[16:]))

	c.mu.Lock()
	sub := c.subscriptions[subRegID]
	if sub != nil {
		for i, img := range sub.images {
			if img.CorrelationID == corrID {
				if img.LogBuffers != nil {
					img.LogBuffers.Close()
				}
				sub.images = append(sub.images[:i], sub.images[i+1:]...)
				break
			}
		}
	}
	c.mu.Unlock()
}

func (c *Conductor) onError(msg []byte) {
	if len(msg) < 12 {
		return
	}
	corrID := int64(binary.LittleEndian.Uint64(msg[0:]))
	errCode := int32(binary.LittleEndian.Uint32(msg[8:]))
	errMsg := ""
	if len(msg) >= 16 {
		errMsgLen := int32(binary.LittleEndian.Uint32(msg[12:]))
		if len(msg) >= 16+int(errMsgLen) {
			errMsg = string(msg[16 : 16+errMsgLen])
		}
	}

	err := fmt.Errorf("driver error (corrID=%d, code=%d): %s", corrID, errCode, errMsg)
	c.mu.Lock()
	c.errors = append(c.errors, err)
	c.mu.Unlock()

	log.Printf("native: %v", err)
}

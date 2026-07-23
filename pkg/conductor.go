package aeron

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
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

	// terminated flips once when the client becomes closed-equivalent (the
	// driver timed out this client, or the driver itself went unresponsive).
	// Offers on this conductor's publications then return Closed and Add*
	// calls fail fast with fatalErr.
	terminated atomic.Bool

	mu            sync.Mutex
	publications  map[int64]*publicationState
	subscriptions map[int64]*subscriptionState
	errors        []error
	fatalErr      error
}

type publicationState struct {
	correlationID     int64
	registrationID    int64
	channel           string
	streamID          int32
	logBuffersPath    string
	sessionID         int32
	posLimitCounterID int32
	channelStatusID   int32
	ready             bool
	logBuffers        *LogBuffers
	// err records the driver's rejection (RespOnError) of the add command so
	// the pending AddPublication/AddExclusivePublication call fails with it.
	err error
}

type subscriptionState struct {
	correlationID   int64
	channel         string
	streamID        int32
	channelStatusID int32
	ready           bool
	images          []*Image
	// err records the driver's rejection (RespOnError) of the add command so
	// the pending AddSubscription call fails with it.
	err error
}

// Image represents a subscription's connection to a publication.
type Image struct {
	SessionID          int32
	CorrelationID      int64
	LogBuffers         *LogBuffers
	SubscriberPos      int32 // driver-allocated subscriber position counter ID
	JoinPosition       int64
	SourceIdentity     string
	subscriberPosition int64 // current read position (internal)

	// counterValues is the driver's counter values buffer. The subscriber
	// position counter at SubscriberPos must be updated on every position
	// advance so the driver's flow control sees this subscriber consuming;
	// otherwise the publication stalls at joinPosition + termLength/2.
	counterValues *AtomicBuffer
}

// updatePosition records a new subscriber position and publishes it to the
// driver's subscriber position counter with an ordered store. A nil counter
// values buffer or negative counter ID (in-memory tests) skips the counter
// write; the internal position still advances.
func (img *Image) updatePosition(position int64) {
	img.subscriberPosition = position
	if img.counterValues != nil && img.SubscriberPos >= 0 {
		img.counterValues.PutInt64Ordered(img.SubscriberPos*CounterValueLength, position)
	}
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

// AddExclusivePublication requests a new exclusive publication from the
// media driver. Exclusive publications give the caller their own private
// log buffer (not shared with other publishers on the same channel/stream),
// which unlocks higher single-publisher throughput because the term
// position is uncontended. The response path is the same as a shared
// publication (the driver responds with RespOnExclusivePublication, which
// the conductor handles identically).
func (c *Conductor) AddExclusivePublication(channel string, streamID int32) int64 {
	corrID := c.proxy.AddExclusivePublication(channel, streamID)
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
	corrID := c.proxy.AddSubscription(channel, streamID)
	if corrID < 0 {
		return corrID
	}

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
		// Piggyback the driver-liveness check on the keepalive interval
		// (mirrors Java ClientConductor.checkLiveness): a stale driver
		// heartbeat terminates the conductor so Offers return Closed and
		// Add* calls fail fast instead of hanging.
		if !c.terminated.Load() {
			if err := c.checkDriverLiveness(time.Now().UnixMilli()); err != nil {
				c.terminate(err)
			}
		}

		c.proxy.SendClientKeepalive()

		if c.heartbeatCounterId < 0 {
			c.heartbeatCounterId = FindHeartbeatCounter(
				c.cnc.CounterMetadata, c.cnc.CounterValues, c.clientID)
		}
		if c.heartbeatCounterId >= 0 {
			UpdateHeartbeatCounter(c.cnc.CounterValues, c.heartbeatCounterId)
		}

		c.lastKeepaliveNs = now
	}

	return workCount
}

func (c *Conductor) onDriverMessage(msgTypeID int32, buffer []byte, offset, length int32) {
	switch msgTypeID {
	case RespOnPublication, RespOnExclusivePublication:
		c.onNewPublication(buffer[offset : offset+length])
	case RespOnSubscription:
		c.onSubscriptionReady(buffer[offset : offset+length])
	case RespOnError:
		c.onError(buffer[offset : offset+length])
	case RespOnAvailableImage:
		c.onAvailableImage(buffer[offset : offset+length])
	case RespOnUnavailableImage:
		c.onUnavailableImage(buffer[offset : offset+length])
	case RespOnCounter:
	case RespOnUnavailableCounter:
	case RespOnOperationSuccess:
	case RespOnClientTimeout:
		c.onClientTimeout(buffer[offset : offset+length])
	default:
	}
}

// onClientTimeout handles RespOnClientTimeout: the driver stopped receiving
// this client's keepalives and released its resources. The conductor becomes
// closed-equivalent (mirrors Java ClientConductor.onClientTimeout).
//
// Message layout (io.aeron.command.ClientTimeoutFlyweight):
//
//	offset 0: clientID int64
func (c *Conductor) onClientTimeout(msg []byte) {
	if len(msg) < 8 {
		return
	}
	clientID := int64(binary.LittleEndian.Uint64(msg[0:]))
	// The event is broadcast to all clients; only act on our own timeout
	// (mirrors Java DriverEventsAdapter's clientId filter).
	if clientID != c.clientID {
		return
	}
	c.terminate(&ClientTimeoutError{})
}

// terminate marks the conductor closed-equivalent with the fatal error that
// caused it. The first terminal error wins; later calls are no-ops.
func (c *Conductor) terminate(err error) {
	if !c.terminated.CompareAndSwap(false, true) {
		return
	}
	c.mu.Lock()
	c.fatalErr = err
	c.errors = append(c.errors, err)
	c.mu.Unlock()
	log.Printf("native: %v", err)
}

// isTerminated reports whether the conductor is closed-equivalent.
func (c *Conductor) isTerminated() bool {
	return c.terminated.Load()
}

// FatalError returns the terminal error that closed this conductor (driver
// timeout or client timeout), or nil while the conductor is healthy.
func (c *Conductor) FatalError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fatalErr
}

// pendingError returns the driver rejection recorded against a pending
// AddPublication/AddExclusivePublication/AddSubscription correlation ID,
// or nil when the command has not been rejected.
func (c *Conductor) pendingError(corrID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pub := c.publications[corrID]; pub != nil && pub.err != nil {
		return pub.err
	}
	if sub := c.subscriptions[corrID]; sub != nil && sub.err != nil {
		return sub.err
	}
	return nil
}

// checkDriverLiveness returns a DriverTimeoutError when the media driver's
// heartbeat timestamp is older than the driver timeout (mirrors Java
// ClientConductor.checkLiveness). A nil cnc (in-memory tests) always passes.
func (c *Conductor) checkDriverLiveness(nowMs int64) error {
	if c.cnc == nil {
		return nil
	}
	timeoutMs := c.driverTimeoutNs / 1_000_000
	heartbeatMs := c.cnc.DriverHeartbeat()
	if nowMs > heartbeatMs+timeoutMs {
		return &DriverTimeoutError{HeartbeatAgeMs: nowMs - heartbeatMs, TimeoutMs: timeoutMs}
	}
	return nil
}

func (c *Conductor) onNewPublication(msg []byte) {
	if len(msg) < 36 {
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

	c.mu.Lock()
	sub := c.subscriptions[corrID]
	if sub != nil {
		sub.channelStatusID = channelStatusID
		sub.ready = true
	}
	c.mu.Unlock()
}

func (c *Conductor) onAvailableImage(msg []byte) {
	if len(msg) < 32 {
		return
	}
	// Response layout (matches io.aeron.command.ImageBuffersReadyFlyweight):
	//   offset 0:  correlationID                int64
	//   offset 8:  sessionID                    int32
	//   offset 12: streamID                     int32
	//   offset 16: subscriptionRegistrationID   int64
	//   offset 24: subscriberPositionID         int32
	//   offset 28: logFileName length           int32
	//   offset 32: logFileName bytes            ASCII
	//   offset 32 + align(logFileNameLength, 4):
	//              sourceIdentity length        int32
	//              sourceIdentity bytes         ASCII
	corrID := int64(binary.LittleEndian.Uint64(msg[0:]))
	sessionID := int32(binary.LittleEndian.Uint32(msg[8:]))
	// streamID at offset 12 — not currently needed
	subRegID := int64(binary.LittleEndian.Uint64(msg[16:]))
	subPosID := int32(binary.LittleEndian.Uint32(msg[24:]))
	logFileLen := int32(binary.LittleEndian.Uint32(msg[28:]))
	logFile := ""
	if logFileLen > 0 && len(msg) >= 32+int(logFileLen) {
		logFile = string(msg[32 : 32+logFileLen])
	}

	// The source identity is a second length-prefixed ASCII string after the
	// log file name; its length prefix is int-aligned (mirrors Java
	// ImageBuffersReadyFlyweight.sourceIdentityOffset).
	sourceIdentity := ""
	srcOff := 32 + int(align(logFileLen, 4))
	if len(msg) >= srcOff+4 {
		srcLen := int32(binary.LittleEndian.Uint32(msg[srcOff:]))
		if srcLen > 0 && len(msg) >= srcOff+4+int(srcLen) {
			sourceIdentity = string(msg[srcOff+4 : srcOff+4+int(srcLen)])
		}
	}
	// The driver allocates a subscriber position counter per image and
	// initialises it to the stream join position. Adopt that as the initial
	// read position; Poll ordered-updates the counter as it consumes so the
	// driver's flow control tracks this subscriber (see Image.updatePosition).
	var counterValues *AtomicBuffer
	if c.cnc != nil {
		counterValues = c.cnc.CounterValues
	}
	joinPos := int64(0)
	if counterValues != nil && subPosID >= 0 {
		joinPos = counterValues.GetInt64Volatile(subPosID * CounterValueLength)
	}

	lb, err := MapLogBuffers(logFile)
	if err != nil {
		log.Printf("native: failed to map image log buffers %s: %v", logFile, err)
		return
	}

	img := &Image{
		SessionID:          sessionID,
		CorrelationID:      corrID,
		LogBuffers:         lb,
		SubscriberPos:      subPosID,
		JoinPosition:       joinPos,
		SourceIdentity:     sourceIdentity,
		subscriberPosition: joinPos,
		counterValues:      counterValues,
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

// onError handles RespOnError: the driver rejected a command. The error is
// recorded against the offending correlation ID so a pending Add* call fails
// with it, and accumulated in the conductor's error list.
//
// Message layout (io.aeron.command.ErrorResponseFlyweight):
//
//	offset 0:  offendingCommandCorrelationID int64
//	offset 8:  errorCode                     int32
//	offset 12: errorMessageLength            int32
//	offset 16: errorMessage                  ASCII bytes
func (c *Conductor) onError(msg []byte) {
	if len(msg) < 12 {
		return
	}
	corrID := int64(binary.LittleEndian.Uint64(msg[0:]))
	errCode := int32(binary.LittleEndian.Uint32(msg[8:]))
	errMsg := ""
	if len(msg) >= 16 {
		errMsgLen := int32(binary.LittleEndian.Uint32(msg[12:]))
		if errMsgLen > 0 && len(msg) >= 16+int(errMsgLen) {
			errMsg = string(msg[16 : 16+errMsgLen])
		}
	}

	err := &RegistrationError{CorrelationID: corrID, Code: errCode, Message: errMsg}
	c.mu.Lock()
	if pub := c.publications[corrID]; pub != nil {
		pub.err = err
	}
	if sub := c.subscriptions[corrID]; sub != nil {
		sub.err = err
	}
	c.errors = append(c.errors, err)
	c.mu.Unlock()

	log.Printf("native: %v", err)
}

package cluster

import (
	"fmt"
	"log"
	"runtime"
	"time"

	aeron "github.com/andrewwormald/aergo/pkg/aeron/native"
	"github.com/andrewwormald/aergo/pkg/codec/cluster"
	"github.com/andrewwormald/aergo/pkg/codec/sbe"
)

// Cluster is the interface for sending and receiving messages on an Aeron cluster.
type Cluster interface {
	Connect()
	Poll() int
	Offer(buf []byte) int64
	State() State
	GracefulClose()
	Close() error
	ClusterSessionId() int64
	LeadershipTermId() int64
	LeaderMemberId() int32
}

// State represents the cluster client connection state.
type State int

const (
	StateDisconnected State = iota
	StateCreateEgressSubscription
	StateAwaitSubscriptionConnected
	StateCreateIngressPublications
	StateAwaitPublicationConnected
	StateSendConnectRequest
	StateAwaitConnectReply
	StateConnected
	StateClosing
	StateClosed
)

const (
	DefaultIngressStreamId     = 101
	DefaultEgressStreamId      = 102
	DefaultEgressChannel       = "aeron:udp?endpoint=localhost:19876"
	DefaultKeepAliveIntervalMs = 1000
	DefaultConnectTimeoutMs    = 5000
	DefaultReconnectBackoffMs  = 1000
	DefaultMaxReconnectBackMs  = 30000
)

type ClusterMember struct {
	MemberId int32
	Endpoint string
}

type Config struct {
	IngressChannel        string
	IngressStreamId       int32
	EgressChannel         string
	EgressStreamId        int32
	Members               []ClusterMember
	Listener              EgressListener
	AeronDir              string
	KeepAliveIntervalMs   int64
	ConnectTimeoutMs      int64
	ReconnectBackoffMs    int64
	MaxReconnectBackoffMs int64
	MaxReconnectAttempts  int
	AutoReconnect         bool
	SendBufSize           int
	LockOSThread          bool
}

func DefaultConfig() Config {
	return Config{
		IngressStreamId:       DefaultIngressStreamId,
		EgressChannel:         DefaultEgressChannel,
		EgressStreamId:        DefaultEgressStreamId,
		Listener:              &NoopListener{},
		KeepAliveIntervalMs:   DefaultKeepAliveIntervalMs,
		ConnectTimeoutMs:      DefaultConnectTimeoutMs,
		ReconnectBackoffMs:    DefaultReconnectBackoffMs,
		MaxReconnectBackoffMs: DefaultMaxReconnectBackMs,
		AutoReconnect:         true,
		SendBufSize:           4096,
		LockOSThread:          true,
	}
}

var _ Cluster = (*AeronCluster)(nil)

type AeronCluster struct {
	cfg Config

	aeronClient *aeron.Aeron
	egressSub   *aeron.Subscription
	ingressPubs []*aeron.Publication

	leaderMemberId   int32
	leadershipTermId int64
	clusterSessionId int64
	correlationId    int64
	state            State
	lastKeepAliveMs  int64
	sendBuf          []byte
	connectStartMs   int64
	reconnectAttempts int
	reconnectBackoff  int64
	lastReconnectMs   int64
	osThreadLocked    bool
}

func New(cfg Config) (*AeronCluster, error) {
	ac, err := aeron.Connect(aeron.WithDir(cfg.AeronDir))
	if err != nil {
		return nil, fmt.Errorf("create aeron client: %w", err)
	}

	bufSize := cfg.SendBufSize
	if bufSize <= 0 {
		bufSize = 4096
	}

	return &AeronCluster{
		cfg:              cfg,
		aeronClient:      ac,
		state:            StateDisconnected,
		sendBuf:          make([]byte, bufSize),
		reconnectBackoff: cfg.ReconnectBackoffMs,
	}, nil
}

func (c *AeronCluster) Connect() {
	c.state = StateCreateEgressSubscription
	c.connectStartMs = time.Now().UnixMilli()
}

func (c *AeronCluster) Poll() int {
	if c.cfg.LockOSThread && !c.osThreadLocked {
		runtime.LockOSThread()
		c.osThreadLocked = true
	}

	// Process driver responses
	c.aeronClient.DoWork()

	switch c.state {
	case StateDisconnected:
		return c.tryReconnect()
	case StateCreateEgressSubscription:
		return c.createEgressSubscription()
	case StateAwaitSubscriptionConnected:
		return c.awaitSubscriptionConnected()
	case StateCreateIngressPublications:
		return c.createIngressPublications()
	case StateAwaitPublicationConnected:
		return c.awaitPublicationConnected()
	case StateSendConnectRequest:
		return c.sendConnectRequest()
	case StateAwaitConnectReply:
		return c.awaitConnectReply()
	case StateConnected:
		return c.pollConnected()
	case StateClosing:
		return c.pollClosing()
	}
	return 0
}

func (c *AeronCluster) Offer(buf []byte) int64 {
	if c.state != StateConnected {
		return -1
	}

	smh := cluster.SessionMessageHeader{
		LeadershipTermId: c.leadershipTermId,
		ClusterSessionId: c.clusterSessionId,
	}
	n := smh.Encode(c.sendBuf, 0)
	copy(c.sendBuf[n:], buf)

	pub := c.leaderPublication()
	if pub == nil {
		return -1
	}
	return pub.Offer(c.sendBuf[:n+len(buf)])
}

func (c *AeronCluster) GracefulClose() {
	if c.state == StateConnected {
		c.sendCloseRequest()
		c.state = StateClosing
	} else {
		c.state = StateClosed
	}
}

func (c *AeronCluster) State() State              { return c.state }
func (c *AeronCluster) LeaderMemberId() int32     { return c.leaderMemberId }
func (c *AeronCluster) ClusterSessionId() int64   { return c.clusterSessionId }
func (c *AeronCluster) LeadershipTermId() int64   { return c.leadershipTermId }

func (c *AeronCluster) Close() error {
	c.state = StateClosed
	if c.egressSub != nil {
		c.egressSub.Close()
	}
	for _, pub := range c.ingressPubs {
		pub.Close()
	}
	if c.osThreadLocked {
		runtime.UnlockOSThread()
		c.osThreadLocked = false
	}
	if c.aeronClient != nil {
		return c.aeronClient.Close()
	}
	return nil
}

// --- State machine ---

func (c *AeronCluster) createEgressSubscription() int {
	sub, err := c.aeronClient.AddSubscription(c.cfg.EgressChannel, c.cfg.EgressStreamId)
	if err != nil {
		log.Printf("aergo: failed to create egress subscription: %v", err)
		return 0
	}
	c.egressSub = sub
	c.state = StateAwaitSubscriptionConnected
	return 1
}

func (c *AeronCluster) awaitSubscriptionConnected() int {
	// With native client, subscription is ready when the conductor confirms it
	c.state = StateCreateIngressPublications
	return 1
}

func (c *AeronCluster) createIngressPublications() int {
	c.ingressPubs = make([]*aeron.Publication, 0, len(c.cfg.Members))
	for _, member := range c.cfg.Members {
		uri := fmt.Sprintf("aeron:udp?endpoint=%s", member.Endpoint)
		if c.cfg.IngressChannel != "" {
			uri = c.cfg.IngressChannel
		}
		pub, err := c.aeronClient.AddPublication(uri, c.cfg.IngressStreamId)
		if err != nil {
			log.Printf("aergo: failed to create ingress publication to member %d: %v", member.MemberId, err)
			continue
		}
		c.ingressPubs = append(c.ingressPubs, pub)
	}
	if len(c.ingressPubs) == 0 {
		log.Printf("aergo: no ingress publications created")
		c.handleDisconnect("no ingress publications")
		return 0
	}
	c.state = StateAwaitPublicationConnected
	return 1
}

func (c *AeronCluster) awaitPublicationConnected() int {
	for _, pub := range c.ingressPubs {
		if pub.IsConnected() {
			c.state = StateSendConnectRequest
			return 1
		}
	}
	if c.isConnectTimedOut() {
		log.Printf("aergo: publication connect timeout")
		c.handleDisconnect("publication connect timeout")
	}
	return 0
}

func (c *AeronCluster) sendConnectRequest() int {
	c.correlationId = time.Now().UnixNano()

	req := cluster.SessionConnectRequest{
		CorrelationId:    c.correlationId,
		ResponseStreamId: c.cfg.EgressStreamId,
		Version:          int32(cluster.SchemaVersion),
		ResponseChannel:  c.cfg.EgressChannel,
	}
	n := req.Encode(c.sendBuf, 0)

	for _, pub := range c.ingressPubs {
		if pub.IsConnected() {
			result := pub.Offer(c.sendBuf[:n])
			if result > 0 {
				c.state = StateAwaitConnectReply
				return 1
			}
		}
	}
	return 0
}

func (c *AeronCluster) awaitConnectReply() int {
	workCount := 0
	c.egressSub.Poll(func(buffer []byte, header *aeron.Header) {
		if len(buffer) < sbe.HeaderSize {
			return
		}
		var hdr sbe.MessageHeader
		hdr.Decode(buffer, 0)

		if hdr.TemplateId == cluster.TemplateIdSessionEvent {
			var evt cluster.SessionEvent
			evt.DecodeWithBlockLength(buffer, sbe.HeaderSize, int(hdr.BlockLength))
			if evt.CorrelationId == c.correlationId {
				if evt.Code == cluster.EventCodeOK {
					c.clusterSessionId = evt.ClusterSessionId
					c.leaderMemberId = evt.LeaderMemberId
					c.leadershipTermId = evt.LeadershipTermId
					c.lastKeepAliveMs = time.Now().UnixMilli()
					c.state = StateConnected
					c.reconnectAttempts = 0
					c.reconnectBackoff = c.cfg.ReconnectBackoffMs
					log.Printf("aergo: connected session=%d leader=%d term=%d",
						c.clusterSessionId, c.leaderMemberId, c.leadershipTermId)
					c.cfg.Listener.OnSessionEvent(c, &evt)
				} else {
					log.Printf("aergo: connect rejected: code=%d detail=%s", evt.Code, evt.Detail)
					c.cfg.Listener.OnSessionEvent(c, &evt)
					c.handleDisconnect(fmt.Sprintf("rejected: %s", evt.Detail))
				}
			}
		}
		workCount++
	}, 10)

	if c.state == StateAwaitConnectReply && c.isConnectTimedOut() {
		log.Printf("aergo: connect reply timeout")
		c.handleDisconnect("connect reply timeout")
	}
	return workCount
}

func (c *AeronCluster) pollConnected() int {
	workCount := 0
	c.egressSub.Poll(func(buffer []byte, header *aeron.Header) {
		if len(buffer) < sbe.HeaderSize {
			return
		}
		var hdr sbe.MessageHeader
		hdr.Decode(buffer, 0)
		bodyOffset := sbe.HeaderSize

		switch hdr.TemplateId {
		case cluster.TemplateIdSessionMessageHeader:
			var smh cluster.SessionMessageHeader
			consumed := smh.DecodeWithBlockLength(buffer, bodyOffset, int(hdr.BlockLength))
			payloadOffset := bodyOffset + consumed
			payloadLen := len(buffer) - payloadOffset
			c.cfg.Listener.OnMessage(c, smh.Timestamp, buffer, payloadOffset, payloadLen)

		case cluster.TemplateIdSessionEvent:
			var evt cluster.SessionEvent
			evt.DecodeWithBlockLength(buffer, bodyOffset, int(hdr.BlockLength))
			c.cfg.Listener.OnSessionEvent(c, &evt)
			if evt.Code == cluster.EventCodeClosed {
				log.Printf("aergo: session closed by cluster: %s", evt.Detail)
				c.handleDisconnect("session closed by cluster")
			}

		case cluster.TemplateIdNewLeaderEvent:
			var evt cluster.NewLeaderEvent
			evt.DecodeWithBlockLength(buffer, bodyOffset, int(hdr.BlockLength))
			c.leaderMemberId = evt.LeaderMemberId
			c.leadershipTermId = evt.LeadershipTermId
			log.Printf("aergo: new leader: member=%d term=%d", evt.LeaderMemberId, evt.LeadershipTermId)
			c.cfg.Listener.OnNewLeader(c, &evt)

		case cluster.TemplateIdChallenge:
			var ch cluster.Challenge
			ch.DecodeWithBlockLength(buffer, bodyOffset, int(hdr.BlockLength))
			log.Printf("aergo: received challenge (correlationId=%d)", ch.CorrelationId)
			c.handleChallenge(&ch)

		default:
			c.cfg.Listener.OnMessage(c, 0, buffer, 0, len(buffer))
		}
		workCount++
	}, 10)

	nowMs := time.Now().UnixMilli()
	if nowMs-c.lastKeepAliveMs >= c.cfg.KeepAliveIntervalMs {
		c.sendKeepAlive()
		c.lastKeepAliveMs = nowMs
		workCount++
	}
	return workCount
}

func (c *AeronCluster) pollClosing() int {
	workCount := 0
	c.egressSub.Poll(func(buffer []byte, header *aeron.Header) {
		if len(buffer) < sbe.HeaderSize {
			return
		}
		var hdr sbe.MessageHeader
		hdr.Decode(buffer, 0)
		if hdr.TemplateId == cluster.TemplateIdSessionEvent {
			var evt cluster.SessionEvent
			evt.DecodeWithBlockLength(buffer, sbe.HeaderSize, int(hdr.BlockLength))
			if evt.Code == cluster.EventCodeClosed {
				log.Printf("aergo: graceful close acknowledged")
			}
		}
		workCount++
	}, 10)
	c.state = StateClosed
	return workCount
}

func (c *AeronCluster) sendCloseRequest() {
	req := cluster.SessionCloseRequest{
		ClusterSessionId: c.clusterSessionId,
		LeadershipTermId: c.leadershipTermId,
	}
	n := req.Encode(c.sendBuf, 0)
	if pub := c.leaderPublication(); pub != nil {
		pub.Offer(c.sendBuf[:n])
		log.Printf("aergo: sent close request for session=%d", c.clusterSessionId)
	}
}

func (c *AeronCluster) handleChallenge(ch *cluster.Challenge) {
	responseData := c.cfg.Listener.OnChallenge(c, ch)
	if responseData == nil {
		log.Printf("aergo: challenge rejected by listener (no response data)")
		return
	}
	resp := cluster.ChallengeResponse{
		CorrelationId:    ch.CorrelationId,
		ClusterSessionId: ch.ClusterSessionId,
		ChallengeData:    responseData,
	}
	n := resp.Encode(c.sendBuf, 0)
	if pub := c.leaderPublication(); pub != nil {
		result := pub.Offer(c.sendBuf[:n])
		if result > 0 {
			log.Printf("aergo: sent challenge response (correlationId=%d)", ch.CorrelationId)
		} else {
			log.Printf("aergo: failed to send challenge response: %d", result)
		}
	}
}

func (c *AeronCluster) sendKeepAlive() {
	ka := cluster.SessionKeepAlive{
		LeadershipTermId: c.leadershipTermId,
		ClusterSessionId: c.clusterSessionId,
	}
	n := ka.Encode(c.sendBuf, 0)
	if pub := c.leaderPublication(); pub != nil {
		pub.Offer(c.sendBuf[:n])
	}
}

func (c *AeronCluster) handleDisconnect(reason string) {
	log.Printf("aergo: disconnected: %s", reason)
	if c.egressSub != nil {
		c.egressSub.Close()
		c.egressSub = nil
	}
	for _, pub := range c.ingressPubs {
		pub.Close()
	}
	c.ingressPubs = nil

	if c.cfg.AutoReconnect {
		if c.cfg.MaxReconnectAttempts > 0 && c.reconnectAttempts >= c.cfg.MaxReconnectAttempts {
			log.Printf("aergo: max reconnect attempts (%d) reached", c.cfg.MaxReconnectAttempts)
			c.state = StateClosed
			return
		}
		c.state = StateDisconnected
		c.lastReconnectMs = time.Now().UnixMilli()
	} else {
		c.state = StateClosed
	}
}

func (c *AeronCluster) tryReconnect() int {
	if !c.cfg.AutoReconnect {
		return 0
	}
	nowMs := time.Now().UnixMilli()
	if nowMs-c.lastReconnectMs < c.reconnectBackoff {
		return 0
	}
	c.reconnectAttempts++
	log.Printf("aergo: reconnect attempt %d (backoff=%dms)", c.reconnectAttempts, c.reconnectBackoff)
	c.reconnectBackoff = c.reconnectBackoff * 2
	if c.reconnectBackoff > c.cfg.MaxReconnectBackoffMs {
		c.reconnectBackoff = c.cfg.MaxReconnectBackoffMs
	}
	c.connectStartMs = time.Now().UnixMilli()
	c.state = StateCreateEgressSubscription
	return 1
}

func (c *AeronCluster) isConnectTimedOut() bool {
	return time.Now().UnixMilli()-c.connectStartMs > c.cfg.ConnectTimeoutMs
}

func (c *AeronCluster) leaderPublication() *aeron.Publication {
	if len(c.ingressPubs) == 0 {
		return nil
	}
	idx := int(c.leaderMemberId)
	if idx >= 0 && idx < len(c.ingressPubs) {
		return c.ingressPubs[idx]
	}
	for _, pub := range c.ingressPubs {
		if pub.IsConnected() {
			return pub
		}
	}
	return nil
}

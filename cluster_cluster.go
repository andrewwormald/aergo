package aergo

import (
	"fmt"
	"runtime"
	"time"
)

// Cluster is the interface for sending and receiving messages on an Aeron cluster.
type Cluster interface {
	Connect()
	Poll() int
	Offer(buf []byte) int64
	State() ClusterState
	GracefulClose()
	Close() error
	ClusterSessionId() int64
	LeadershipTermId() int64
	LeaderMemberId() int32
}

// ClusterState represents the cluster client connection state.
type ClusterState int

const (
	StateDisconnected ClusterState = iota
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

// maxRedirectAttempts bounds how many times a single connect handshake will
// follow a REDIRECT before giving up, guarding against a pathological loop
// if members disagree about who the leader is.
const maxRedirectAttempts = 5

type ClusterMember struct {
	MemberId int32
	Endpoint string
}

type ClusterConfig struct {
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

func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
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
	cfg ClusterConfig

	aeronClient *Aeron
	egressSub   *Subscription
	ingressPubs []*Publication

	leaderMemberId    int32
	leadershipTermId  int64
	clusterSessionId  int64
	correlationId     int64
	state             ClusterState
	lastKeepAliveMs   int64
	sendBuf           []byte
	connectStartMs    int64
	reconnectAttempts int
	reconnectBackoff  int64
	lastReconnectMs   int64
	osThreadLocked    bool
	redirectAttempts  int
}

func NewCluster(cfg ClusterConfig) (*AeronCluster, error) {
	ac, err := Connect(WithDir(cfg.AeronDir))
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
	c.redirectAttempts = 0
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
		return NotConnected
	}

	smh := SessionMessageHeader{
		LeadershipTermId: c.leadershipTermId,
		ClusterSessionId: c.clusterSessionId,
	}
	n := smh.Encode(c.sendBuf, 0)
	copy(c.sendBuf[n:], buf)

	pub := c.leaderPublication()
	if pub == nil {
		return NotConnected
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

func (c *AeronCluster) State() ClusterState     { return c.state }
func (c *AeronCluster) LeaderMemberId() int32   { return c.leaderMemberId }
func (c *AeronCluster) ClusterSessionId() int64 { return c.clusterSessionId }
func (c *AeronCluster) LeadershipTermId() int64 { return c.leadershipTermId }

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
		c.cfg.Listener.OnError(c, fmt.Errorf("create egress subscription: %w", err))
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
	c.ingressPubs = make([]*Publication, 0, len(c.cfg.Members))
	for _, member := range c.cfg.Members {
		uri := fmt.Sprintf("aeron:udp?endpoint=%s", member.Endpoint)
		if c.cfg.IngressChannel != "" {
			uri = c.cfg.IngressChannel
		}
		pub, err := c.aeronClient.AddPublication(uri, c.cfg.IngressStreamId)
		if err != nil {
			c.cfg.Listener.OnError(c, fmt.Errorf("create ingress publication to member %d: %w", member.MemberId, err))
			continue
		}
		c.ingressPubs = append(c.ingressPubs, pub)
	}
	if len(c.ingressPubs) == 0 {
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
		c.handleDisconnect("publication connect timeout")
	}
	return 0
}

func (c *AeronCluster) sendConnectRequest() int {
	c.correlationId = time.Now().UnixNano()

	req := SessionConnectRequest{
		CorrelationId:      c.correlationId,
		ResponseStreamId:   c.cfg.EgressStreamId,
		Version:            ProtocolSemanticVersion,
		ResponseChannel:    c.cfg.EgressChannel,
		EncodedCredentials: nil,
	}
	n := req.Encode(c.sendBuf, 0)

	// leaderPublication prefers c.leaderMemberId when known (set by a prior
	// REDIRECT), falling back to the first connected member otherwise. The
	// leader isn't known on the very first attempt, so any member is asked
	// first, and later attempts target whoever REDIRECT actually named.
	if pub := c.leaderPublication(); pub != nil && pub.IsConnected() {
		result := pub.Offer(c.sendBuf[:n])
		if result > 0 {
			c.state = StateAwaitConnectReply
			return 1
		}
	}
	return 0
}

func (c *AeronCluster) awaitConnectReply() int {
	workCount := 0
	c.egressSub.Poll(func(buffer []byte, header *Header) {
		if len(buffer) < HeaderSize {
			return
		}
		var hdr MessageHeader
		hdr.Decode(buffer, 0)

		if hdr.TemplateId == TemplateIdSessionEvent {
			var evt SessionEvent
			evt.DecodeWithBlockLength(buffer, HeaderSize, int(hdr.BlockLength))
			if evt.CorrelationId == c.correlationId {
				if evt.Code == EventCodeOK {
					c.clusterSessionId = evt.ClusterSessionId
					c.leaderMemberId = evt.LeaderMemberId
					c.leadershipTermId = evt.LeadershipTermId
					c.lastKeepAliveMs = time.Now().UnixMilli()
					c.state = StateConnected
					c.reconnectAttempts = 0
					c.reconnectBackoff = c.cfg.ReconnectBackoffMs
					c.redirectAttempts = 0
					c.cfg.Listener.OnSessionEvent(c, &evt)
				} else if evt.Code == EventCodeRedirect && c.redirectAttempts < maxRedirectAttempts {
					// The member we asked isn't the leader; it names the
					// actual leader in LeaderMemberId. Retry against that
					// member instead of treating this as a rejection —
					// leaderPublication() (used by sendConnectRequest) picks
					// it up via c.leaderMemberId on the next attempt.
					c.redirectAttempts++
					c.leaderMemberId = evt.LeaderMemberId
					c.cfg.Listener.OnSessionEvent(c, &evt)
					c.state = StateSendConnectRequest
				} else {
					c.cfg.Listener.OnSessionEvent(c, &evt)
					c.handleDisconnect(fmt.Sprintf("rejected: %s", evt.Detail))
				}
			}
		}
		workCount++
	}, 10)

	if c.state == StateAwaitConnectReply && c.isConnectTimedOut() {
		c.handleDisconnect("connect reply timeout")
	}
	return workCount
}

func (c *AeronCluster) pollConnected() int {
	workCount := 0
	c.egressSub.Poll(func(buffer []byte, header *Header) {
		if len(buffer) < HeaderSize {
			return
		}
		var hdr MessageHeader
		hdr.Decode(buffer, 0)
		bodyOffset := HeaderSize

		switch hdr.TemplateId {
		case TemplateIdSessionMessageHeader:
			var smh SessionMessageHeader
			consumed := smh.DecodeWithBlockLength(buffer, bodyOffset, int(hdr.BlockLength))
			payloadOffset := bodyOffset + consumed
			payloadLen := len(buffer) - payloadOffset
			c.cfg.Listener.OnMessage(c, smh.Timestamp, buffer, payloadOffset, payloadLen)

		case TemplateIdSessionEvent:
			var evt SessionEvent
			evt.DecodeWithBlockLength(buffer, bodyOffset, int(hdr.BlockLength))
			c.cfg.Listener.OnSessionEvent(c, &evt)
			if evt.Code == EventCodeClosed {
				c.handleDisconnect("session closed by cluster")
			}

		case TemplateIdNewLeaderEvent:
			var evt NewLeaderEvent
			evt.DecodeWithBlockLength(buffer, bodyOffset, int(hdr.BlockLength))
			c.leaderMemberId = evt.LeaderMemberId
			c.leadershipTermId = evt.LeadershipTermId
			c.cfg.Listener.OnNewLeader(c, &evt)

		case TemplateIdChallenge:
			var ch Challenge
			ch.DecodeWithBlockLength(buffer, bodyOffset, int(hdr.BlockLength))
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
	c.egressSub.Poll(func(buffer []byte, header *Header) {
		if len(buffer) < HeaderSize {
			return
		}
		var hdr MessageHeader
		hdr.Decode(buffer, 0)
		if hdr.TemplateId == TemplateIdSessionEvent {
			var evt SessionEvent
			evt.DecodeWithBlockLength(buffer, HeaderSize, int(hdr.BlockLength))
			if evt.Code == EventCodeClosed {
				c.cfg.Listener.OnSessionEvent(c, &evt)
			}
		}
		workCount++
	}, 10)
	c.state = StateClosed
	return workCount
}

func (c *AeronCluster) sendCloseRequest() {
	req := SessionCloseRequest{
		ClusterSessionId: c.clusterSessionId,
		LeadershipTermId: c.leadershipTermId,
	}
	n := req.Encode(c.sendBuf, 0)
	if pub := c.leaderPublication(); pub != nil {
		pub.Offer(c.sendBuf[:n])
	}
}

func (c *AeronCluster) handleChallenge(ch *Challenge) {
	responseData := c.cfg.Listener.OnChallenge(c, ch)
	if responseData == nil {
		return
	}
	resp := ChallengeResponse{
		CorrelationId:    ch.CorrelationId,
		ClusterSessionId: ch.ClusterSessionId,
		ChallengeData:    responseData,
	}
	n := resp.Encode(c.sendBuf, 0)
	if pub := c.leaderPublication(); pub != nil {
		result := pub.Offer(c.sendBuf[:n])
		if result <= 0 {
			c.cfg.Listener.OnError(c, fmt.Errorf("send challenge response: offer result=%d", result))
		}
	}
}

func (c *AeronCluster) sendKeepAlive() {
	ka := SessionKeepAlive{
		LeadershipTermId: c.leadershipTermId,
		ClusterSessionId: c.clusterSessionId,
	}
	n := ka.Encode(c.sendBuf, 0)
	if pub := c.leaderPublication(); pub != nil {
		pub.Offer(c.sendBuf[:n])
	}
}

func (c *AeronCluster) handleDisconnect(reason string) {
	c.cfg.Listener.OnError(c, fmt.Errorf("disconnected: %s", reason))
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
			c.cfg.Listener.OnError(c, fmt.Errorf("max reconnect attempts (%d) reached, giving up", c.cfg.MaxReconnectAttempts))
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

func (c *AeronCluster) leaderPublication() *Publication {
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

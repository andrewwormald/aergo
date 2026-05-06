package cluster

import (
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/andrewwormald/aergo/pkg/aeron/client"
	"github.com/andrewwormald/aergo/pkg/codec/cluster"
	"github.com/andrewwormald/aergo/pkg/codec/sbe"
)

// Cluster is the interface for sending and receiving messages on an Aeron cluster.
// Consumers should depend on this interface rather than the concrete AeronCluster type.
type Cluster interface {
	Connect()
	Poll() int
	Offer(buf []byte) int64
	OfferWithBackpressure(buf []byte, strategy client.BackpressureStrategy, maxRetries int) int64
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
	StateClosing // graceful close in progress
	StateClosed
)

// Default Aeron cluster stream IDs.
const (
	DefaultIngressStreamId = 101
	DefaultEgressStreamId  = 102
)

const DefaultEgressChannel = "aeron:udp?endpoint=localhost:19876"
const DefaultKeepAliveIntervalMs = 1000
const DefaultConnectTimeoutMs = 5000
const DefaultReconnectBackoffMs = 1000
const DefaultMaxReconnectBackoffMs = 30000

// ClusterMember represents an Aeron cluster member endpoint.
type ClusterMember struct {
	MemberId int32
	Endpoint string
}

// Config holds cluster client configuration.
type Config struct {
	IngressChannel  string
	IngressStreamId int32
	EgressChannel   string
	EgressStreamId  int32
	Members         []ClusterMember
	Listener        EgressListener
	AeronDir        string

	KeepAliveIntervalMs    int64
	ConnectTimeoutMs       int64
	ReconnectBackoffMs     int64
	MaxReconnectBackoffMs  int64
	MaxReconnectAttempts   int // 0 = unlimited
	AutoReconnect          bool

	// SendBufSize is the size of the reusable send buffer.
	SendBufSize int

	// LockOSThread pins the poll loop to an OS thread.
	LockOSThread bool
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		IngressStreamId:       DefaultIngressStreamId,
		EgressChannel:         DefaultEgressChannel,
		EgressStreamId:        DefaultEgressStreamId,
		Listener:              &NoopListener{},
		KeepAliveIntervalMs:   DefaultKeepAliveIntervalMs,
		ConnectTimeoutMs:      DefaultConnectTimeoutMs,
		ReconnectBackoffMs:    DefaultReconnectBackoffMs,
		MaxReconnectBackoffMs: DefaultMaxReconnectBackoffMs,
		AutoReconnect:         true,
		SendBufSize:           4096,
		LockOSThread:          true,
	}
}

// Compile-time check that AeronCluster implements Cluster.
var _ Cluster = (*AeronCluster)(nil)

// AeronCluster is the main cluster client.
type AeronCluster struct {
	cfg Config

	aeronClient *client.Client

	// Egress subscription (cluster -> client)
	egressSub *client.Subscription

	// Ingress publications (client -> cluster), one per member
	ingressPubs []*client.Publication

	// Current leader
	leaderMemberId   int32
	leadershipTermId int64
	clusterSessionId int64

	// Correlation tracking
	correlationId int64

	// State machine
	state State

	// Keepalive
	lastKeepAliveMs int64

	// Send buffer (reusable, pre-allocated)
	sendBuf *client.BufferPool

	// Connect timing
	connectStartMs int64

	// Reconnection
	reconnectAttempts int
	reconnectBackoff  int64
	lastReconnectMs   int64

	// Thread pinning
	osThreadLocked bool
}

// New creates a new cluster client.
func New(cfg Config) (*AeronCluster, error) {
	opts := []client.Option{}
	if cfg.AeronDir != "" {
		opts = append(opts, client.WithDir(cfg.AeronDir))
	}

	ac, err := client.New(opts...)
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
		sendBuf:          client.NewBufferPool(4, bufSize),
		reconnectBackoff: cfg.ReconnectBackoffMs,
	}, nil
}

// Connect initiates the cluster connection. Call Poll() in a loop after this.
func (c *AeronCluster) Connect() {
	c.state = StateCreateEgressSubscription
	c.connectStartMs = time.Now().UnixMilli()
}

// Poll drives the cluster client state machine. Returns work count.
// Should be called from a single goroutine. Use Config.LockOSThread=true
// to pin to an OS thread (recommended for latency).
func (c *AeronCluster) Poll() int {
	if c.cfg.LockOSThread && !c.osThreadLocked {
		runtime.LockOSThread()
		c.osThreadLocked = true
	}

	workCount := 0

	switch c.state {
	case StateDisconnected:
		workCount += c.tryReconnect()
	case StateCreateEgressSubscription:
		workCount += c.createEgressSubscription()
	case StateAwaitSubscriptionConnected:
		workCount += c.awaitSubscriptionConnected()
	case StateCreateIngressPublications:
		workCount += c.createIngressPublications()
	case StateAwaitPublicationConnected:
		workCount += c.awaitPublicationConnected()
	case StateSendConnectRequest:
		workCount += c.sendConnectRequest()
	case StateAwaitConnectReply:
		workCount += c.awaitConnectReply()
	case StateConnected:
		workCount += c.pollConnected()
	case StateClosing:
		workCount += c.pollClosing()
	}

	return workCount
}

// Offer sends an application message to the cluster leader.
// Automatically prepends SessionMessageHeader.
func (c *AeronCluster) Offer(buf []byte) int64 {
	if c.state != StateConnected {
		return -1
	}

	sendBuf := c.sendBuf.Get()

	smh := cluster.SessionMessageHeader{
		LeadershipTermId: c.leadershipTermId,
		ClusterSessionId: c.clusterSessionId,
		Timestamp:        0,
	}
	n := smh.Encode(sendBuf, 0)
	copy(sendBuf[n:], buf)
	totalLen := n + len(buf)

	leaderPub := c.leaderPublication()
	if leaderPub == nil {
		return -1
	}

	return leaderPub.Offer(sendBuf[:totalLen])
}

// OfferWithBackpressure sends with configurable backpressure handling.
func (c *AeronCluster) OfferWithBackpressure(buf []byte, strategy client.BackpressureStrategy, maxRetries int) int64 {
	if c.state != StateConnected {
		return -1
	}

	sendBuf := c.sendBuf.Get()

	smh := cluster.SessionMessageHeader{
		LeadershipTermId: c.leadershipTermId,
		ClusterSessionId: c.clusterSessionId,
		Timestamp:        0,
	}
	n := smh.Encode(sendBuf, 0)
	copy(sendBuf[n:], buf)
	totalLen := n + len(buf)

	leaderPub := c.leaderPublication()
	if leaderPub == nil {
		return -1
	}

	return leaderPub.OfferWithBackpressure(sendBuf[:totalLen], strategy, maxRetries)
}

// GracefulClose initiates graceful shutdown by sending SessionCloseRequest.
// Poll() must continue to be called until State() == StateClosed.
func (c *AeronCluster) GracefulClose() {
	if c.state == StateConnected {
		c.sendCloseRequest()
		c.state = StateClosing
	} else {
		c.state = StateClosed
	}
}

// State returns the current connection state.
func (c *AeronCluster) State() State {
	return c.state
}

// LeaderMemberId returns the current leader member ID.
func (c *AeronCluster) LeaderMemberId() int32 {
	return c.leaderMemberId
}

// ClusterSessionId returns the established session ID.
func (c *AeronCluster) ClusterSessionId() int64 {
	return c.clusterSessionId
}

// LeadershipTermId returns the current leadership term ID.
func (c *AeronCluster) LeadershipTermId() int64 {
	return c.leadershipTermId
}

// Close immediately shuts down the cluster client.
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

// -- State machine steps ---------------------------------------------------

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
	if c.egressSub.IsConnected() || c.egressSub.ChannelStatus() > 0 {
		c.state = StateCreateIngressPublications
		return 1
	}
	if c.isConnectTimedOut() {
		log.Printf("aergo: subscription connect timeout")
		c.handleDisconnect("subscription connect timeout")
		return 0
	}
	return 0
}

func (c *AeronCluster) createIngressPublications() int {
	c.ingressPubs = make([]*client.Publication, 0, len(c.cfg.Members))
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
		return 0
	}
	return 0
}

func (c *AeronCluster) sendConnectRequest() int {
	c.correlationId = c.aeronClient.NextCorrelationId()

	req := cluster.SessionConnectRequest{
		CorrelationId:    c.correlationId,
		ResponseStreamId: c.cfg.EgressStreamId,
		Version:          int32(cluster.SchemaVersion),
		ResponseChannel:  c.cfg.EgressChannel,
	}

	sendBuf := c.sendBuf.Get()
	n := req.Encode(sendBuf, 0)

	for _, pub := range c.ingressPubs {
		if pub.IsConnected() {
			result := pub.Offer(sendBuf[:n])
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

	c.egressSub.Poll(func(buffer []byte, header *client.Header) {
		if len(buffer) < sbe.HeaderSize {
			return
		}

		var hdr sbe.MessageHeader
		hdr.Decode(buffer, 0)

		switch hdr.TemplateId {
		case cluster.TemplateIdSessionEvent:
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

	c.egressSub.Poll(func(buffer []byte, header *client.Header) {
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
			// Pass messages with unrecognized template IDs as raw bytes.
			c.cfg.Listener.OnMessage(c, 0, buffer, 0, len(buffer))
		}

		workCount++
	}, 10)

	// Keepalive
	nowMs := time.Now().UnixMilli()
	if nowMs-c.lastKeepAliveMs >= c.cfg.KeepAliveIntervalMs {
		c.sendKeepAlive()
		c.lastKeepAliveMs = nowMs
		workCount++
	}

	return workCount
}

func (c *AeronCluster) pollClosing() int {
	// Poll for close acknowledgment from cluster
	workCount := 0
	c.egressSub.Poll(func(buffer []byte, header *client.Header) {
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

	// Don't wait forever for ack
	c.state = StateClosed
	return workCount
}

// -- Graceful shutdown -----------------------------------------------------

func (c *AeronCluster) sendCloseRequest() {
	req := cluster.SessionCloseRequest{
		ClusterSessionId: c.clusterSessionId,
		LeadershipTermId: c.leadershipTermId,
	}
	sendBuf := c.sendBuf.Get()
	n := req.Encode(sendBuf, 0)
	pub := c.leaderPublication()
	if pub != nil {
		pub.Offer(sendBuf[:n])
		log.Printf("aergo: sent close request for session=%d", c.clusterSessionId)
	}
}

// -- Challenge-response authentication -------------------------------------

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

	sendBuf := c.sendBuf.Get()
	n := resp.Encode(sendBuf, 0)
	pub := c.leaderPublication()
	if pub != nil {
		result := pub.Offer(sendBuf[:n])
		if result > 0 {
			log.Printf("aergo: sent challenge response (correlationId=%d)", ch.CorrelationId)
		} else {
			log.Printf("aergo: failed to send challenge response: %d", result)
		}
	}
}

// -- Keepalive -------------------------------------------------------------

func (c *AeronCluster) sendKeepAlive() {
	ka := cluster.SessionKeepAlive{
		LeadershipTermId: c.leadershipTermId,
		ClusterSessionId: c.clusterSessionId,
	}
	sendBuf := c.sendBuf.Get()
	n := ka.Encode(sendBuf, 0)
	pub := c.leaderPublication()
	if pub != nil {
		pub.Offer(sendBuf[:n])
	}
}

// -- Reconnection ----------------------------------------------------------

func (c *AeronCluster) handleDisconnect(reason string) {
	log.Printf("aergo: disconnected: %s", reason)

	// Clean up existing resources
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

	// Exponential backoff with cap
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

// -- Leader publication selection ------------------------------------------

func (c *AeronCluster) leaderPublication() *client.Publication {
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

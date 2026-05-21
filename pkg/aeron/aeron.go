package aeron

import (
	"fmt"
	"log"
	"time"
)

// Client is the main entry point for connecting to an Aeron media driver.
// This is the pure Go implementation -- no C library required.
type Aeron struct {
	conductor *Conductor
	closed    bool
}

// publicationWaitTimeout is the deadline AddPublication and
// AddExclusivePublication wait for the driver response. Test code may
// override this to keep tests fast.
var publicationWaitTimeout = 15 * time.Second

// Option configures the Aeron client.
type ContextOption func(*Context)

// WithDir sets the Aeron media driver directory.
func WithDir(dir string) ContextOption {
	return func(ctx *Context) { ctx.AeronDir = dir }
}

// NewClient creates a new Aeron client connected to the media driver.
func Connect(opts ...ContextOption) (*Aeron, error) {
	cfg := DefaultContext()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.AeronDir == "" {
		return nil, fmt.Errorf("aeron directory not set")
	}

	conductor, err := NewConductor(cfg)
	if err != nil {
		return nil, err
	}

	return &Aeron{conductor: conductor}, nil
}

// AddPublication creates a publication for the given channel and stream.
func (c *Aeron) AddPublication(channel string, streamID int32) (*Publication, error) {
	corrID := c.conductor.AddPublication(channel, streamID)
	if corrID < 0 {
		return nil, fmt.Errorf("add publication failed")
	}

	deadline := time.Now().Add(publicationWaitTimeout)
	for time.Now().Before(deadline) {
		c.conductor.DoWork()
		if state := c.conductor.FindPublication(corrID); state != nil {
			c.tryFindHeartbeatCounter()
			return newPublication(c.conductor, state), nil
		}
		time.Sleep(time.Millisecond)
	}
	return nil, fmt.Errorf("publication timeout")
}

// AddExclusivePublication creates an exclusive publication for the given
// channel and stream. The returned publication has a private log buffer,
// so its term position is not contended with other publishers on the same
// channel/stream — required to get the same throughput as the standard
// Aeron Cluster Java client which uses exclusive ingress publications.
//
// On the driver side this is a different command (CmdAddExclusivePublication)
// but the response and Publication type are identical to AddPublication.
func (c *Aeron) AddExclusivePublication(channel string, streamID int32) (*Publication, error) {
	corrID := c.conductor.AddExclusivePublication(channel, streamID)
	if corrID < 0 {
		return nil, fmt.Errorf("add exclusive publication failed")
	}

	deadline := time.Now().Add(publicationWaitTimeout)
	for time.Now().Before(deadline) {
		c.conductor.DoWork()
		if state := c.conductor.FindPublication(corrID); state != nil {
			c.tryFindHeartbeatCounter()
			return newPublication(c.conductor, state), nil
		}
		time.Sleep(time.Millisecond)
	}
	return nil, fmt.Errorf("exclusive publication timeout")
}

// AddSubscription creates a subscription for the given channel and stream.
func (c *Aeron) AddSubscription(channel string, streamID int32) (*Subscription, error) {
	corrID := c.conductor.AddSubscription(channel, streamID)
	if corrID < 0 {
		return nil, fmt.Errorf("add subscription failed")
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		c.conductor.DoWork()
		if state := c.conductor.FindSubscription(corrID); state != nil {
			c.tryFindHeartbeatCounter()
			return newSubscription(c.conductor, corrID, state), nil
		}
		time.Sleep(time.Millisecond)
	}
	return nil, fmt.Errorf("subscription timeout")
}

// tryFindHeartbeatCounter searches for our heartbeat counter after the driver
// has registered our client (triggered by the first successful command).
func (c *Aeron) tryFindHeartbeatCounter() {
	cond := c.conductor
	if cond.heartbeatCounterId >= 0 {
		return // already found
	}
	cond.heartbeatCounterId = FindHeartbeatCounter(
		cond.cnc.CounterMetadata, cond.cnc.CounterValues, cond.clientID)
	if cond.heartbeatCounterId >= 0 {
		log.Printf("aeron: found heartbeat counter=%d for clientID=%d, updating immediately",
			cond.heartbeatCounterId, cond.clientID)
		UpdateHeartbeatCounter(cond.cnc.CounterValues, cond.heartbeatCounterId)
	}
}

// DoWork processes driver responses. Call this periodically.
func (c *Aeron) DoWork() int {
	return c.conductor.DoWork()
}

// Close shuts down the client and releases resources.
func (c *Aeron) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conductor.Close()
}

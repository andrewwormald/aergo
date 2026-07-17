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
// AddExclusivePublication wait for a driver response before giving up.
// Mutate only via PublicationWaitTimeoutForTesting.
var publicationWaitTimeout = 15 * time.Second

// PublicationWaitTimeoutForTesting overrides the wait deadline used by
// AddPublication and AddExclusivePublication when waiting for the
// driver's response. Returns the previous value so the caller can
// restore it (typically via t.Cleanup).
//
// Test-only — keep production code on the default 15 s.
func PublicationWaitTimeoutForTesting(d time.Duration) time.Duration {
	prev := publicationWaitTimeout
	publicationWaitTimeout = d
	return prev
}

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

	// Fail fast when the media driver is dead: a stale heartbeat means every
	// subsequent Add* would hang until its wait timeout.
	if err := conductor.checkDriverLiveness(time.Now().UnixMilli()); err != nil {
		if closeErr := conductor.Close(); closeErr != nil {
			log.Printf("aeron: close after failed liveness check: %v", closeErr)
		}
		return nil, err
	}

	return &Aeron{conductor: conductor}, nil
}

// preAddCheck fails fast before waiting on a driver response: a terminated
// conductor (the driver timed out this client) or a stale driver heartbeat
// means the wait could never succeed.
func (c *Aeron) preAddCheck() error {
	if err := c.conductor.FatalError(); err != nil {
		return err
	}
	return c.conductor.checkDriverLiveness(time.Now().UnixMilli())
}

// awaitError reports the first error that should abort a pending Add* wait:
// a driver rejection of the command (RespOnError) or a terminal client
// error (driver/client timeout).
func (c *Aeron) awaitError(corrID int64) error {
	if err := c.conductor.pendingError(corrID); err != nil {
		return err
	}
	return c.conductor.FatalError()
}

// AddPublication creates a publication for the given channel and stream.
func (c *Aeron) AddPublication(channel string, streamID int32) (*Publication, error) {
	if err := c.preAddCheck(); err != nil {
		return nil, err
	}

	corrID := c.conductor.AddPublication(channel, streamID)
	if corrID < 0 {
		return nil, fmt.Errorf("add publication failed")
	}

	deadline := time.Now().Add(publicationWaitTimeout)
	for time.Now().Before(deadline) {
		c.conductor.DoWork()
		if err := c.awaitError(corrID); err != nil {
			return nil, err
		}
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
	if err := c.preAddCheck(); err != nil {
		return nil, err
	}

	corrID := c.conductor.AddExclusivePublication(channel, streamID)
	if corrID < 0 {
		return nil, fmt.Errorf("add exclusive publication failed")
	}

	deadline := time.Now().Add(publicationWaitTimeout)
	for time.Now().Before(deadline) {
		c.conductor.DoWork()
		if err := c.awaitError(corrID); err != nil {
			return nil, err
		}
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
	if err := c.preAddCheck(); err != nil {
		return nil, err
	}

	corrID := c.conductor.AddSubscription(channel, streamID)
	if corrID < 0 {
		return nil, fmt.Errorf("add subscription failed")
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		c.conductor.DoWork()
		if err := c.awaitError(corrID); err != nil {
			return nil, err
		}
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

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

	a := &Aeron{conductor: conductor}

	// Warmup: send keepalives until the driver allocates our heartbeat counter.
	// This ensures the driver has registered our client before we send commands.
	log.Printf("aeron: warming up (waiting for driver to register client)...")
	warmupDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(warmupDeadline) {
		conductor.DoWork()
		if conductor.heartbeatCounterId >= 0 {
			log.Printf("aeron: driver registered our client (heartbeat counter=%d)", conductor.heartbeatCounterId)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if conductor.heartbeatCounterId < 0 {
		log.Printf("aeron: warning: heartbeat counter not found after warmup")
	}

	return a, nil
}

// AddPublication creates a publication for the given channel and stream.
func (c *Aeron) AddPublication(channel string, streamID int32) (*Publication, error) {
	corrID := c.conductor.AddPublication(channel, streamID)
	if corrID < 0 {
		return nil, fmt.Errorf("add publication failed")
	}

	// Poll for the driver response
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		c.conductor.DoWork()
		if state := c.conductor.FindPublication(corrID); state != nil {
			return newPublication(c.conductor, state), nil
		}
		time.Sleep(time.Millisecond)
	}
	return nil, fmt.Errorf("publication timeout")
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
			return newSubscription(c.conductor, corrID, state), nil
		}
		time.Sleep(time.Millisecond)
	}
	return nil, fmt.Errorf("subscription timeout")
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

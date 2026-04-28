package client

import (
	"runtime"
	"runtime/debug"
	"sync/atomic"
)

// Poller runs subscription polling on a dedicated, OS-thread-pinned goroutine
// with optional GC tuning for low-latency operation.
type Poller struct {
	sub       *Subscription
	handler   FragmentHandler
	limit     int
	running   atomic.Bool
	stopCh    chan struct{}
	doneCh    chan struct{}

	// Stats
	totalFragments atomic.Int64
	totalPolls     atomic.Int64
}

// PollerConfig configures the poller.
type PollerConfig struct {
	// FragmentLimit is the max fragments per poll call.
	FragmentLimit int
	// DisableGC disables the Go garbage collector on the poll thread.
	// The GC will only run when manually triggered or the poller stops.
	DisableGC bool
	// IdleStrategy is called when a poll returns 0 fragments.
	// If nil, the poller spins (lowest latency, highest CPU).
	IdleStrategy func(workCount int)
}

// DefaultPollerConfig returns a sensible default configuration.
func DefaultPollerConfig() PollerConfig {
	return PollerConfig{
		FragmentLimit: 10,
		DisableGC:     false,
		IdleStrategy:  nil, // spin
	}
}

// NewPoller creates a poller for the given subscription.
func NewPoller(sub *Subscription, handler FragmentHandler, cfg PollerConfig) *Poller {
	if cfg.FragmentLimit <= 0 {
		cfg.FragmentLimit = 10
	}
	return &Poller{
		sub:     sub,
		handler: handler,
		limit:   cfg.FragmentLimit,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// Start begins polling on a dedicated goroutine pinned to an OS thread.
func (p *Poller) Start(cfg PollerConfig) {
	if p.running.Load() {
		return
	}
	p.running.Store(true)

	go func() {
		// Pin this goroutine to an OS thread -- Aeron C client expects
		// single-threaded access per subscription.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer close(p.doneCh)

		// Optional: disable GC on this thread
		var oldGCPercent int32
		if cfg.DisableGC {
			oldGCPercent = int32(debug.SetGCPercent(-1))
		}
		defer func() {
			if cfg.DisableGC {
				debug.SetGCPercent(int(oldGCPercent))
			}
		}()

		for p.running.Load() {
			select {
			case <-p.stopCh:
				return
			default:
			}

			fragments := p.sub.Poll(p.handler, p.limit)
			p.totalPolls.Add(1)
			p.totalFragments.Add(int64(fragments))

			if fragments == 0 && cfg.IdleStrategy != nil {
				cfg.IdleStrategy(fragments)
			}
		}
	}()
}

// Stop signals the poller to stop and waits for it to finish.
func (p *Poller) Stop() {
	if !p.running.Load() {
		return
	}
	p.running.Store(false)
	close(p.stopCh)
	<-p.doneCh
}

// Stats returns total polls and fragments processed.
func (p *Poller) Stats() (polls, fragments int64) {
	return p.totalPolls.Load(), p.totalFragments.Load()
}

// ---------------------------------------------------------------------------
// Idle strategies
// ---------------------------------------------------------------------------

// IdleYield calls runtime.Gosched().
func IdleYield(_ int) {
	runtime.Gosched()
}

// IdleSpin does nothing (busy-wait).
func IdleSpin(_ int) {}

// IdleBackoff implements a backoff idle strategy.
// First N iterations spin, then yield, then sleep.
type IdleBackoff struct {
	spinCount  int
	yieldCount int
	iteration  int
}

// NewIdleBackoff creates a backoff strategy.
// Spins for spinCount iterations, yields for yieldCount, then calls runtime.Gosched().
func NewIdleBackoff(spinCount, yieldCount int) *IdleBackoff {
	return &IdleBackoff{
		spinCount:  spinCount,
		yieldCount: yieldCount,
	}
}

// Idle implements the backoff strategy.
func (b *IdleBackoff) Idle(workCount int) {
	if workCount > 0 {
		b.iteration = 0
		return
	}
	b.iteration++
	if b.iteration <= b.spinCount {
		return // spin
	}
	runtime.Gosched() // yield
}

// Reset resets the backoff state.
func (b *IdleBackoff) Reset() {
	b.iteration = 0
}

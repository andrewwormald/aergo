package aeron

import (
	"runtime"
	"time"
)

// IdleStrategy paces a busy-poll loop (Offer retry, Poll loop, duty cycle)
// so it doesn't spin the CPU at 100% while waiting for work. Mirrors Java
// Agrona's org.agrona.concurrent.IdleStrategy, the standard Aeron-ecosystem
// idiom for this problem.
type IdleStrategy interface {
	// Idle is called once per work-cycle with the amount of work done that
	// cycle. workCount > 0 resets the strategy to its most aggressive
	// (spinning) state. workCount == 0 applies and advances the current
	// backoff step.
	Idle(workCount int)

	// IdleNoWork is sugar for Idle(0) — no natural work-count available
	// (e.g. a failed Offer with no partial-progress notion).
	IdleNoWork()

	// Reset forces the strategy back to its most aggressive (spinning)
	// state, discarding any accumulated backoff.
	Reset()
}

// BackoffIdleStrategy mirrors Agrona's real state machine: SPINNING (pure
// busy counter, no OS yield) for maxSpins calls -> YIELDING
// (runtime.Gosched()) for maxYields calls -> PARKING (time.Sleep(parkPeriod),
// doubling each call, capped at maxParkPeriod). Any call with workCount > 0
// via Idle, or an explicit Reset(), zeroes the spin/yield counters and
// resets parkPeriod back to minParkPeriod.
type BackoffIdleStrategy struct {
	maxSpins      int
	maxYields     int
	minParkPeriod time.Duration
	maxParkPeriod time.Duration

	spins      int
	yields     int
	parkPeriod time.Duration
}

var _ IdleStrategy = (*BackoffIdleStrategy)(nil)

// NewBackoffIdleStrategy returns a BackoffIdleStrategy with Agrona's standard
// defaults: 10 spins, 5 yields, park doubling from 1μs to 1ms.
func NewBackoffIdleStrategy() *BackoffIdleStrategy {
	return NewBackoffIdleStrategyWithConfig(10, 5, 1*time.Microsecond, 1*time.Millisecond)
}

// NewBackoffIdleStrategyWithConfig allows overriding all four parameters.
func NewBackoffIdleStrategyWithConfig(maxSpins, maxYields int, minPark, maxPark time.Duration) *BackoffIdleStrategy {
	return &BackoffIdleStrategy{
		maxSpins:      maxSpins,
		maxYields:     maxYields,
		minParkPeriod: minPark,
		maxParkPeriod: maxPark,
		parkPeriod:    minPark,
	}
}

// Idle is called once per work-cycle with the amount of work done that
// cycle. workCount > 0 resets the strategy to its most aggressive
// (spinning) state. workCount == 0 applies and advances the current
// backoff step.
func (b *BackoffIdleStrategy) Idle(workCount int) {
	if workCount > 0 {
		b.Reset()
		return
	}
	b.IdleNoWork()
}

// IdleNoWork is sugar for Idle(0) — no natural work-count available (e.g. a
// failed Offer with no partial-progress notion).
func (b *BackoffIdleStrategy) IdleNoWork() {
	if b.spins < b.maxSpins {
		b.spins++
		return
	}
	if b.yields < b.maxYields {
		b.yields++
		runtime.Gosched()
		return
	}
	time.Sleep(b.parkPeriod)
	b.parkPeriod *= 2
	if b.parkPeriod > b.maxParkPeriod {
		b.parkPeriod = b.maxParkPeriod
	}
}

// Reset forces the strategy back to its most aggressive (spinning) state,
// discarding any accumulated backoff.
func (b *BackoffIdleStrategy) Reset() {
	b.spins = 0
	b.yields = 0
	b.parkPeriod = b.minParkPeriod
}

// NoOpIdleStrategy never idles — a pure busy-spin. Equivalent to today's
// OfferWithRetry behavior, given a name so callers can swap strategies
// without special-casing "no idle."
type NoOpIdleStrategy struct{}

var _ IdleStrategy = NoOpIdleStrategy{}

// Idle is a no-op.
func (NoOpIdleStrategy) Idle(workCount int) {}

// IdleNoWork is a no-op.
func (NoOpIdleStrategy) IdleNoWork() {}

// Reset is a no-op.
func (NoOpIdleStrategy) Reset() {}

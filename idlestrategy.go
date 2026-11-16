package aergo

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

	// sleep parks the goroutine for one park period. time.Sleep by default.
	sleep func(time.Duration)
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
		sleep:         time.Sleep,
	}
}

// NewPreciseBackoffIdleStrategy returns a BackoffIdleStrategy with Agrona's
// standard defaults (10 spins, 5 yields, park doubling from 1µs to 1ms) whose every
// park uses PreciseSleep. Use it for the one goroutine that polls for frames; see
// WithSleep for why, and for what it costs.
func NewPreciseBackoffIdleStrategy() *BackoffIdleStrategy {
	return NewBackoffIdleStrategy().WithSleep(PreciseSleep)
}

// NewPreciseBackoffIdleStrategyWithConfig is NewPreciseBackoffIdleStrategy with
// all four parameters set.
func NewPreciseBackoffIdleStrategyWithConfig(maxSpins, maxYields int, minPark, maxPark time.Duration) *BackoffIdleStrategy {
	return NewBackoffIdleStrategyWithConfig(maxSpins, maxYields, minPark, maxPark).WithSleep(PreciseSleep)
}

// WithSleep makes the park phase sleep with sleep instead of time.Sleep, and
// returns b. A strategy is not safe for concurrent use, so call it before the
// strategy is used.
//
// time.Sleep is the wrong sleep for a loop that is waiting for a frame. When no
// other goroutine is runnable, Go's idle thread waits in epoll_wait, whose
// timeout is in whole milliseconds, and the runtime rounds any wait shorter than
// 1ms up to 1ms. On Linux a time.Sleep of 64µs takes about 1.2ms, so a frame that
// arrives while the loop is parked waits that long to be seen. PreciseSleep does
// not have that floor.
func (b *BackoffIdleStrategy) WithSleep(sleep func(time.Duration)) *BackoffIdleStrategy {
	b.sleep = sleep
	return b
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
	b.sleep(b.parkPeriod)
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

// YieldingIdleStrategy yields the thread to the scheduler on every empty poll,
// and never sleeps. A frame waits only for one trip round the scheduler, and one
// core stays busy. It is Agrona's YieldingIdleStrategy.
type YieldingIdleStrategy struct{}

var _ IdleStrategy = YieldingIdleStrategy{}

// NewYieldingIdleStrategy returns a YieldingIdleStrategy.
func NewYieldingIdleStrategy() YieldingIdleStrategy { return YieldingIdleStrategy{} }

// Idle yields when workCount is 0, and does nothing otherwise.
func (YieldingIdleStrategy) Idle(workCount int) {
	if workCount == 0 {
		runtime.Gosched()
	}
}

// IdleNoWork yields.
func (YieldingIdleStrategy) IdleNoWork() { runtime.Gosched() }

// Reset is a no-op: there is no state.
func (YieldingIdleStrategy) Reset() {}

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

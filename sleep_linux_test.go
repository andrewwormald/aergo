package aergo

import (
	"sort"
	"testing"
	"time"
)

func TestPreciseSleep_DoesNotRoundAShortSleepUpToAMillisecond(t *testing.T) {
	const ask = 64 * time.Microsecond
	took := make([]time.Duration, 200)
	for i := range took {
		start := time.Now()
		PreciseSleep(ask)
		took[i] = time.Since(start)
	}
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
	median := took[len(took)/2]

	if median < ask {
		t.Errorf("median sleep = %v, shorter than the %v asked for", median, ask)
	}
	// time.Sleep takes about 1.2ms here, because the runtime rounds a wait of under
	// 1ms up to 1ms. The bound is well above what a precise sleep takes, so a slow
	// machine does not fail it, and well below that.
	if median > 900*time.Microsecond {
		t.Errorf("median sleep of %v for %v asked: it is rounded up to a millisecond", median, ask)
	}
}

func TestPreciseSleep_NonPositiveReturnsAtOnce(t *testing.T) {
	start := time.Now()
	PreciseSleep(0)
	PreciseSleep(-time.Second)
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("took %v", d)
	}
}

func TestBackoffIdleStrategy_ParksWithoutTheMillisecondFloorByDefault(t *testing.T) {
	const park = 64 * time.Microsecond
	b := NewBackoffIdleStrategyWithConfig(1, 1, park, park)
	b.IdleNoWork() // the spin
	b.IdleNoWork() // the yield

	took := make([]time.Duration, 100)
	for i := range took {
		start := time.Now()
		b.IdleNoWork() // a park
		took[i] = time.Since(start)
	}
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })

	// With time.Sleep the median is about 1.2ms, see the test above.
	if median := took[len(took)/2]; median > 900*time.Microsecond {
		t.Errorf("median park of %v for %v asked: the backoff is not using PreciseSleep by default", median, park)
	}
}

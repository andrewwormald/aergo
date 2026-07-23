package aeron

import (
	"testing"
	"time"
)

func TestBackoffIdleStrategy_StateTransitions(t *testing.T) {
	maxSpins := 3
	maxYields := 2
	minPark := 100 * time.Microsecond
	maxPark := 1 * time.Millisecond

	b := NewBackoffIdleStrategyWithConfig(maxSpins, maxYields, minPark, maxPark)

	// SPINNING phase: spins increments, yields stays 0, parkPeriod unused.
	for i := 1; i <= maxSpins; i++ {
		b.IdleNoWork()
		if b.spins != i {
			t.Errorf("spin %d: spins = %d, want %d", i, b.spins, i)
		}
		if b.yields != 0 {
			t.Errorf("spin %d: yields = %d, want 0", i, b.yields)
		}
		if b.parkPeriod != minPark {
			t.Errorf("spin %d: parkPeriod = %v, want %v", i, b.parkPeriod, minPark)
		}
	}

	// YIELDING phase: yields increments, spins stays at maxSpins.
	for i := 1; i <= maxYields; i++ {
		b.IdleNoWork()
		if b.spins != maxSpins {
			t.Errorf("yield %d: spins = %d, want %d", i, b.spins, maxSpins)
		}
		if b.yields != i {
			t.Errorf("yield %d: yields = %d, want %d", i, b.yields, i)
		}
	}

	// PARKING phase: the first call sleeps minPark, then doubles the
	// stored parkPeriod for the next call, capped at maxPark. So the
	// field's value *after* call i reflects the duration that will be
	// slept on call i+1.
	want := minPark * 2
	for i := 1; i <= 6; i++ {
		b.IdleNoWork()
		if b.parkPeriod != want {
			t.Errorf("park %d: parkPeriod = %v, want %v", i, b.parkPeriod, want)
		}
		want *= 2
		if want > maxPark {
			want = maxPark
		}
	}
}

func TestBackoffIdleStrategy_ResetOnWork(t *testing.T) {
	maxSpins := 3
	maxYields := 2
	minPark := 100 * time.Microsecond
	maxPark := 1 * time.Millisecond

	driveIntoParking := func() *BackoffIdleStrategy {
		b := NewBackoffIdleStrategyWithConfig(maxSpins, maxYields, minPark, maxPark)
		for i := 0; i < maxSpins+maxYields+2; i++ {
			b.IdleNoWork()
		}
		if b.parkPeriod <= minPark {
			t.Fatalf("expected parkPeriod to have grown past minPark, got %v", b.parkPeriod)
		}
		return b
	}

	t.Run("Idle(1)", func(t *testing.T) {
		b := driveIntoParking()
		b.Idle(1)
		if b.spins != 0 {
			t.Errorf("spins = %d, want 0", b.spins)
		}
		if b.yields != 0 {
			t.Errorf("yields = %d, want 0", b.yields)
		}
		if b.parkPeriod != minPark {
			t.Errorf("parkPeriod = %v, want %v", b.parkPeriod, minPark)
		}
	})

	t.Run("Reset()", func(t *testing.T) {
		b := driveIntoParking()
		b.Reset()
		if b.spins != 0 {
			t.Errorf("spins = %d, want 0", b.spins)
		}
		if b.yields != 0 {
			t.Errorf("yields = %d, want 0", b.yields)
		}
		if b.parkPeriod != minPark {
			t.Errorf("parkPeriod = %v, want %v", b.parkPeriod, minPark)
		}
	})
}

func TestNoOpIdleStrategy(t *testing.T) {
	var n NoOpIdleStrategy

	start := time.Now()
	for i := 0; i < 1000; i++ {
		n.Idle(0)
		n.Idle(5)
		n.IdleNoWork()
		n.Reset()
	}
	elapsed := time.Since(start)

	if elapsed > 5*time.Millisecond {
		t.Errorf("NoOpIdleStrategy took %v for 1000 iterations, want < 5ms", elapsed)
	}
}

func TestIdleStrategyImplementations(t *testing.T) {
	var _ IdleStrategy = (*BackoffIdleStrategy)(nil)
	var _ IdleStrategy = NoOpIdleStrategy{}
	var _ IdleStrategy = NewBackoffIdleStrategy()
}

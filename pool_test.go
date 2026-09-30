package aergo

import (
	"runtime"
	"sync"
	"testing"
)

func TestNewPool_PreFilled(t *testing.T) {
	p := NewPool(8, func() int { return 42 }, nil)
	if got := p.Len(); got != 8 {
		t.Errorf("Len = %d, want 8: an empty pool allocates on every Get until it warms", got)
	}
	if got := p.Cap(); got != 8 {
		t.Errorf("Cap = %d, want 8", got)
	}
	if got := p.Misses(); got != 0 {
		t.Errorf("Misses = %d before any Get, want 0", got)
	}
}

func TestNewPool_CapacityFloor(t *testing.T) {
	for _, c := range []int{0, -1, -100} {
		if got := NewPool(c, func() int { return 1 }, nil).Cap(); got != 1 {
			t.Errorf("NewPool(%d).Cap() = %d, want 1", c, got)
		}
	}
}

// TestPool_MissesCountsEmptyGets pins the observability: an undersized
// pool keeps working and quietly allocates, which is the failure it was
// added to prevent, so the miss has to be countable.
func TestPool_MissesCountsEmptyGets(t *testing.T) {
	p := NewPool(2, func() int { return 7 }, nil)
	p.Get()
	p.Get()
	if got := p.Misses(); got != 0 {
		t.Fatalf("Misses = %d while the pool still had values, want 0", got)
	}
	p.Get()
	p.Get()
	if got := p.Misses(); got != 2 {
		t.Errorf("Misses = %d after two Gets on an empty pool, want 2", got)
	}
}

func TestPool_PutDiscardsWhenFull(t *testing.T) {
	p := NewPool(2, func() int { return 0 }, nil)
	for i := 0; i < 100; i++ {
		p.Put(i)
	}
	if got := p.Len(); got != 2 {
		t.Errorf("Len = %d after over-filling, want the fixed capacity 2", got)
	}
}

func TestPool_PutAppliesReset(t *testing.T) {
	p := NewPool(2,
		func() *[]byte { b := make([]byte, 0, 8); return &b },
		func(b *[]byte) { *b = (*b)[:0] })
	b := p.Get()
	*b = append(*b, 1, 2, 3)
	p.Put(b)
	if got := p.Get(); len(*got) != 0 {
		t.Errorf("value came back with len %d, want 0 - reset was not applied", len(*got))
	}
}

// TestPool_GetIsLIFO matters for cache locality: the most recently
// released value is the most likely to still be warm.
func TestPool_GetIsLIFO(t *testing.T) {
	p := NewPool(4, func() string { return "new" }, nil)
	for p.Len() > 0 {
		p.Get()
	}
	p.Put("first")
	p.Put("second")
	if got := p.Get(); got != "second" {
		t.Errorf("Get = %q, want %q (most recently released)", got, "second")
	}
}

// TestPool_SurvivesGC is the reason the type exists rather than a
// sync.Pool.
func TestPool_SurvivesGC(t *testing.T) {
	p := NewPool(16, func() *[]byte { b := make([]byte, 0, 512); return &b }, nil)
	for i := 0; i < 5; i++ {
		runtime.GC()
	}
	if got := p.Len(); got != 16 {
		t.Errorf("Len = %d after 5 GCs, want 16 - the pool was reclaimed", got)
	}
}

// TestSyncPoolIsClearedByGC is the contrast, so the test above is not
// vacuously true. If the stdlib stops clearing, this reports it rather
// than leaving the premise unchecked.
func TestSyncPoolIsClearedByGC(t *testing.T) {
	var sp sync.Pool
	sp.New = func() any { return new(int) }
	sp.Put(new(int))
	runtime.GC()
	runtime.GC() // second cycle drops the victim cache

	built := 0
	sp.New = func() any { built++; return new(int) }
	_ = sp.Get()
	if built == 0 {
		t.Skip("sync.Pool retained its value across two GCs on this runtime; " +
			"Pool's advantage here is version-dependent, not absolute")
	}
}

// TestPool_GetReleasesReference stops a value the caller drops from
// staying reachable through the pool for ever.
func TestPool_GetReleasesReference(t *testing.T) {
	p := NewPool(1, func() *int { v := 0; return &v }, nil)
	p.Get()
	if len(p.free) != 0 {
		t.Fatalf("free len = %d after draining, want 0", len(p.free))
	}
	if p.free[:1][0] != nil {
		t.Error("pool still references the value it handed out")
	}
}

func TestPool_ZeroAllocSteadyState(t *testing.T) {
	p := NewPool(4, func() *[]byte { b := make([]byte, 0, 512); return &b }, nil)
	n := testing.AllocsPerRun(1000, func() { p.Put(p.Get()) })
	if n != 0 {
		t.Logf("allocs/op = %v (non-zero is expected under -race)", n)
	}
}

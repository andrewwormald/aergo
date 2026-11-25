package aergo

import "testing"

// BenchmarkConductorDoWork measures one idle conductor cycle: the driver
// broadcast receive, the keepalive interval check and the liveness check. It
// is the cost every poll loop pays per cycle, so it is the number to compare
// when DoWork gains locking.
func BenchmarkConductorDoWork(b *testing.B) {
	c, _ := newInMemConductor(42)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.DoWork()
	}
}

// BenchmarkConductorDoWorkSlowCaller is the same cycle for a caller that
// DoWork classes as slow, so the clock is read on every call. It is the cost
// of the path a low-rate caller takes, and the upper bound of the sampled one.
func BenchmarkConductorDoWorkSlowCaller(b *testing.B) {
	c, _ := newInMemConductor(42)
	c.keepaliveInterNs = 1 // any gap between calls counts as slow
	c.DoWork()
	c.DoWork()
	if c.clockEvery != 1 {
		b.Fatalf("clockEvery = %d, want 1", c.clockEvery)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.DoWork()
	}
}

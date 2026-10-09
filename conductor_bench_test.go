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

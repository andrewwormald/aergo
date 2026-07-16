package aeron

import "testing"

func BenchmarkBackoffIdleStrategy_Spinning(b *testing.B) {
	idle := NewBackoffIdleStrategy()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idle.Idle(1)
	}
}

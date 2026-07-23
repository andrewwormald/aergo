package aeron

import (
	"fmt"
	"testing"
)

func BenchmarkRingBufferWrite(b *testing.B) {
	sizes := []int{32, 256, 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			rb := newTestRingBuffer(1 << 20) // 1 MiB ring
			payload := make([]byte, size)
			headOff := rb.capacity + rbHeadPositionOffset
			headCacheOff := rb.capacity + rbHeadCachePositionOff
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !rb.Write(1, payload) {
					// Drain: advance head to current tail to free capacity.
					b.StopTimer()
					tail := rb.TailPosition()
					rb.buffer.PutInt64Ordered(headOff, tail)
					rb.buffer.PutInt64Ordered(headCacheOff, tail)
					b.StartTimer()
					rb.Write(1, payload)
				}
			}
		})
	}
}

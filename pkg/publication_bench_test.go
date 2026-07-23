package aeron

import (
	"fmt"
	"testing"
)

func BenchmarkPublicationOffer(b *testing.B) {
	sizes := []int{32, 256, 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			termLen := int32(64 * 1024)
			lb := newInMemLogBuffers(termLen)
			pub := newInMemPublication(lb, 1, 1001)
			payload := make([]byte, size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if pub.Offer(payload) < 0 {
					b.StopTimer()
					resetTermTail(lb, 0)
					zeroTerm(lb, 0, termLen)
					b.StartTimer()
				}
			}
		})
	}
}

package aergo

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
					resetLogBufferToGenesis(lb, termLen)
					b.StartTimer()
				}
			}
		})
	}
}

// BenchmarkPublicationOffer_BuildThenSend mirrors the real caller pattern
// this benchmark set exists to contrast with TryClaim against: the message
// content is assembled field-by-field into a freshly allocated buffer (as
// hydraclient's call()/offerInSession do today, building up a header+payload
// buffer before ever touching the publication), then handed to Offer, which
// copies it once more into the claimed log buffer region.
func BenchmarkPublicationOffer_BuildThenSend(b *testing.B) {
	sizes := []int{32, 256, 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			termLen := int32(64 * 1024)
			lb := newInMemLogBuffers(termLen)
			pub := newInMemPublication(lb, 1, 1001)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf := make([]byte, size) // simulates building the message off-buffer
				for j := range buf {
					buf[j] = byte(j)
				}
				if pub.Offer(buf) < 0 {
					b.StopTimer()
					resetLogBufferToGenesis(lb, termLen)
					b.StartTimer()
				}
			}
		})
	}
}

// BenchmarkPublicationTryClaim writes message content directly into the
// claimed log buffer region (no intermediate buffer, no extra copy) via
// TryClaim/Buffer/Commit - the zero-copy counterpart to
// BenchmarkPublicationOffer_BuildThenSend above, same payload sizes.
func BenchmarkPublicationTryClaim(b *testing.B) {
	sizes := []int{32, 256, 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			termLen := int32(64 * 1024)
			lb := newInMemLogBuffers(termLen)
			pub := newInMemPublication(lb, 1, 1001)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				claim, pos := pub.TryClaim(size)
				if pos < 0 {
					b.StopTimer()
					resetLogBufferToGenesis(lb, termLen)
					b.StartTimer()
					continue
				}
				dst := claim.Buffer()
				for j := range dst {
					dst[j] = byte(j)
				}
				claim.Commit()
			}
		})
	}
}

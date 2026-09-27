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
					resetTermTail(lb, 0)
					zeroTerm(lb, 0, termLen)
					b.StartTimer()
				}
			}
		})
	}
}

// resetToPartition0 rewinds the log buffer to a fresh, self-consistent state
// on partition 0 only (tail counter AND active term count both reset to 0,
// matching the buffer's original bootstrap state), so a benchmark loop can
// keep reusing the same partition indefinitely without ever exercising
// rotation across all three partitions. resetTermTail alone is not
// sufficient here: it only rewinds one partition's tail counter, and calling
// it while active term count has advanced past that partition's turn
// desyncs the two, which sends Offer/TryClaim into a permanent AdminAction
// livelock the moment active term count wraps back around to partition 0
// (confirmed by hand while benchmarking this change - not something this
// helper reintroduces).
func resetToPartition0(lb *LogBuffers, termLen int32) {
	lb.meta.PutInt32Ordered(MetaActiveTermCountOff, 0)
	resetTermTail(lb, 0)
	zeroTerm(lb, 0, termLen)
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
					resetToPartition0(lb, termLen)
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
					resetToPartition0(lb, termLen)
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

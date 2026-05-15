package aeron

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// BenchmarkEndToEndSendReceive measures the full per-message hotpath in one
// process: SBE-style header encode → Publication.Offer → Subscription.Poll →
// SBE-style header decode. The SBE work is inlined here (matching the
// MessageHeader + SessionMessageHeader binary layout from pkg/cluster) so the
// benchmark stays self-contained and does not create an import cycle.
func BenchmarkEndToEndSendReceive(b *testing.B) {
	sizes := []int{32, 256, 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			const sbeHeaderBytes = 8 + 24 // MessageHeader + SessionMessageHeader fixed block

			termLen := int32(64 * 1024)
			lb := newInMemLogBuffers(termLen)
			pub := newInMemPublication(lb, 1, 1001)
			sub := newInMemSubscription(lb, 1001)
			img := sub.conductor.subscriptions[sub.registrationID].images[0]

			frameLen := int32(DataFrameHeaderLen + sbeHeaderBytes + size)
			alignedFrame := align(frameLen, DataFrameHeaderLen)

			payload := make([]byte, sbeHeaderBytes+size)
			var decHdr struct {
				blockLength, templateID, schemaID, version uint16
				leadershipTermID, clusterSessionID, ts     int64
			}

			handler := func(buf []byte, h *Header) {
				decHdr.blockLength = binary.LittleEndian.Uint16(buf[0:])
				decHdr.templateID = binary.LittleEndian.Uint16(buf[2:])
				decHdr.schemaID = binary.LittleEndian.Uint16(buf[4:])
				decHdr.version = binary.LittleEndian.Uint16(buf[6:])
				decHdr.leadershipTermID = int64(binary.LittleEndian.Uint64(buf[8:]))
				decHdr.clusterSessionID = int64(binary.LittleEndian.Uint64(buf[16:]))
				decHdr.ts = int64(binary.LittleEndian.Uint64(buf[24:]))
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Reset if next frame would overflow the term.
				_, tail := lb.TermTailCounter(0)
				if tail+alignedFrame > termLen {
					b.StopTimer()
					resetTermTail(lb, 0)
					zeroTerm(lb, 0, termLen)
					img.subscriberPosition = 0
					b.StartTimer()
					_, tail = lb.TermTailCounter(0)
				}

				binary.LittleEndian.PutUint16(payload[0:], 24)
				binary.LittleEndian.PutUint16(payload[2:], 1)
				binary.LittleEndian.PutUint16(payload[4:], 111)
				binary.LittleEndian.PutUint16(payload[6:], 8)
				binary.LittleEndian.PutUint64(payload[8:], uint64(i))
				binary.LittleEndian.PutUint64(payload[16:], 42)
				binary.LittleEndian.PutUint64(payload[24:], uint64(i))

				writePos := pub.Offer(payload)
				if writePos < 0 {
					b.Fatalf("offer failed: %d", writePos)
				}

				// Position the subscription at the frame just written and poll it.
				img.subscriberPosition = int64(tail)
				if sub.Poll(handler, 1) != 1 {
					b.Fatalf("poll returned no fragments at tail=%d", tail)
				}
			}
			_ = decHdr
		})
	}
}

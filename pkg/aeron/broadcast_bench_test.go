package aeron

import (
	"encoding/binary"
	"testing"
)

// buildBroadcastBuffer fills a broadcast buffer with `count` records of the
// given msgTypeID and payload size, then sets trailer counters so a fresh
// receiver sees all of them as available.
func buildBroadcastBuffer(capacity int32, msgTypeID int32, payload []byte, count int) ([]byte, *AtomicBuffer, int64) {
	data := make([]byte, capacity+bcTrailerLength)
	buf := NewAtomicBuffer(data)

	recordLen := int32(RecordHeaderLength + len(payload))
	alignedLen := align(recordLen, RecordAlignment)

	offset := int32(0)
	for i := 0; i < count; i++ {
		if offset+alignedLen > capacity {
			break
		}
		binary.LittleEndian.PutUint32(data[offset:], uint32(recordLen))
		binary.LittleEndian.PutUint32(data[offset+4:], uint32(msgTypeID))
		copy(data[offset+RecordHeaderLength:], payload)
		offset += alignedLen
	}

	tailOff := capacity + bcTailCounterOff
	tailIntentOff := capacity + bcTailIntentCounterOff
	latestOff := capacity + bcLatestCounterOff
	buf.PutInt64Ordered(tailIntentOff, int64(offset))
	buf.PutInt64Ordered(tailOff, int64(offset))
	buf.PutInt64Ordered(latestOff, int64(offset-alignedLen))

	return data, buf, int64(offset)
}

func BenchmarkBroadcastReceiveNext(b *testing.B) {
	capacity := int32(64 * 1024)
	payload := make([]byte, 32)
	records := 128
	_, buf, tail := buildBroadcastBuffer(capacity, 7, payload, records)

	r := &BroadcastReceiver{buffer: buf, capacity: capacity, mask: capacity - 1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !r.ReceiveNext() {
			// Reset receiver cursor to drain again.
			r.cursor = 0
			r.nextRecord = 0
			r.recordOffset = 0
			if !r.ReceiveNext() {
				b.Fatal("receiver did not produce a record")
			}
		}
		_ = tail
	}
}

func BenchmarkCopyBroadcastReceive(b *testing.B) {
	capacity := int32(64 * 1024)
	payload := make([]byte, 32)
	records := 128
	_, buf, _ := buildBroadcastBuffer(capacity, 7, payload, records)

	r := &BroadcastReceiver{buffer: buf, capacity: capacity, mask: capacity - 1}
	cr := NewCopyBroadcastReceiver(r)
	noop := func(msgTypeID int32, buf []byte, offset, length int32) {}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := cr.Receive(noop, records)
		if n == 0 {
			r.cursor = 0
			r.nextRecord = 0
			r.recordOffset = 0
		}
	}
}

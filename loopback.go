package aergo

import (
	"math"
	"unsafe"
)

// This file provides loopback constructors for Publication/Subscription
// pairs backed by a heap-allocated log buffer instead of a live Aeron media
// driver connection. They exist for tests and benchmarks in downstream
// packages that want to exercise real wire-format encode/decode and
// claim/commit/poll mechanics — the exact cost their own client code adds —
// without paying for, or requiring, a live media driver process alongside
// the test.
//
// These bypass the driver conductor entirely: there is no flow control, no
// liveness/heartbeat tracking, and no real inter-process transport. Do not
// use them for anything other than local tests/benchmarks.

// NewLoopbackLogBuffers builds a heap-backed LogBuffers (no mmap, no Aeron
// media driver) with termLen-sized partitions, in the same bootstrap state
// MapLogBuffers leaves a freshly created stream in.
func NewLoopbackLogBuffers(termLen int32) *LogBuffers {
	total := int(3*termLen) + LogMetaDataLength
	data := make([]byte, total)

	// data left nil (not the heap slice) so Close() -- which calls
	// syscall.Munmap on a non-nil data field -- remains a safe no-op for a
	// loopback instance; heapData holds the only reference keeping the
	// backing array alive for the AtomicBuffers wrapping it.
	lb := &LogBuffers{heapData: data, termLen: termLen}
	for i := 0; i < PartitionCount; i++ {
		offset := int32(i) * termLen
		lb.terms[i] = WrapPtr(unsafe.Pointer(&data[offset]), termLen)
	}
	metaOff := int32(PartitionCount) * termLen
	lb.meta = WrapPtr(unsafe.Pointer(&data[metaOff]), LogMetaDataLength)

	lb.meta.PutInt32(MetaTermLenOff, termLen)
	lb.meta.PutInt32(MetaInitialTermIDOff, 0)
	lb.meta.PutInt32Ordered(MetaIsConnectedOff, 1)

	// Initialise the tail counters the way the media driver does: partition 0
	// carries the initial termID; the others carry the stale termID expected
	// by rotateLog (initialTermID + i - PartitionCount).
	const initialTermID = int32(0)
	for i := 1; i < PartitionCount; i++ {
		expectedTermID := initialTermID + int32(i) - PartitionCount
		lb.meta.PutInt64Ordered(int32(MetaTermTailCounterOff+i*8), packTail(expectedTermID, 0))
	}
	lb.meta.PutInt64Ordered(MetaTermTailCounterOff, packTail(initialTermID, 0))

	return lb
}

// NewLoopbackPublication builds a Publication that writes directly into lb,
// with no conductor and an unbounded flow-control window (position limit is
// stubbed to math.MaxInt64) — a real Publication for wire-format and
// claim/commit purposes, but with none of the driver-side backpressure or
// liveness behaviour a connected publication would have.
func NewLoopbackPublication(lb *LogBuffers, sessionID, streamID int32) *Publication {
	counterValues := NewAtomicBuffer(make([]byte, CounterValueLength))
	counterValues.PutInt64Ordered(0, math.MaxInt64)
	return &Publication{
		channel:           "aeron:ipc",
		streamID:          streamID,
		sessionID:         sessionID,
		logBuffers:        lb,
		initialTermID:     lb.InitialTermID(),
		posLimitCounterID: 0,
		counterValues:     counterValues,
	}
}

// NewLoopbackSubscription builds a Subscription that reads whatever a
// NewLoopbackPublication over the same lb writes, with no conductor and no
// media driver.
func NewLoopbackSubscription(lb *LogBuffers, streamID int32) *Subscription {
	c := &Conductor{
		publications:  map[int64]*publicationState{},
		subscriptions: map[int64]*subscriptionState{},
	}
	const corrID = int64(1)
	c.subscriptions[corrID] = &subscriptionState{
		correlationID: corrID,
		channel:       "aeron:ipc",
		streamID:      streamID,
		ready:         true,
		images: []*Image{{
			SessionID:  1,
			LogBuffers: lb,
		}},
	}
	return &Subscription{
		conductor:      c,
		channel:        "aeron:ipc",
		streamID:       streamID,
		registrationID: corrID,
	}
}

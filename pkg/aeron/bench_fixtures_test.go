package aeron

import (
	"unsafe"
)

// newInMemLogBuffers builds a LogBuffers backed by a heap-allocated slice
// (no mmap, no aeronmd). Used by benchmarks to exercise the publication and
// subscription hotpaths in isolation.
func newInMemLogBuffers(termLen int32) *LogBuffers {
	total := int(3*termLen) + LogMetaDataLength
	data := make([]byte, total)

	lb := &LogBuffers{data: nil, termLen: termLen} // data left nil so Close() is a no-op
	for i := 0; i < PartitionCount; i++ {
		offset := int32(i) * termLen
		lb.terms[i] = WrapPtr(unsafe.Pointer(&data[offset]), termLen)
	}
	metaOff := int32(PartitionCount) * termLen
	lb.meta = WrapPtr(unsafe.Pointer(&data[metaOff]), LogMetaDataLength)

	lb.meta.PutInt32(MetaTermLenOff, termLen)
	lb.meta.PutInt32(MetaInitialTermIDOff, 0)
	lb.meta.PutInt32Ordered(MetaIsConnectedOff, 1)

	// Keep backing slice alive for the lifetime of the LogBuffers by stashing
	// it on the (unused-by-tests) data field via a parallel reference.
	keepAlive[lb] = data
	return lb
}

var keepAlive = map[*LogBuffers][]byte{}

// resetTermTail zeroes the tail counter for partition 0 so the term can be
// re-filled in a benchmark loop without rotating partitions.
func resetTermTail(lb *LogBuffers, partIndex int) {
	off := int32(MetaTermTailCounterOff + partIndex*8)
	lb.meta.PutInt64Ordered(off, 0)
}

// zeroTerm zeroes out the first n bytes of a partition term so stale frame
// headers from a previous bench iteration do not get read as committed frames.
func zeroTerm(lb *LogBuffers, partIndex int, n int32) {
	t := lb.terms[partIndex]
	s := unsafe.Slice((*byte)(t.Ptr()), n)
	for i := range s {
		s[i] = 0
	}
}

// newInMemPublication builds a Publication that writes into the supplied
// in-memory LogBuffers. The conductor is left nil — Offer never touches it.
func newInMemPublication(lb *LogBuffers, sessionID, streamID int32) *Publication {
	return &Publication{
		channel:       "aeron:ipc",
		streamID:      streamID,
		sessionID:     sessionID,
		logBuffers:    lb,
		initialTermID: lb.InitialTermID(),
	}
}

// newInMemSubscription builds a Subscription with a single ready Image
// pointing at the supplied LogBuffers.
func newInMemSubscription(lb *LogBuffers, streamID int32) *Subscription {
	c := &Conductor{
		publications:  map[int64]*publicationState{},
		subscriptions: map[int64]*subscriptionState{},
	}
	corrID := int64(1)
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

package aeron

import (
	"math"
	"time"
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

	// Initialise the tail counters the way the media driver does: partition 0
	// carries the initial termID; the others carry the stale termID expected
	// by rotateLog (initialTermID + i - PartitionCount).
	const initialTermID = int32(0)
	for i := 1; i < PartitionCount; i++ {
		expectedTermID := initialTermID + int32(i) - PartitionCount
		lb.meta.PutInt64Ordered(int32(MetaTermTailCounterOff+i*8), packTail(expectedTermID, 0))
	}
	lb.meta.PutInt64Ordered(MetaTermTailCounterOff, packTail(initialTermID, 0))

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
// A single-counter values buffer stubs the driver's publication-limit
// counter; the limit defaults to unbounded (use setInMemPosLimit to change).
func newInMemPublication(lb *LogBuffers, sessionID, streamID int32) *Publication {
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

// setInMemPosLimit sets the stubbed publication-limit counter value.
func setInMemPosLimit(pub *Publication, limit int64) {
	pub.counterValues.PutInt64Ordered(pub.posLimitCounterID*CounterValueLength, limit)
}

// setInMemSubscriberPosCounter attaches a stubbed counter values buffer to
// the subscription's single image, mirroring the driver-allocated subscriber
// position counter. Returns the buffer so tests can assert the counter slot
// at counterID*CounterValueLength.
func setInMemSubscriberPosCounter(sub *Subscription, counterID int32) *AtomicBuffer {
	counterValues := NewAtomicBuffer(make([]byte, (counterID+1)*CounterValueLength))
	img := sub.conductor.subscriptions[sub.registrationID].images[0]
	img.SubscriberPos = counterID
	img.counterValues = counterValues
	return counterValues
}

// newInMemCnc builds a MappedCnc backed by heap buffers (no mmap, no
// aeronmd) with room for numCounters counters. The driver heartbeat (the
// to-driver ring buffer's consumer-heartbeat trailer field) starts at zero;
// doctor it with setInMemDriverHeartbeat.
func newInMemCnc(numCounters int32) *MappedCnc {
	return &MappedCnc{
		ToDriverBuffer:  NewAtomicBuffer(make([]byte, 1024+rbTrailerLength)),
		CounterMetadata: NewAtomicBuffer(make([]byte, numCounters*CounterMetadataLength)),
		CounterValues:   NewAtomicBuffer(make([]byte, numCounters*CounterValueLength)),
	}
}

// setInMemDriverHeartbeat doctors the driver keepalive timestamp (the
// to-driver ring buffer's consumer-heartbeat trailer field) read by
// MappedCnc.DriverHeartbeat.
func setInMemDriverHeartbeat(cnc *MappedCnc, timestampMs int64) {
	buf := cnc.ToDriverBuffer
	buf.PutInt64Ordered(buf.Capacity()-rbTrailerLength+rbConsumerHeartbeatOff, timestampMs)
}

// inMemBroadcast is a heap-backed to-clients broadcast buffer plus a
// single-threaded transmitter, standing in for the media driver's side of
// the broadcast protocol so tests can inject driver responses.
type inMemBroadcast struct {
	buf      *AtomicBuffer
	capacity int32
	tail     int64
}

func newInMemBroadcast(capacity int32) *inMemBroadcast {
	return &inMemBroadcast{
		buf:      NewAtomicBuffer(make([]byte, capacity+bcTrailerLength)),
		capacity: capacity,
	}
}

// transmit appends one record the way the driver's broadcast transmitter
// does: claim via the tail-intent counter, write the record, then publish
// the latest and tail counters.
func (b *inMemBroadcast) transmit(msgTypeID int32, payload []byte) {
	recordLen := int32(RecordHeaderLength + len(payload))
	alignedLen := align(recordLen, RecordAlignment)
	recordOffset := int32(b.tail) & (b.capacity - 1)

	b.buf.PutInt64Ordered(b.capacity+bcTailIntentCounterOff, b.tail+int64(alignedLen))
	b.buf.PutInt32(recordOffset, recordLen)
	b.buf.PutInt32(recordOffset+4, msgTypeID)
	b.buf.PutBytes(recordOffset+RecordHeaderLength, payload)
	b.buf.PutInt64Ordered(b.capacity+bcLatestCounterOff, b.tail)
	b.buf.PutInt64Ordered(b.capacity+bcTailCounterOff, b.tail+int64(alignedLen))

	b.tail += int64(alignedLen)
}

// newInMemConductor builds a Conductor wired to in-memory stand-ins for the
// driver interfaces: a cnc fixture (with a fresh heartbeat), a to-driver ring
// buffer, and a to-clients broadcast buffer whose transmitter is returned so
// tests can inject driver responses.
func newInMemConductor(clientID int64) (*Conductor, *inMemBroadcast) {
	bcast := newInMemBroadcast(1024)
	cnc := newInMemCnc(4)
	setInMemDriverHeartbeat(cnc, time.Now().UnixMilli())

	cfg := DefaultContext()
	return &Conductor{
		cnc:                cnc,
		proxy:              NewDriverProxy(newTestRingBuffer(4096), clientID),
		broadcastRecv:      NewCopyBroadcastReceiver(NewBroadcastReceiver(bcast.buf)),
		clientID:           clientID,
		driverTimeoutNs:    cfg.DriverTimeoutMs * 1_000_000,
		keepaliveInterNs:   cfg.KeepaliveInterMs * 1_000_000,
		heartbeatCounterId: -1,
		publications:       make(map[int64]*publicationState),
		subscriptions:      make(map[int64]*subscriptionState),
	}, bcast
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

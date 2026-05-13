package aeron

import "errors"

// Ring buffer record header layout.
const (
	RecordHeaderLength = 8 // int32 length + int32 type_id
	RecordAlignment    = 8
	PaddingMsgTypeID   = -1
)

// Ring buffer trailer layout (cache-line aligned offsets from end of buffer).
const (
	rbTrailerLength          = 12 * CacheLineLength
	rbTailPositionOffset     = int32(2 * CacheLineLength)
	rbHeadCachePositionOff   = int32(4 * CacheLineLength)
	rbHeadPositionOffset     = int32(6 * CacheLineLength)
	rbCorrelationCounterOff  = int32(8 * CacheLineLength)
	rbConsumerHeartbeatOff   = int32(10 * CacheLineLength)
)

// ManyToOneRingBuffer is a multiple-producer, single-consumer ring buffer
// used for sending commands to the Aeron media driver.
type ManyToOneRingBuffer struct {
	buffer   *AtomicBuffer
	capacity int32
}

// NewManyToOneRingBuffer wraps a buffer as an MPSC ring buffer.
func NewManyToOneRingBuffer(buf *AtomicBuffer) (*ManyToOneRingBuffer, error) {
	capacity := buf.Capacity() - int32(rbTrailerLength)
	if capacity <= 0 || !isPowerOfTwo(int(capacity)) {
		return nil, errors.New("ring buffer capacity must be a positive power of two")
	}
	return &ManyToOneRingBuffer{buffer: buf, capacity: capacity}, nil
}

// Write writes a message to the ring buffer.
func (rb *ManyToOneRingBuffer) Write(msgTypeID int32, src []byte) bool {
	recordLength := int32(RecordHeaderLength + len(src))
	alignedLength := align(recordLength, RecordAlignment)

	tailPos, ok := rb.claimCapacity(alignedLength)
	if !ok {
		return false
	}

	index := tailPos & (rb.capacity - 1)

	rb.buffer.PutInt32Ordered(index, -recordLength)
	rb.buffer.PutBytes(index+RecordHeaderLength, src)
	rb.buffer.PutInt32(index+4, msgTypeID)
	rb.buffer.PutInt32Ordered(index, recordLength)

	return true
}

func (rb *ManyToOneRingBuffer) claimCapacity(required int32) (int32, bool) {
	mask := rb.capacity - 1
	headCacheOff := rb.capacity + rbHeadCachePositionOff
	tailOff := rb.capacity + rbTailPositionOffset

	for {
		head := rb.buffer.GetInt64Volatile(headCacheOff)
		tail := rb.buffer.GetInt64Volatile(tailOff)
		available := rb.capacity - int32(tail-head)

		if required > available {
			head = rb.buffer.GetInt64Volatile(rb.capacity + rbHeadPositionOffset)
			rb.buffer.PutInt64Ordered(headCacheOff, head)
			available = rb.capacity - int32(tail-head)
			if required > available {
				return 0, false
			}
		}

		tailIndex := int32(tail) & mask
		padding := rb.capacity - tailIndex

		if required > padding {
			if rb.buffer.CompareAndSetInt64(tailOff, tail, tail+int64(padding)) {
				rb.buffer.PutInt32Ordered(tailIndex, -padding)
				rb.buffer.PutBytes(tailIndex+RecordHeaderLength, nil)
				rb.buffer.PutInt32(tailIndex+4, PaddingMsgTypeID)
				rb.buffer.PutInt32Ordered(tailIndex, padding)
				continue
			}
			continue
		}

		if rb.buffer.CompareAndSetInt64(tailOff, tail, tail+int64(required)) {
			return tailIndex, true
		}
	}
}

// NextCorrelationID generates a unique correlation ID.
func (rb *ManyToOneRingBuffer) NextCorrelationID() int64 {
	off := rb.capacity + rbCorrelationCounterOff
	return rb.buffer.GetAndAddInt64(off, 1)
}

// ConsumerHeartbeatTime returns the consumer's last heartbeat timestamp.
func (rb *ManyToOneRingBuffer) ConsumerHeartbeatTime() int64 {
	return rb.buffer.GetInt64Volatile(rb.capacity + rbConsumerHeartbeatOff)
}

// HeadPosition returns the consumer's current head position.
func (rb *ManyToOneRingBuffer) HeadPosition() int64 {
	return rb.buffer.GetInt64Volatile(rb.capacity + rbHeadPositionOffset)
}

// TailPosition returns the producer's current tail position.
func (rb *ManyToOneRingBuffer) TailPosition() int64 {
	return rb.buffer.GetInt64Volatile(rb.capacity + rbTailPositionOffset)
}

func align(value, alignment int32) int32 {
	return (value + alignment - 1) & ^(alignment - 1)
}

func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

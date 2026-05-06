package aeron

// Broadcast buffer trailer layout (single-producer to multiple-consumers).
const (
	bcTailIntentCounterOff = int32(0)
	bcTailCounterOff       = int32(CacheLineLength)
	bcLatestCounterOff     = int32(2 * CacheLineLength)
	bcTrailerLength        = int32(3 * CacheLineLength)
)

// BroadcastReceiver reads messages from the media driver's to-clients broadcast buffer.
// The driver is the single producer; multiple clients consume independently.
// If a client falls behind, it will detect lapping and skip to the latest message.
type BroadcastReceiver struct {
	buffer       *AtomicBuffer
	capacity     int32
	mask         int32
	cursor       int64
	nextRecord   int64
	recordOffset int32
	lappedCount  int64
}

// NewBroadcastReceiver wraps a buffer as a broadcast receiver.
func NewBroadcastReceiver(buf *AtomicBuffer) *BroadcastReceiver {
	capacity := buf.Capacity() - bcTrailerLength
	r := &BroadcastReceiver{
		buffer:   buf,
		capacity: capacity,
		mask:     capacity - 1,
	}
	// Initialize to latest position
	r.cursor = buf.GetInt64Volatile(capacity + bcLatestCounterOff)
	r.nextRecord = r.cursor
	return r
}

// LappedCount returns the number of times the receiver was lapped.
func (r *BroadcastReceiver) LappedCount() int64 { return r.lappedCount }

// ReceiveNext advances to the next available message.
// Returns true if a message is ready. Call MsgTypeID(), Offset(), Length()
// to access the current message, then call Validate() after reading.
func (r *BroadcastReceiver) ReceiveNext() bool {
	for {
		tail := r.buffer.GetInt64Volatile(r.capacity + bcTailCounterOff)
		if tail <= r.nextRecord {
			return false
		}

		r.recordOffset = int32(r.nextRecord) & r.mask

		if !r.validate(r.nextRecord) {
			r.lappedCount++
			r.nextRecord = r.buffer.GetInt64Volatile(r.capacity + bcLatestCounterOff)
			continue
		}

		length := r.buffer.GetInt32(r.recordOffset)
		if length <= 0 {
			return false
		}

		alignedLen := align(length, RecordAlignment)
		msgTypeID := r.buffer.GetInt32(r.recordOffset + 4)

		r.cursor = r.nextRecord
		r.nextRecord += int64(alignedLen)

		if msgTypeID == PaddingMsgTypeID {
			continue
		}

		return true
	}
}

// MsgTypeID returns the type ID of the current message.
func (r *BroadcastReceiver) MsgTypeID() int32 {
	return r.buffer.GetInt32(r.recordOffset + 4)
}

// Offset returns the payload offset of the current message.
func (r *BroadcastReceiver) Offset() int32 {
	return r.recordOffset + RecordHeaderLength
}

// Length returns the payload length of the current message.
func (r *BroadcastReceiver) Length() int32 {
	return r.buffer.GetInt32(r.recordOffset) - RecordHeaderLength
}

// Validate checks if the current record hasn't been overwritten since reading.
// Must be called after processing a message.
func (r *BroadcastReceiver) Validate() bool {
	return r.validate(r.cursor)
}

func (r *BroadcastReceiver) validate(cursor int64) bool {
	tailIntent := r.buffer.GetInt64Volatile(r.capacity + bcTailIntentCounterOff)
	return (cursor + int64(r.capacity)) > tailIntent
}

// CopyBroadcastReceiver wraps a BroadcastReceiver and copies each message
// to a scratch buffer, validating after copy to detect lapping.
type CopyBroadcastReceiver struct {
	receiver *BroadcastReceiver
	scratch  []byte
}

// NewCopyBroadcastReceiver creates a copying broadcast receiver.
func NewCopyBroadcastReceiver(receiver *BroadcastReceiver) *CopyBroadcastReceiver {
	return &CopyBroadcastReceiver{
		receiver: receiver,
		scratch:  make([]byte, 4096),
	}
}

// MessageHandler is called for each broadcast message received.
type MessageHandler func(msgTypeID int32, buffer []byte, offset, length int32)

// Receive polls for messages, copying each to a scratch buffer.
// Returns the number of messages received.
func (cr *CopyBroadcastReceiver) Receive(handler MessageHandler, limit int) int {
	count := 0
	r := cr.receiver

	for count < limit && r.ReceiveNext() {
		length := r.Length()
		if int(length) > len(cr.scratch) {
			cr.scratch = make([]byte, length)
		}

		msgTypeID := r.MsgTypeID()
		offset := r.Offset()
		r.buffer.GetBytes(offset, cr.scratch[:length])

		if r.Validate() {
			handler(msgTypeID, cr.scratch, 0, length)
			count++
		}
	}
	return count
}

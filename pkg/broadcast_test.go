package aeron

import (
	"encoding/binary"
	"testing"
)

func TestBroadcastReceiverInit(t *testing.T) {
	// Create a broadcast buffer with trailer
	capacity := int32(1024)
	data := make([]byte, capacity+bcTrailerLength)
	buf := NewAtomicBuffer(data)

	r := NewBroadcastReceiver(buf)
	if r.LappedCount() != 0 {
		t.Errorf("initial lapped count: got %d", r.LappedCount())
	}
}

func TestBroadcastReceiverNoMessages(t *testing.T) {
	capacity := int32(1024)
	data := make([]byte, capacity+bcTrailerLength)
	buf := NewAtomicBuffer(data)

	r := NewBroadcastReceiver(buf)
	if r.ReceiveNext() {
		t.Error("expected no message available")
	}
}

func TestCopyBroadcastReceiverNoMessages(t *testing.T) {
	capacity := int32(1024)
	data := make([]byte, capacity+bcTrailerLength)
	buf := NewAtomicBuffer(data)

	r := NewBroadcastReceiver(buf)
	cr := NewCopyBroadcastReceiver(r)

	count := cr.Receive(func(msgTypeID int32, buffer []byte, offset, length int32) {
		t.Error("should not be called")
	}, 10)

	if count != 0 {
		t.Errorf("count: got %d, want 0", count)
	}
}

func TestBroadcastReceiverSimulatedMessage(t *testing.T) {
	// Simulate what the media driver does: write a message into the broadcast buffer
	capacity := int32(1024)
	data := make([]byte, capacity+bcTrailerLength)
	buf := NewAtomicBuffer(data)

	// Write a message at offset 0
	msgTypeID := int32(7)
	payload := []byte("test broadcast")
	recordLen := int32(RecordHeaderLength + len(payload))
	alignedLen := align(recordLen, RecordAlignment)

	// Write record header
	binary.LittleEndian.PutUint32(data[0:], uint32(recordLen))
	binary.LittleEndian.PutUint32(data[4:], uint32(msgTypeID))
	copy(data[RecordHeaderLength:], payload)

	// Update trailer counters (simulate driver)
	tailOff := capacity + bcTailCounterOff
	tailIntentOff := capacity + bcTailIntentCounterOff
	latestOff := capacity + bcLatestCounterOff

	buf.PutInt64Ordered(tailIntentOff, int64(alignedLen))
	buf.PutInt64Ordered(tailOff, int64(alignedLen))
	buf.PutInt64Ordered(latestOff, 0) // latest record at offset 0

	// Create receiver starting at position 0
	r := &BroadcastReceiver{
		buffer:     buf,
		capacity:   capacity,
		mask:       capacity - 1,
		cursor:     0,
		nextRecord: 0,
	}

	if !r.ReceiveNext() {
		t.Fatal("expected message available")
	}

	if r.MsgTypeID() != msgTypeID {
		t.Errorf("msgTypeID: got %d, want %d", r.MsgTypeID(), msgTypeID)
	}
	if r.Length() != int32(len(payload)) {
		t.Errorf("length: got %d, want %d", r.Length(), len(payload))
	}
}

func TestLogBufferConstants(t *testing.T) {
	// Verify key constants match Aeron spec
	if DataFrameHeaderLen != 32 {
		t.Errorf("DataFrameHeaderLen: got %d, want 32", DataFrameHeaderLen)
	}
	if PartitionCount != 3 {
		t.Errorf("PartitionCount: got %d, want 3", PartitionCount)
	}
	if CacheLineLength != 64 {
		t.Errorf("CacheLineLength: got %d, want 64", CacheLineLength)
	}
}

func TestTermReader(t *testing.T) {
	// Create a term buffer with a single committed frame
	termLen := int32(4096)
	data := make([]byte, termLen)
	term := NewAtomicBuffer(data)

	payload := []byte("hello from term reader")
	frameLen := int32(DataFrameHeaderLen + len(payload))

	// Write a committed frame at offset 0
	term.PutInt32Ordered(0+FrameLengthOffset, frameLen) // committed (positive)
	term.PutUint8(0+FrameFlagsOffset, FlagUnfrag)
	term.PutInt32(0+FrameTypeOffset, FrameTypeData)
	term.PutInt32(0+FrameSessionIDOff, 42)
	term.PutInt32(0+FrameStreamIDOff, 1001)
	term.PutInt32(0+FrameTermIDOff, 0)
	term.PutBytes(0+DataFrameHeaderLen, payload)

	var received bool
	var receivedPayload []byte
	var receivedHeader *DataFrameHeader

	fragments, newOffset := ReadTerm(term, 0, func(buf *AtomicBuffer, offset, length int32, hdr *DataFrameHeader) {
		received = true
		receivedPayload = make([]byte, length)
		buf.GetBytes(offset, receivedPayload)
		receivedHeader = hdr
	}, 10)

	if !received {
		t.Fatal("handler not called")
	}
	if fragments != 1 {
		t.Errorf("fragments: got %d, want 1", fragments)
	}
	if newOffset <= 0 {
		t.Errorf("newOffset: got %d", newOffset)
	}
	if string(receivedPayload) != "hello from term reader" {
		t.Errorf("payload: got %q", receivedPayload)
	}
	if receivedHeader.SessionID != 42 {
		t.Errorf("sessionID: got %d", receivedHeader.SessionID)
	}
	if receivedHeader.StreamID != 1001 {
		t.Errorf("streamID: got %d", receivedHeader.StreamID)
	}
}

func TestTermReaderSkipsPadding(t *testing.T) {
	termLen := int32(4096)
	data := make([]byte, termLen)
	term := NewAtomicBuffer(data)

	// Write a padding frame at offset 0
	paddingLen := int32(64)
	term.PutInt32Ordered(0, paddingLen)
	term.PutInt32(0+FrameTypeOffset, int32(FrameTypePadding))

	// Write a data frame after padding
	payload := []byte("after padding")
	frameLen := int32(DataFrameHeaderLen + len(payload))
	term.PutInt32Ordered(paddingLen, frameLen)
	term.PutInt32(paddingLen+FrameTypeOffset, FrameTypeData)
	term.PutUint8(paddingLen+FrameFlagsOffset, FlagUnfrag)
	term.PutBytes(paddingLen+DataFrameHeaderLen, payload)

	var received string
	fragments, _ := ReadTerm(term, 0, func(buf *AtomicBuffer, offset, length int32, hdr *DataFrameHeader) {
		p := make([]byte, length)
		buf.GetBytes(offset, p)
		received = string(p)
	}, 10)

	if fragments != 1 {
		t.Errorf("fragments: got %d, want 1 (should skip padding)", fragments)
	}
	if received != "after padding" {
		t.Errorf("payload: got %q", received)
	}
}

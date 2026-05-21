package aeron

import (
	"encoding/binary"
	"testing"
)

func TestOnNewPublication(t *testing.T) {
	c := &Conductor{
		publications:  make(map[int64]*publicationState),
		subscriptions: make(map[int64]*subscriptionState),
	}

	corrID := int64(42)
	c.publications[corrID] = &publicationState{correlationID: corrID, channel: "test", streamID: 1}

	// Simulate driver response
	msg := make([]byte, 64)
	binary.LittleEndian.PutUint64(msg[0:], uint64(corrID))       // correlationID
	binary.LittleEndian.PutUint64(msg[8:], uint64(100))          // registrationID
	binary.LittleEndian.PutUint32(msg[16:], uint32(7))           // sessionID
	binary.LittleEndian.PutUint32(msg[20:], uint32(1))           // streamID
	binary.LittleEndian.PutUint32(msg[24:], uint32(5))           // posLimitCounterID
	binary.LittleEndian.PutUint32(msg[28:], uint32(3))           // channelStatusID
	binary.LittleEndian.PutUint32(msg[32:], uint32(0))           // logFileLength = 0 (no file)

	c.onNewPublication(msg)

	pub := c.FindPublication(corrID)
	if pub == nil {
		t.Fatal("publication not ready")
	}
	if pub.registrationID != 100 {
		t.Errorf("registrationID: got %d", pub.registrationID)
	}
	if pub.sessionID != 7 {
		t.Errorf("sessionID: got %d", pub.sessionID)
	}
}

func TestOnSubscriptionReady(t *testing.T) {
	c := &Conductor{
		publications:  make(map[int64]*publicationState),
		subscriptions: make(map[int64]*subscriptionState),
	}

	corrID := int64(99)
	c.subscriptions[corrID] = &subscriptionState{correlationID: corrID}

	msg := make([]byte, 16)
	binary.LittleEndian.PutUint64(msg[0:], uint64(corrID))
	binary.LittleEndian.PutUint32(msg[8:], uint32(4)) // channelStatusID

	c.onSubscriptionReady(msg)

	sub := c.FindSubscription(corrID)
	if sub == nil {
		t.Fatal("subscription not ready")
	}
	if sub.channelStatusID != 4 {
		t.Errorf("channelStatusID: got %d", sub.channelStatusID)
	}
}

func TestOnError(t *testing.T) {
	c := &Conductor{
		publications:  make(map[int64]*publicationState),
		subscriptions: make(map[int64]*subscriptionState),
	}

	errMsg := "test error"
	msg := make([]byte, 16+len(errMsg))
	binary.LittleEndian.PutUint64(msg[0:], uint64(1))             // correlationID
	binary.LittleEndian.PutUint32(msg[8:], uint32(42))            // errorCode
	binary.LittleEndian.PutUint32(msg[12:], uint32(len(errMsg)))  // msgLength
	copy(msg[16:], errMsg)

	c.onError(msg)

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.errors) != 1 {
		t.Fatalf("expected 1 error, got %d", len(c.errors))
	}
}

func TestOnDriverMessageDispatch(t *testing.T) {
	c := &Conductor{
		publications:  make(map[int64]*publicationState),
		subscriptions: make(map[int64]*subscriptionState),
	}

	corrID := int64(10)
	c.subscriptions[corrID] = &subscriptionState{correlationID: corrID}

	msg := make([]byte, 16)
	binary.LittleEndian.PutUint64(msg[0:], uint64(corrID))
	binary.LittleEndian.PutUint32(msg[8:], uint32(1))

	// Test dispatch to onSubscriptionReady
	c.onDriverMessage(RespOnSubscription, msg, 0, int32(len(msg)))

	sub := c.FindSubscription(corrID)
	if sub == nil {
		t.Fatal("subscription should be ready after dispatch")
	}

	// Test dispatch to onError
	errMsg := make([]byte, 16)
	binary.LittleEndian.PutUint64(errMsg[0:], uint64(1))
	binary.LittleEndian.PutUint32(errMsg[8:], uint32(1))

	c.onDriverMessage(RespOnError, errMsg, 0, int32(len(errMsg)))
	c.mu.Lock()
	errCount := len(c.errors)
	c.mu.Unlock()
	if errCount != 1 {
		t.Errorf("expected 1 error from dispatch")
	}

	// Test dispatch to operation success (no-op, shouldn't panic)
	c.onDriverMessage(RespOnOperationSuccess, msg, 0, int32(len(msg)))
}

func TestFindPublicationNotReady(t *testing.T) {
	c := &Conductor{
		publications:  make(map[int64]*publicationState),
		subscriptions: make(map[int64]*subscriptionState),
	}

	c.publications[1] = &publicationState{correlationID: 1, ready: false}
	if c.FindPublication(1) != nil {
		t.Error("should return nil when not ready")
	}
	if c.FindPublication(999) != nil {
		t.Error("should return nil for unknown corrID")
	}
}

func TestFindSubscriptionNotReady(t *testing.T) {
	c := &Conductor{
		publications:  make(map[int64]*publicationState),
		subscriptions: make(map[int64]*subscriptionState),
	}

	c.subscriptions[1] = &subscriptionState{correlationID: 1, ready: false}
	if c.FindSubscription(1) != nil {
		t.Error("should return nil when not ready")
	}
}

func TestOnUnavailableImage(t *testing.T) {
	c := &Conductor{
		publications:  make(map[int64]*publicationState),
		subscriptions: make(map[int64]*subscriptionState),
	}

	subRegID := int64(5)
	c.subscriptions[subRegID] = &subscriptionState{
		correlationID: subRegID,
		ready:         true,
		images: []*Image{
			{CorrelationID: 10, SessionID: 1},
			{CorrelationID: 20, SessionID: 2},
		},
	}

	msg := make([]byte, 24)
	binary.LittleEndian.PutUint64(msg[0:], uint64(10)) // correlationID of image
	binary.LittleEndian.PutUint64(msg[8:], uint64(0))
	binary.LittleEndian.PutUint64(msg[16:], uint64(subRegID))

	c.onUnavailableImage(msg)

	c.mu.Lock()
	defer c.mu.Unlock()
	sub := c.subscriptions[subRegID]
	if len(sub.images) != 1 {
		t.Errorf("expected 1 image remaining, got %d", len(sub.images))
	}
	if sub.images[0].CorrelationID != 20 {
		t.Errorf("wrong image remaining: corrID=%d", sub.images[0].CorrelationID)
	}
}

// TestConductorAddPublication verifies the shared path adds an entry to
// publications, returns a positive correlation ID, and routes through
// CmdAddPublication (sanity-check before the exclusive variant test).
func TestConductorAddPublication(t *testing.T) {
	rb := newTestRingBuffer(4096)
	c := &Conductor{
		proxy:         NewDriverProxy(rb, 42),
		publications:  make(map[int64]*publicationState),
		subscriptions: make(map[int64]*subscriptionState),
	}

	corrID := c.AddPublication("aeron:ipc", 101)
	if corrID < 0 {
		t.Fatalf("AddPublication returned %d", corrID)
	}

	c.mu.Lock()
	state, ok := c.publications[corrID]
	c.mu.Unlock()
	if !ok {
		t.Fatalf("publications map missing entry for corrID=%d", corrID)
	}
	if state.channel != "aeron:ipc" || state.streamID != 101 {
		t.Errorf("publication state wrong: channel=%q streamID=%d", state.channel, state.streamID)
	}

	// Verify the command that was written to the ring buffer was
	// CmdAddPublication, not CmdAddExclusivePublication.
	if cmd := peekFirstCommandType(t, rb); cmd != CmdAddPublication {
		t.Errorf("expected CmdAddPublication (0x%x), got 0x%x", CmdAddPublication, cmd)
	}
}

// TestConductorAddExclusivePublication verifies the exclusive variant
// adds an entry to the same publications map (so the response handler in
// the RespOnPublication / RespOnExclusivePublication branch can find it)
// and that the underlying driver command is CmdAddExclusivePublication
// rather than CmdAddPublication.
func TestConductorAddExclusivePublication(t *testing.T) {
	rb := newTestRingBuffer(4096)
	c := &Conductor{
		proxy:         NewDriverProxy(rb, 42),
		publications:  make(map[int64]*publicationState),
		subscriptions: make(map[int64]*subscriptionState),
	}

	corrID := c.AddExclusivePublication("aeron:ipc?term-length=128k", 102)
	if corrID < 0 {
		t.Fatalf("AddExclusivePublication returned %d", corrID)
	}

	c.mu.Lock()
	state, ok := c.publications[corrID]
	c.mu.Unlock()
	if !ok {
		t.Fatalf("publications map missing entry for corrID=%d", corrID)
	}
	if state.channel != "aeron:ipc?term-length=128k" || state.streamID != 102 {
		t.Errorf("publication state wrong: channel=%q streamID=%d", state.channel, state.streamID)
	}

	if cmd := peekFirstCommandType(t, rb); cmd != CmdAddExclusivePublication {
		t.Errorf("expected CmdAddExclusivePublication (0x%x), got 0x%x",
			CmdAddExclusivePublication, cmd)
	}
}

// Aeron.AddExclusivePublication wraps Conductor.AddExclusivePublication
// + Conductor.DoWork polling. DoWork dereferences the broadcast
// receiver, so any meaningful unit test needs a live media driver to
// stand it up. The wrapper is consequently covered by integration
// (loadtester end-to-end) the same way Aeron.AddPublication is — see
// TestConductorAddExclusivePublication for the deepest unit coverage of
// the new code path.

// peekFirstCommandType reads the msgTypeID field of the first record in
// the ring buffer. Used to assert the right driver command was sent.
// The ring-buffer record layout is [int32 recordLength][int32 msgTypeID][bytes],
// so msgTypeID lives at offset 4 of the buffer's data area (index 0).
func peekFirstCommandType(t *testing.T, rb *ManyToOneRingBuffer) int32 {
	t.Helper()
	if rb.buffer.GetInt32(0) <= 0 {
		t.Fatal("ring buffer was empty — no command written")
	}
	return rb.buffer.GetInt32(4)
}

func TestOnNewPublicationShortMessage(t *testing.T) {
	c := &Conductor{
		publications:  make(map[int64]*publicationState),
		subscriptions: make(map[int64]*subscriptionState),
	}
	// Should not panic on short messages
	c.onNewPublication(make([]byte, 10))
	c.onSubscriptionReady(make([]byte, 5))
	c.onError(make([]byte, 5))
	c.onAvailableImage(make([]byte, 10))
	c.onUnavailableImage(make([]byte, 10))
}

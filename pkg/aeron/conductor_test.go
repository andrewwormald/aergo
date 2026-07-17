package aeron

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	binary.LittleEndian.PutUint64(msg[0:], uint64(corrID)) // correlationID
	binary.LittleEndian.PutUint64(msg[8:], uint64(100))    // registrationID
	binary.LittleEndian.PutUint32(msg[16:], uint32(7))     // sessionID
	binary.LittleEndian.PutUint32(msg[20:], uint32(1))     // streamID
	binary.LittleEndian.PutUint32(msg[24:], uint32(5))     // posLimitCounterID
	binary.LittleEndian.PutUint32(msg[28:], uint32(3))     // channelStatusID
	binary.LittleEndian.PutUint32(msg[32:], uint32(0))     // logFileLength = 0 (no file)

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
	binary.LittleEndian.PutUint64(msg[0:], uint64(1))            // correlationID
	binary.LittleEndian.PutUint32(msg[8:], uint32(42))           // errorCode
	binary.LittleEndian.PutUint32(msg[12:], uint32(len(errMsg))) // msgLength
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

// encodeErrorResponse builds a RespOnError message per
// io.aeron.command.ErrorResponseFlyweight.
func encodeErrorResponse(corrID int64, errCode int32, errMsg string) []byte {
	msg := make([]byte, 16+len(errMsg))
	binary.LittleEndian.PutUint64(msg[0:], uint64(corrID))
	binary.LittleEndian.PutUint32(msg[8:], uint32(errCode))
	binary.LittleEndian.PutUint32(msg[12:], uint32(len(errMsg)))
	copy(msg[16:], errMsg)
	return msg
}

func TestOnErrorFailsPendingAdd(t *testing.T) {
	tests := []struct {
		name string
		add  func(c *Conductor) int64
	}{
		{"publication", func(c *Conductor) int64 { return c.AddPublication("aeron:ipc", 101) }},
		{"exclusive publication", func(c *Conductor) int64 { return c.AddExclusivePublication("aeron:ipc", 101) }},
		{"subscription", func(c *Conductor) int64 { return c.AddSubscription("aeron:ipc", 101) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conductor{
				proxy:         NewDriverProxy(newTestRingBuffer(4096), 42),
				publications:  make(map[int64]*publicationState),
				subscriptions: make(map[int64]*subscriptionState),
			}

			corrID := tc.add(c)
			if corrID < 0 {
				t.Fatalf("add returned %d", corrID)
			}
			if err := c.pendingError(corrID); err != nil {
				t.Fatalf("pendingError before rejection: got %v, want nil", err)
			}

			driverMsg := "invalid channel: no such network interface"
			msg := encodeErrorResponse(corrID, 1 /* INVALID_CHANNEL */, driverMsg)
			c.onDriverMessage(RespOnError, msg, 0, int32(len(msg)))

			err := c.pendingError(corrID)
			if err == nil {
				t.Fatal("pendingError after rejection: got nil, want error")
			}
			var regErr *RegistrationError
			if !errors.As(err, &regErr) {
				t.Fatalf("error type: got %T, want *RegistrationError", err)
			}
			if regErr.CorrelationID != corrID || regErr.Code != 1 || regErr.Message != driverMsg {
				t.Errorf("RegistrationError fields: got %+v", regErr)
			}
			if !strings.Contains(err.Error(), "INVALID_CHANNEL") || !strings.Contains(err.Error(), driverMsg) {
				t.Errorf("error text missing code name or driver message: %q", err.Error())
			}

			// An unrelated correlation ID must be unaffected.
			if err := c.pendingError(corrID + 1000); err != nil {
				t.Errorf("pendingError for unrelated corrID: got %v, want nil", err)
			}
		})
	}
}

func TestOnClientTimeout(t *testing.T) {
	newConductor := func() *Conductor {
		return &Conductor{
			clientID:      42,
			publications:  make(map[int64]*publicationState),
			subscriptions: make(map[int64]*subscriptionState),
		}
	}
	encodeClientTimeout := func(clientID int64) []byte {
		msg := make([]byte, 8)
		binary.LittleEndian.PutUint64(msg[0:], uint64(clientID))
		return msg
	}

	t.Run("other client's timeout is ignored", func(t *testing.T) {
		c := newConductor()
		msg := encodeClientTimeout(7)
		c.onDriverMessage(RespOnClientTimeout, msg, 0, int32(len(msg)))

		if c.isTerminated() {
			t.Error("conductor terminated by another client's timeout")
		}
		if err := c.FatalError(); err != nil {
			t.Errorf("FatalError: got %v, want nil", err)
		}
	})

	t.Run("own timeout terminates the client", func(t *testing.T) {
		c := newConductor()
		msg := encodeClientTimeout(42)
		c.onDriverMessage(RespOnClientTimeout, msg, 0, int32(len(msg)))

		if !c.isTerminated() {
			t.Fatal("conductor not terminated by own client timeout")
		}
		var timeoutErr *ClientTimeoutError
		if err := c.FatalError(); !errors.As(err, &timeoutErr) {
			t.Fatalf("FatalError: got %v, want *ClientTimeoutError", err)
		}

		// Offers on the terminated client's publications must return Closed.
		lb := newInMemLogBuffers(offerTestTermLen)
		pub := newInMemPublication(lb, 7, 1001)
		pub.conductor = c
		if got := pub.Offer(make([]byte, 8)); got != Closed {
			t.Errorf("Offer after client timeout: got %d, want Closed", got)
		}
	})
}

func TestCheckDriverLiveness(t *testing.T) {
	const timeoutMs = int64(10_000)
	nowMs := int64(1_000_000)

	tests := []struct {
		name        string
		cnc         *MappedCnc
		heartbeatMs int64
		wantErr     bool
	}{
		{name: "nil cnc passes", cnc: nil},
		{name: "fresh heartbeat passes", cnc: newInMemCnc(1), heartbeatMs: nowMs - 500},
		{name: "heartbeat at timeout boundary passes", cnc: newInMemCnc(1), heartbeatMs: nowMs - timeoutMs},
		{name: "stale heartbeat fails", cnc: newInMemCnc(1), heartbeatMs: nowMs - timeoutMs - 1, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conductor{cnc: tc.cnc, driverTimeoutNs: timeoutMs * 1_000_000}
			if tc.cnc != nil {
				setInMemDriverHeartbeat(tc.cnc, tc.heartbeatMs)
			}

			err := c.checkDriverLiveness(nowMs)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("checkDriverLiveness: got %v, want nil", err)
				}
				return
			}
			var timeoutErr *DriverTimeoutError
			if !errors.As(err, &timeoutErr) {
				t.Fatalf("error type: got %T (%v), want *DriverTimeoutError", err, err)
			}
			if timeoutErr.HeartbeatAgeMs != nowMs-tc.heartbeatMs || timeoutErr.TimeoutMs != timeoutMs {
				t.Errorf("DriverTimeoutError fields: got %+v", timeoutErr)
			}
		})
	}
}

func TestDoWorkTerminatesOnStaleDriverHeartbeat(t *testing.T) {
	c, _ := newInMemConductor(42)
	setInMemDriverHeartbeat(c.cnc, time.Now().UnixMilli()-c.driverTimeoutNs/1_000_000-1)

	c.DoWork()

	if !c.isTerminated() {
		t.Fatal("conductor not terminated by stale driver heartbeat")
	}
	var timeoutErr *DriverTimeoutError
	if err := c.FatalError(); !errors.As(err, &timeoutErr) {
		t.Fatalf("FatalError: got %v, want *DriverTimeoutError", err)
	}

	lb := newInMemLogBuffers(offerTestTermLen)
	pub := newInMemPublication(lb, 7, 1001)
	pub.conductor = c
	if got := pub.Offer(make([]byte, 8)); got != Closed {
		t.Errorf("Offer after driver timeout: got %d, want Closed", got)
	}
}

func TestOnAvailableImageParsesSourceIdentity(t *testing.T) {
	// onAvailableImage maps the log file, so back the message with a real
	// (temp) file shaped like a driver log buffer.
	const termLen = int32(4 * 1024)
	logFile := filepath.Join(t.TempDir(), "image.logbuffer")
	if err := os.WriteFile(logFile, make([]byte, int(PartitionCount*termLen)+LogMetaDataLength), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &Conductor{
		publications:  make(map[int64]*publicationState),
		subscriptions: make(map[int64]*subscriptionState),
	}
	subRegID := int64(5)
	c.subscriptions[subRegID] = &subscriptionState{correlationID: subRegID, ready: true}

	// Craft an ImageBuffersReady message (io.aeron.command.
	// ImageBuffersReadyFlyweight): the source identity is a length-prefixed
	// ASCII string whose length prefix sits int-aligned after the log file
	// name.
	sourceIdentity := "aeron:udp?endpoint=192.168.0.7:40456"
	srcOff := 32 + int(align(int32(len(logFile)), 4))
	msg := make([]byte, srcOff+4+len(sourceIdentity))
	binary.LittleEndian.PutUint64(msg[0:], uint64(77))            // correlationID
	binary.LittleEndian.PutUint32(msg[8:], uint32(9))             // sessionID
	binary.LittleEndian.PutUint32(msg[12:], uint32(1001))         // streamID
	binary.LittleEndian.PutUint64(msg[16:], uint64(subRegID))     // subscriptionRegistrationID
	binary.LittleEndian.PutUint32(msg[24:], ^uint32(0))           // subscriberPositionID = -1
	binary.LittleEndian.PutUint32(msg[28:], uint32(len(logFile))) // logFileName length
	copy(msg[32:], logFile)                                       // logFileName
	binary.LittleEndian.PutUint32(msg[srcOff:], uint32(len(sourceIdentity)))
	copy(msg[srcOff+4:], sourceIdentity)

	c.onDriverMessage(RespOnAvailableImage, msg, 0, int32(len(msg)))

	c.mu.Lock()
	defer c.mu.Unlock()
	sub := c.subscriptions[subRegID]
	if len(sub.images) != 1 {
		t.Fatalf("images: got %d, want 1", len(sub.images))
	}
	img := sub.images[0]
	t.Cleanup(func() {
		if err := img.LogBuffers.Close(); err != nil {
			t.Errorf("close image log buffers: %v", err)
		}
	})
	if img.SourceIdentity != sourceIdentity {
		t.Errorf("SourceIdentity: got %q, want %q", img.SourceIdentity, sourceIdentity)
	}
	if img.CorrelationID != 77 || img.SessionID != 9 {
		t.Errorf("image identity fields: got corrID=%d sessionID=%d", img.CorrelationID, img.SessionID)
	}
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

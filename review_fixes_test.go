package aergo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func writeLogFile(t *testing.T, termLen int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "x.logbuffer")
	if err := os.WriteFile(path, make([]byte, 3*termLen+LogMetaDataLength), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// runAfterWork runs the callbacks a driver message handler queued, as DoWork
// does once it lets go of its lock, for tests that call the handlers directly.
func runAfterWork(c *Conductor) {
	callbacks := c.afterWork
	c.afterWork = nil
	for _, fn := range callbacks {
		fn()
	}
}

func newBareConductor() *Conductor {
	return &Conductor{
		publications:  map[int64]*publicationState{},
		subscriptions: map[int64]*subscriptionState{},
	}
}

func publicationReply(corrID int64, logFile string) []byte {
	msg := make([]byte, 36+len(logFile))
	binary.LittleEndian.PutUint64(msg[0:], uint64(corrID))
	binary.LittleEndian.PutUint64(msg[8:], uint64(corrID))
	binary.LittleEndian.PutUint32(msg[32:], uint32(len(logFile)))
	copy(msg[36:], logFile)
	return msg
}

// unavailableImageReply builds the driver's ON_UNAVAILABLE_IMAGE message in the
// layout of Java's ImageMessageFlyweight: correlation ID, then the subscription
// registration ID, then the stream ID and channel.
func unavailableImageReply(imageCorrID, subRegID int64) []byte {
	msg := make([]byte, 24)
	binary.LittleEndian.PutUint64(msg[0:], uint64(imageCorrID))
	binary.LittleEndian.PutUint64(msg[8:], uint64(subRegID))
	binary.LittleEndian.PutUint32(msg[16:], 1001) // stream ID
	return msg
}

func imageReply(subRegID int64, logFile string) []byte {
	msg := make([]byte, 32+len(logFile))
	binary.LittleEndian.PutUint64(msg[16:], uint64(subRegID))
	binary.LittleEndian.PutUint32(msg[24:], 0xFFFFFFFF) // no subscriber position counter
	binary.LittleEndian.PutUint32(msg[28:], uint32(len(logFile)))
	copy(msg[32:], logFile)
	return msg
}

// A reply for an add that no longer exists (it timed out), or a duplicate reply,
// must not leave a reference to the mapped log behind.
func TestLateOrDuplicatePublicationReplyDoesNotKeepTheLog(t *testing.T) {
	logFile := writeLogFile(t, 64*1024)

	c := newBareConductor()
	c.onNewPublication(publicationReply(5, logFile)) // no pending add 5
	if len(c.logs) != 0 {
		t.Errorf("a reply with no pending add kept %d log mapping(s)", len(c.logs))
	}

	c.publications[6] = &publicationState{correlationID: 6}
	c.onNewPublication(publicationReply(6, logFile))
	if lb := c.logs[logFile]; lb == nil || lb.refs != 1 {
		t.Fatalf("first reply: refs = %v, want 1", lb)
	}
	c.onNewPublication(publicationReply(6, logFile)) // duplicate
	if lb := c.logs[logFile]; lb == nil || lb.refs != 1 {
		t.Errorf("duplicate reply changed the reference count: %+v", lb)
	}
}

func TestImageForAnUnknownSubscriptionIsReleased(t *testing.T) {
	logFile := writeLogFile(t, 64*1024)
	c := newBareConductor()
	c.onAvailableImage(imageReply(99, logFile))
	if len(c.logs) != 0 {
		t.Errorf("an image nobody wanted kept %d log mapping(s)", len(c.logs))
	}
}

// Closing a subscription must give back the logs of its images: the driver
// does not report them unavailable, so they would otherwise stay mapped until
// the client closed, once per closed subscription (every reconnect).
func TestSubscriptionCloseReleasesItsImagesAndLateOnes(t *testing.T) {
	logFile := writeLogFile(t, 64*1024)
	c := newBareConductor()
	const id = int64(7)
	state := &subscriptionState{correlationID: id, ready: true}
	c.subscriptions[id] = state
	sub := newSubscription(c, id, state)

	c.onAvailableImage(imageReply(id, logFile))
	if len(state.images) != 1 || len(c.logs) != 1 {
		t.Fatalf("setup: %d images, %d mappings", len(state.images), len(c.logs))
	}
	lb := state.images[0].LogBuffers

	sub.Close()
	if _, ok := c.subscriptions[id]; ok {
		t.Error("closed subscription is still registered with the conductor")
	}
	if n := sub.Poll(func([]byte, *Header) {}, 10); n != 0 {
		t.Errorf("Poll on a closed subscription read %d fragments", n)
	}
	if lb.data == nil {
		t.Fatal("log unmapped immediately; a goroutine may still be polling it")
	}
	c.reapLingering(time.Now().UnixNano() + resourceLingerNs + 1)
	if lb.data != nil || len(c.logs) != 0 {
		t.Errorf("log still mapped after the linger (mappings: %d)", len(c.logs))
	}

	c.onAvailableImage(imageReply(id, logFile)) // the driver was slower than the close
	if len(c.logs) != 0 {
		t.Errorf("an image arriving after Close kept %d mapping(s)", len(c.logs))
	}
}

// A publication without a log is closed; the lengths it would be checked
// against do not exist, so it must not claim every message is too long.
func TestPublicationWithoutALogReportsClosed(t *testing.T) {
	p := &Publication{}
	if r, err := p.Offer([]byte("x")); err != nil || r != Closed {
		t.Errorf("Offer = %d, %v; want Closed", r, err)
	}
	if r, err := p.OfferParts([]byte("x"), []byte("y")); err != nil || r != Closed {
		t.Errorf("OfferParts = %d, %v; want Closed", r, err)
	}
	if _, r, err := p.TryClaim(1); err != nil || r != Closed {
		t.Errorf("TryClaim = %d, %v; want Closed", r, err)
	}
}

// Abort turns a claim into padding. The frame type is a 16-bit field; writing
// it as 32 bits also zeroed the low half of the term offset after it.
func TestAbortKeepsTheTermOffset(t *testing.T) {
	pub, sub, lb := newFragPair(fragTestTermLen)
	if _, err := pub.Offer(make([]byte, 100)); err != nil { // advance past offset 0
		t.Fatal(err)
	}
	claim, pos, err := pub.TryClaim(10)
	if err != nil || pos <= 0 {
		t.Fatalf("TryClaim: %d, %v", pos, err)
	}
	offset := claim.offset
	if offset == 0 {
		t.Fatal("test needs a claim away from offset 0")
	}
	claim.Abort()

	term := lb.Term(0)
	if got := term.GetInt16(offset + FrameTypeOffset); got != FrameTypePadding {
		t.Errorf("type = %d, want padding", got)
	}
	if got := term.GetInt32(offset + FrameTermOffsetOff); got != offset {
		t.Errorf("term offset = %d, want %d (Abort overwrote it)", got, offset)
	}
	if got := term.GetInt32(offset + FrameLengthOffset); got != DataFrameHeaderLen+10 {
		t.Errorf("length = %d", got)
	}
	// A subscriber still reads the message before it and skips the padding.
	if got := drain(sub); len(got) != 1 {
		t.Errorf("delivered %d messages around the aborted claim, want 1", len(got))
	}
}

// publishAndPollConcurrently runs publishers while a subscriber polls the same
// log at the same time, which is the visibility the ordering claims depend on,
// and checks every message arrives whole and exactly once.
func publishAndPollConcurrently(t *testing.T, termLen int32, workers, each, size int, exclusive bool) (adminActions int) {
	t.Helper()
	pub, sub, _ := newFragPair(termLen)
	pub.exclusive = exclusive
	total := workers * each

	var mu sync.Mutex
	seen := make(map[[2]byte]int)
	delivered := 0
	asm := NewFragmentAssembler(func(m []byte, _ *Header) {
		if len(m) != size {
			t.Errorf("delivered a message of %d bytes, want %d", len(m), size)
			return
		}
		w, i := m[0], m[1]
		want := patterned(size, w)
		want[0], want[1] = w, i
		if !bytes.Equal(m, want) {
			t.Errorf("message from worker %d #%d interleaved or corrupted", w, i)
		}
		mu.Lock()
		seen[[2]byte{w, i}]++
		delivered++
		mu.Unlock()
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(30 * time.Second)
		for {
			mu.Lock()
			n := delivered
			mu.Unlock()
			if n >= total || time.Now().After(deadline) {
				return
			}
			sub.Poll(asm.OnFragment, 64)
		}
	}()

	var wg sync.WaitGroup
	var amu sync.Mutex
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				msg := patterned(size, byte(w))
				msg[0], msg[1] = byte(w), byte(i)
				for {
					r, err := pub.Offer(msg)
					if err != nil {
						t.Error(err)
						return
					}
					if r > 0 {
						break
					}
					if r != AdminAction {
						t.Errorf("worker %d: offer = %d", w, r)
						return
					}
					amu.Lock()
					adminActions++
					amu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	<-done

	if delivered != total {
		t.Fatalf("delivered %d messages, want %d", delivered, total)
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("message %v delivered %d times", k, n)
		}
	}
	return adminActions
}

// Publishers and a subscriber at the same time. Under -race this also checks
// the handoff between the frame-length store and the poller's load is seen as
// ordered, which a plain store of the first header word was not.
func TestSubscriberPollsWhilePublishersWrite(t *testing.T) {
	publishAndPollConcurrently(t, 4*1024*1024, 4, 100, 3000, false)
}

// Concurrent fragmented offers whose claims straddle the end of a term: one
// goroutine pads and rotates, the others see AdminAction and retry, and the
// subscriber crosses the padding while it all happens. About two terms of data,
// so no partition is reused (nothing here emulates the driver cleaning them).
func TestConcurrentFragmentedOffersAcrossTermEnds(t *testing.T) {
	const termLen = 512 * 1024
	admin := publishAndPollConcurrently(t, termLen, 4, 40, 7000, false)
	if admin == 0 {
		t.Error("no offer hit the end of a term; the test is not exercising rotation")
	}
	t.Logf("%d AdminAction retries", admin)
}

// A closed publication's log is unmapped after the linger. Asking such a
// publication whether it is connected must not read the unmapped log.
func TestClosedPublicationIsNotConnectedAfterItsLogIsUnmapped(t *testing.T) {
	logFile := writeLogFile(t, 64*1024)
	c := newBareConductor()
	lb, err := c.acquireLog(logFile)
	if err != nil {
		t.Fatal(err)
	}
	lb.Meta().PutInt32Ordered(MetaIsConnectedOff, 1)
	st := &publicationState{logBuffers: lb}
	p := &Publication{conductor: c, state: st, logBuffers: lb}
	if !p.IsConnected() {
		t.Fatal("setup: publication should report connected")
	}

	p.closed.Store(true)
	c.lingerPublicationLog(st)
	c.reapLingering(time.Now().UnixNano() + resourceLingerNs + 1)
	if lb.data != nil {
		t.Fatal("setup: log should be unmapped")
	}

	if p.IsConnected() {
		t.Error("closed publication reports connected")
	}
}

// The driver's ON_UNAVAILABLE_IMAGE names the subscription at offset 8 (Java's
// ImageMessageFlyweight). It was read from offset 16, so no image was ever
// matched and none was ever removed.
func TestUnavailableImageIsRemovedAndReported(t *testing.T) {
	logFile := writeLogFile(t, 64*1024)
	c := newBareConductor()
	const id = int64(7)
	state := &subscriptionState{correlationID: id, ready: true}
	c.subscriptions[id] = state
	sub := newSubscription(c, id, state)
	var ended []int32
	sub.SetUnavailableImageHandler(func(session int32) { ended = append(ended, session) })

	msg := imageReply(id, logFile)
	binary.LittleEndian.PutUint32(msg[8:], 77) // session ID
	binary.LittleEndian.PutUint64(msg[0:], 10) // image correlation ID
	c.onAvailableImage(msg)
	if len(state.images) != 1 {
		t.Fatalf("setup: %d images", len(state.images))
	}
	lb := state.images[0].LogBuffers

	c.onUnavailableImage(unavailableImageReply(10, id))
	runAfterWork(c)

	if len(state.images) != 0 || len(*state.snapshot.Load()) != 0 {
		t.Errorf("image still present after the driver reported it unavailable")
	}
	if len(ended) != 1 || ended[0] != 77 {
		t.Errorf("handler got sessions %v, want [77]", ended)
	}
	if lb.data == nil {
		t.Error("log unmapped immediately; a poller may still be reading it")
	}
	c.reapLingering(time.Now().UnixNano() + resourceLingerNs + 1)
	if lb.data != nil {
		t.Error("log still mapped after the linger")
	}

	// A message for another subscription or image changes nothing.
	c.onUnavailableImage(unavailableImageReply(10, 999))
	runAfterWork(c)
	if len(ended) != 1 {
		t.Errorf("handler called for an unknown subscription: %v", ended)
	}
}

func TestErrorHandlerReceivesErrorsAndTheRecordedListIsBounded(t *testing.T) {
	c := newBareConductor()
	var got []error
	c.errorHandler = func(err error) { got = append(got, err) }

	// A log that cannot be mapped used to vanish without a trace.
	c.subscriptions[3] = &subscriptionState{correlationID: 3, ready: true}
	c.onAvailableImage(imageReply(3, filepath.Join(t.TempDir(), "missing.logbuffer")))
	runAfterWork(c)
	if len(got) != 1 {
		t.Fatalf("handler got %d errors for an unmappable image log, want 1", len(got))
	}

	for i := 0; i < 5*maxRecordedErrors; i++ {
		c.reportError(errors.New("x"))
	}
	if len(c.errors) != maxRecordedErrors {
		t.Errorf("%d errors kept, want the most recent %d", len(c.errors), maxRecordedErrors)
	}

	var viaOption Context
	WithErrorHandler(func(error) {})(&viaOption)
	if viaOption.ErrorHandler == nil {
		t.Error("WithErrorHandler did not set the handler")
	}
}

// A committed length no frame can have is not skipped: the subscription stops
// there, delivers nothing more, and reports it once.
func TestPollStopsAtACorruptFrameAndReportsItOnce(t *testing.T) {
	for name, badLen := range map[string]int32{
		"shorter than a header":    5,
		"longer than the term":     64*1024 + 32,
		"past the end of the term": 64*1024 - 64 + 96,
	} {
		t.Run(name, func(t *testing.T) {
			lb := newInMemLogBuffers(64 * 1024)
			app := NewTermAppender(lb, 0)
			app.Append(0, 1, 1001, []byte("good"))
			// Commit a bad length where the next frame would start (offset 64).
			lb.Term(0).PutInt32Ordered(64+FrameLengthOffset, badLen)

			sub := newInMemSubscription(lb, 1001)
			var errs []error
			sub.conductor.errorHandler = func(err error) { errs = append(errs, err) }
			img := sub.state.images[0]

			var got []string
			handler := func(b []byte, _ *Header) { got = append(got, string(b)) }
			if n := sub.Poll(handler, 10); n != 1 {
				t.Errorf("first Poll = %d, want the 1 good frame", n)
			}
			for i := 0; i < 5; i++ {
				if n := sub.Poll(handler, 10); n != 0 {
					t.Errorf("Poll read %d fragments past a corrupt frame", n)
				}
			}
			if len(got) != 1 || got[0] != "good" {
				t.Errorf("delivered %q", got)
			}
			if img.subscriberPosition != 64 {
				t.Errorf("position = %d, want to stop at the corrupt frame (64)", img.subscriberPosition)
			}
			if len(errs) != 1 || !errors.Is(errs[0], ErrCorruptFrame) {
				t.Errorf("reported %v, want exactly one ErrCorruptFrame", errs)
			}
		})
	}
}

// A reassembled message is described by its header: the whole length, flagged
// unfragmented, starting where the first fragment did (Java's FragmentAssembler).
func TestReassembledMessageHeaderDescribesTheWholeMessage(t *testing.T) {
	var got Header
	var payload string
	a := NewFragmentAssembler(func(b []byte, h *Header) { got, payload = *h, string(b) })
	a.OnFragment([]byte("aa"), &Header{SessionID: 4, Flags: FlagBeginFrag, TermOffset: 64, FrameLength: 34, Position: 128})
	a.OnFragment([]byte("bb"), &Header{SessionID: 4, Flags: 0, TermOffset: 128, FrameLength: 34, Position: 192})
	a.OnFragment([]byte("c"), &Header{SessionID: 4, Flags: FlagEndFrag, TermOffset: 192, FrameLength: 33, Position: 256, StreamID: 9})

	if payload != "aabbc" {
		t.Fatalf("payload %q", payload)
	}
	if got.FrameLength != DataFrameHeaderLen+5 {
		t.Errorf("FrameLength = %d, want %d", got.FrameLength, DataFrameHeaderLen+5)
	}
	if got.Flags != FlagUnfrag {
		t.Errorf("Flags = %#x, want unfragmented", got.Flags)
	}
	if got.TermOffset != 64 {
		t.Errorf("TermOffset = %d, want 64, where the message begins", got.TermOffset)
	}
	if got.Position != 256 || got.SessionID != 4 || got.StreamID != 9 {
		t.Errorf("Position/SessionID/StreamID = %d/%d/%d", got.Position, got.SessionID, got.StreamID)
	}
}

// When the egress image of a session goes away, its reassembly state goes too.
func TestClusterFreesReassemblyStateOfAnEndedEgressSession(t *testing.T) {
	c, _, _ := newEgressTestCluster(StateConnected)
	c.egressSub.SetUnavailableImageHandler(c.onEgressImageUnavailable)

	// Half a message from session 1, whose image is the subscription's only one.
	c.egressAssembler.OnFragment(make([]byte, 100), &Header{SessionID: 1, Flags: FlagBeginFrag})
	if len(c.egressAssembler.builders) != 1 {
		t.Fatalf("setup: %d builders", len(c.egressAssembler.builders))
	}

	c.egressSub.conductor.onUnavailableImage(unavailableImageReply(0, c.egressSub.registrationID))
	runAfterWork(c.egressSub.conductor)
	c.pollEgress() // frees on the polling goroutine

	if len(c.egressAssembler.builders) != 0 {
		t.Errorf("%d builders left for a session that ended", len(c.egressAssembler.builders))
	}
}

// Closing a publication drops its state; the map used to grow with every
// publication ever added.
func TestPublicationCloseForgetsItsState(t *testing.T) {
	logFile := writeLogFile(t, 64*1024)
	c := newBareConductor()
	c.proxy = NewDriverProxy(newTestRingBuffer(4096), 1)
	lb, err := c.acquireLog(logFile)
	if err != nil {
		t.Fatal(err)
	}
	st := &publicationState{correlationID: 9, registrationID: 9, logBuffers: lb}
	c.publications[9] = st
	p := &Publication{conductor: c, state: st, logBuffers: lb, registrationID: 9}

	p.Close()
	if len(c.publications) != 0 {
		t.Errorf("%d publication states left after Close", len(c.publications))
	}
	if len(c.lingering) != 1 {
		t.Errorf("log not queued for release: %d lingering", len(c.lingering))
	}
	p.Close() // idempotent
	if len(c.lingering) != 1 {
		t.Errorf("second Close queued another release: %d lingering", len(c.lingering))
	}
}

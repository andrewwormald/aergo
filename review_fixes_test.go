package aergo

import (
	"bytes"
	"encoding/binary"
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
func publishAndPollConcurrently(t *testing.T, termLen int32, workers, each, size int) (adminActions int) {
	t.Helper()
	pub, sub, _ := newFragPair(termLen)
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
	publishAndPollConcurrently(t, 4*1024*1024, 4, 100, 3000)
}

// Concurrent fragmented offers whose claims straddle the end of a term: one
// goroutine pads and rotates, the others see AdminAction and retry, and the
// subscriber crosses the padding while it all happens. About two terms of data,
// so no partition is reused (nothing here emulates the driver cleaning them).
func TestConcurrentFragmentedOffersAcrossTermEnds(t *testing.T) {
	const termLen = 512 * 1024
	admin := publishAndPollConcurrently(t, termLen, 4, 40, 7000)
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

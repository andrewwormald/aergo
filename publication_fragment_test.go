package aergo

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

const (
	fragTestTermLen    = 64 * 1024
	fragTestMaxPayload = DefaultMtuLength - DataFrameHeaderLen // 1376
	fragTestMaxMessage = fragTestTermLen >> 3                  // 8192

	// clusterSessionHeaderLen is the SBE message header plus the session message body.
	clusterSessionHeaderLen = HeaderSize + sessionMessageHeaderBlockLength
)

// patterned returns n bytes that differ by position and by seed, so a
// fragment landing in the wrong place or message is caught.
func patterned(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7) ^ seed ^ byte(i>>8)
	}
	return b
}

// drain polls sub until empty, reassembling fragments, and returns the
// complete messages in the order they were delivered.
func drain(sub *Subscription) [][]byte {
	var got [][]byte
	asm := NewFragmentAssembler(func(buf []byte, _ *Header) {
		got = append(got, append([]byte(nil), buf...))
	})
	for sub.Poll(asm.OnFragment, 64) > 0 {
	}
	return got
}

func newFragPair(termLen int32) (*Publication, *Subscription, *LogBuffers) {
	lb := NewLoopbackLogBuffers(termLen)
	lb.meta.PutInt32(MetaMtuLenOff, DefaultMtuLength)
	return NewLoopbackPublication(lb, 1, 1001), NewLoopbackSubscription(lb, 1001), lb
}

func TestPublicationLimits(t *testing.T) {
	pub, _, _ := newFragPair(fragTestTermLen)
	if got := pub.MaxPayloadLength(); got != fragTestMaxPayload {
		t.Errorf("MaxPayloadLength = %d, want %d", got, fragTestMaxPayload)
	}
	if got := pub.MaxMessageLength(); got != fragTestMaxMessage {
		t.Errorf("MaxMessageLength = %d, want %d (term/8)", got, fragTestMaxMessage)
	}

	// A large term is capped at 16 MiB (Java FrameDescriptor.MAX_MESSAGE_LENGTH).
	if got := computeMaxMessageLength(1 << 30); got != MaxMessageLengthCap {
		t.Errorf("computeMaxMessageLength(1GiB) = %d, want %d", got, MaxMessageLengthCap)
	}
}

func TestComputeFragmentedFrameLength(t *testing.T) {
	// Mirrors Java LogBufferDescriptor.computeFragmentedFrameLength.
	for _, tc := range []struct{ length, want int32 }{
		{fragTestMaxPayload, 1408},                 // exactly one full frame
		{fragTestMaxPayload + 1, 1408 + 64},        // full frame + 1 byte -> 33 aligned to 64
		{2 * fragTestMaxPayload, 2 * 1408},         // two full frames, no remainder
		{3000, 2*1408 + 288},                       // 2 full + 248 byte tail (280 aligned to 288)
		{fragTestMaxPayload*5 + 120, 5*1408 + 160}, // 120+32=152 aligned to 160
	} {
		if got := computeFragmentedFrameLength(tc.length, fragTestMaxPayload); got != tc.want {
			t.Errorf("computeFragmentedFrameLength(%d) = %d, want %d", tc.length, got, tc.want)
		}
	}
}

// TestOfferFragmentedFrameLayout checks the exact bytes on the log against
// the Aeron framing rules: contiguous frames, each at most one MTU, flags
// BEGIN on the first and END on the last only, term offsets advancing by the
// aligned frame length, and an unfragmented message left untouched.
func TestOfferFragmentedFrameLayout(t *testing.T) {
	pub, _, lb := newFragPair(fragTestTermLen)
	term := lb.Term(0)
	msg := patterned(3000, 1)

	pos, err := pub.Offer(msg)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(2*1408 + 288); pos != want {
		t.Fatalf("position = %d, want %d", pos, want)
	}

	type frame struct {
		offset, length int32
		flags          uint8
	}
	want := []frame{
		{0, DataFrameHeaderLen + 1376, FlagBeginFrag},
		{1408, DataFrameHeaderLen + 1376, 0},
		{2816, DataFrameHeaderLen + 248, FlagEndFrag},
	}
	var reassembled []byte
	for i, w := range want {
		if got := term.GetInt32(w.offset + FrameLengthOffset); got != w.length {
			t.Errorf("frame %d length = %d, want %d", i, got, w.length)
		}
		if got := term.GetUint8(w.offset + FrameFlagsOffset); got != w.flags {
			t.Errorf("frame %d flags = %#x, want %#x", i, got, w.flags)
		}
		if got := term.GetInt32(w.offset + FrameTermOffsetOff); got != w.offset {
			t.Errorf("frame %d term offset = %d, want %d", i, got, w.offset)
		}
		if got := int16(term.GetInt32(w.offset + FrameTypeOffset)); got != FrameTypeData {
			t.Errorf("frame %d type = %d, want data", i, got)
		}
		if got := term.GetInt32(w.offset + FrameSessionIDOff); got != 1 {
			t.Errorf("frame %d session = %d, want 1", i, got)
		}
		if got := term.GetInt32(w.offset + FrameStreamIDOff); got != 1001 {
			t.Errorf("frame %d stream = %d, want 1001", i, got)
		}
		reassembled = append(reassembled, term.Slice(w.offset+DataFrameHeaderLen, w.length-DataFrameHeaderLen)...)
	}
	if !bytes.Equal(reassembled, msg) {
		t.Error("payload bytes across the fragments differ from the message offered")
	}
	// Nothing after the last fragment.
	if got := term.GetInt32(2816 + 288 + FrameLengthOffset); got != 0 {
		t.Errorf("frame after the message has length %d, want 0", got)
	}
}

func TestOfferAtMaxPayloadIsUnfragmented(t *testing.T) {
	pub, _, lb := newFragPair(fragTestTermLen)
	if _, err := pub.Offer(patterned(fragTestMaxPayload, 2)); err != nil {
		t.Fatal(err)
	}
	term := lb.Term(0)
	if got := term.GetUint8(FrameFlagsOffset); got != FlagUnfrag {
		t.Errorf("flags = %#x, want unfragmented %#x", got, FlagUnfrag)
	}
	if got := term.GetInt32(FrameLengthOffset); got != DefaultMtuLength {
		t.Errorf("length = %d, want one full MTU", got)
	}
}

func TestOfferRoundTripAcrossBoundaries(t *testing.T) {
	sizes := []int{0, 1, 31, 32, fragTestMaxPayload - 1, fragTestMaxPayload, fragTestMaxPayload + 1,
		2*fragTestMaxPayload - 1, 2 * fragTestMaxPayload, 2*fragTestMaxPayload + 1, 5000, fragTestMaxMessage}
	for _, n := range sizes {
		pub, sub, _ := newFragPair(fragTestTermLen)
		msg := patterned(n, byte(n))
		if _, err := pub.Offer(msg); err != nil {
			t.Fatalf("size %d: %v", n, err)
		}
		got := drain(sub)
		if len(got) != 1 || !bytes.Equal(got[0], msg) {
			t.Errorf("size %d: delivered %d messages, first matches=%v", n, len(got), len(got) > 0 && bytes.Equal(got[0], msg))
		}
	}
}

// TestOfferPartsMatchesOffer: however the message is split between the two
// parts, including at and around fragment boundaries, the log carries the
// same message as offering it whole.
func TestOfferPartsMatchesOffer(t *testing.T) {
	msg := patterned(4000, 9)
	for _, split := range []int{0, 1, 32, fragTestMaxPayload - 1, fragTestMaxPayload, fragTestMaxPayload + 1,
		2 * fragTestMaxPayload, 3000, len(msg) - 1, len(msg)} {
		pub, sub, _ := newFragPair(fragTestTermLen)
		if _, err := pub.OfferParts(msg[:split], msg[split:]); err != nil {
			t.Fatalf("split %d: %v", split, err)
		}
		got := drain(sub)
		if len(got) != 1 || !bytes.Equal(got[0], msg) {
			t.Errorf("split %d: message not delivered intact (%d messages)", split, len(got))
		}
	}

	// Short enough to be a single frame.
	pub, sub, _ := newFragPair(fragTestTermLen)
	if _, err := pub.OfferParts([]byte("head-"), []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if got := drain(sub); len(got) != 1 || string(got[0]) != "head-tail" {
		t.Errorf("single-frame OfferParts delivered %q", got)
	}
}

func TestOfferTooLong(t *testing.T) {
	pub, sub, lb := newFragPair(fragTestTermLen)

	_, err := pub.Offer(make([]byte, fragTestMaxMessage+1))
	if err == nil {
		t.Fatal("no error for a message over MaxMessageLength")
	}
	if !errors.Is(err, ErrMessageTooLong) {
		t.Errorf("error %v does not match ErrMessageTooLong", err)
	}
	var tooLong *MessageTooLongError
	if !errors.As(err, &tooLong) || tooLong.Length != fragTestMaxMessage+1 || tooLong.Max != fragTestMaxMessage || tooLong.Claim {
		t.Errorf("unexpected error detail: %#v", tooLong)
	}

	// OfferParts counts both parts.
	if _, err := pub.OfferParts(make([]byte, 5000), make([]byte, 5000)); !errors.Is(err, ErrMessageTooLong) {
		t.Errorf("OfferParts over the limit: err = %v", err)
	}

	// Rejected means rejected: nothing was claimed or written.
	if tail := lb.Meta().GetInt64Volatile(MetaTermTailCounterOff); rawTailTermOffset(tail, fragTestTermLen) != 0 {
		t.Errorf("tail advanced to %d by a rejected offer", rawTailTermOffset(tail, fragTestTermLen))
	}
	if got := drain(sub); len(got) != 0 {
		t.Errorf("%d messages delivered after only rejected offers", len(got))
	}

	// The limit itself is accepted, and the publication is still usable.
	if _, err := pub.Offer(make([]byte, fragTestMaxMessage)); err != nil {
		t.Errorf("message of exactly MaxMessageLength: %v", err)
	}
}

// The length check is independent of publication state: a caller gets the
// same answer whether or not the stream is currently back pressured.
func TestOfferTooLongIgnoresBackPressure(t *testing.T) {
	pub, _, _ := newFragPair(fragTestTermLen)
	setInMemPosLimit(pub, 0)
	if r, err := pub.Offer([]byte("x")); err != nil || r != BackPressured {
		t.Fatalf("setup: Offer = %d, %v; want BackPressured", r, err)
	}
	if _, err := pub.Offer(make([]byte, fragTestMaxMessage+1)); !errors.Is(err, ErrMessageTooLong) {
		t.Errorf("err = %v, want ErrMessageTooLong", err)
	}
}

func TestOfferFragmentedReturnsOfferStatuses(t *testing.T) {
	msg := patterned(3000, 3)

	pub, _, _ := newFragPair(fragTestTermLen)
	setInMemPosLimit(pub, 0)
	if r, err := pub.Offer(msg); err != nil || r != BackPressured {
		t.Errorf("fragmented offer under back pressure = %d, %v; want BackPressured", r, err)
	}

	pub, _, _ = newFragPair(fragTestTermLen)
	pub.Close()
	if r, err := pub.Offer(msg); err != nil || r != Closed {
		t.Errorf("fragmented offer on closed publication = %d, %v; want Closed", r, err)
	}
}

func TestTryClaimLimits(t *testing.T) {
	pub, _, _ := newFragPair(fragTestTermLen)

	claim, pos, err := pub.TryClaim(fragTestMaxPayload)
	if err != nil || pos <= 0 {
		t.Fatalf("claim of MaxPayloadLength: pos=%d err=%v", pos, err)
	}
	claim.Abort()

	_, _, err = pub.TryClaim(fragTestMaxPayload + 1)
	var tooLong *MessageTooLongError
	if !errors.Is(err, ErrMessageTooLong) || !errors.As(err, &tooLong) || !tooLong.Claim || tooLong.Max != fragTestMaxPayload {
		t.Errorf("claim over MaxPayloadLength: err = %v", err)
	}

	if _, _, err := pub.TryClaim(-1); err == nil {
		t.Error("negative claim length accepted")
	}
}

// TestOfferFragmentedRotatesTerm sends enough fragmented messages to fill and
// rotate terms several times. A message that does not fit in the rest of a
// term must pad it, return AdminAction, and then land whole in the next term.
func TestOfferFragmentedRotatesTerm(t *testing.T) {
	pub, sub, lb := newFragPair(fragTestTermLen)
	const total = 40
	var got [][]byte
	adminActions := 0

	for i := 0; i < total; i++ {
		msg := patterned(7000, byte(i))
		for {
			r, err := pub.Offer(msg)
			if err != nil {
				t.Fatal(err)
			}
			if r == AdminAction {
				adminActions++
				// The media driver zeroes the partition that becomes active
				// next; with no driver the test does it, or the subscriber
				// would keep reading the previous lap's committed frames.
				zeroTerm(lb, int(lb.ActiveTermCount()+1)%PartitionCount, fragTestTermLen)
				continue
			}
			if r < 0 {
				t.Fatalf("message %d: offer = %d", i, r)
			}
			break
		}
		got = append(got, drain(sub)...) // consume each so the 3 terms never wrap unread
	}

	if adminActions < 2 {
		t.Fatalf("only %d rotations in %d messages; the test is not exercising term rotation", adminActions, total)
	}
	if len(got) != total {
		t.Fatalf("delivered %d messages, want %d", len(got), total)
	}
	for i, m := range got {
		if !bytes.Equal(m, patterned(7000, byte(i))) {
			t.Fatalf("message %d corrupted or out of order", i)
		}
	}
}

// TestOfferFragmentedConcurrent: because every fragment of a message is
// claimed in one step, concurrent offerers never interleave fragments, so a
// single assembler per session sees whole messages.
func TestOfferFragmentedConcurrent(t *testing.T) {
	const (
		workers = 4
		each    = 100
		size    = 3000
	)
	// Large enough that all messages fit in term 0: no rotation, no wrap.
	pub, sub, _ := newFragPair(4 * 1024 * 1024)

	var wg sync.WaitGroup
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
				}
			}
		}(w)
	}
	wg.Wait()

	seen := make(map[[2]byte]int)
	for _, m := range drain(sub) {
		if len(m) != size {
			t.Fatalf("delivered a message of %d bytes, want %d", len(m), size)
		}
		w, i := m[0], m[1]
		want := patterned(size, w)
		want[0], want[1] = w, i
		if !bytes.Equal(m, want) {
			t.Fatalf("message from worker %d #%d interleaved or corrupted", w, i)
		}
		seen[[2]byte{w, i}]++
	}
	if len(seen) != workers*each {
		t.Errorf("%d distinct messages delivered, want %d", len(seen), workers*each)
	}
}

func TestClusterOfferLargeMessage(t *testing.T) {
	pub, sub, _ := newFragPair(fragTestTermLen)
	c := &AeronCluster{
		state:            StateConnected,
		sendBuf:          make([]byte, 4096),
		ingressPubs:      []*Publication{pub},
		clusterSessionId: 77,
		leadershipTermId: 5,
	}

	// 4096 and 5000 are the sizes that used to panic the client's staging buffer.
	for _, n := range []int{0, 100, 4000, 4096, 5000, fragTestMaxMessage - clusterSessionHeaderLen} {
		payload := patterned(n, byte(n))
		r, err := c.Offer(payload)
		if err != nil || r <= 0 {
			t.Fatalf("payload %d: Offer = %d, %v", n, r, err)
		}
		got := drain(sub)
		if len(got) != 1 {
			t.Fatalf("payload %d: delivered %d messages", n, len(got))
		}
		if len(got[0]) != clusterSessionHeaderLen+n || !bytes.Equal(got[0][clusterSessionHeaderLen:], payload) {
			t.Fatalf("payload %d: session header + payload not delivered intact (got %d bytes)", n, len(got[0]))
		}
		var smh SessionMessageHeader
		smh.DecodeWithBlockLength(got[0], HeaderSize, sessionMessageHeaderBlockLength)
		if smh.ClusterSessionId != 77 || smh.LeadershipTermId != 5 {
			t.Fatalf("payload %d: session header = %+v", n, smh)
		}
	}

	// The 32-byte session header counts against the publication's limit.
	_, err := c.Offer(make([]byte, fragTestMaxMessage-clusterSessionHeaderLen+1))
	if !errors.Is(err, ErrMessageTooLong) {
		t.Errorf("oversize cluster offer: err = %v, want ErrMessageTooLong", err)
	}

	c.state = StateDisconnected
	if r, err := c.Offer([]byte("x")); err != nil || r != NotConnected {
		t.Errorf("Offer while disconnected = %d, %v; want NotConnected", r, err)
	}
}

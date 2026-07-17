package aeron

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

// offerTestPayloadLen gives frameLen = 132, aligned to 160, so a 4096-byte
// term holds 25 frames (4000 bytes) with a 96-byte padding gap at the end.
const (
	offerTestTermLen       = int32(4 * 1024)
	offerTestPayloadLen    = 100
	offerTestAlignedLen    = int32(160)
	offerTestFramesPerTerm = 25
	offerTestPaddingLen    = offerTestTermLen - offerTestFramesPerTerm*offerTestAlignedLen // 96
)

// frameAt reads the committed frame header fields at an offset in a term.
// The type field is 16 bits, so mask off the neighbouring bytes (mirrors the
// int16 conversion in ReadTerm).
func frameAt(term *AtomicBuffer, offset int32) (frameLen int32, frameType int32, termOffset, termID int32) {
	return term.GetInt32Volatile(offset + FrameLengthOffset),
		term.GetInt32(offset+FrameTypeOffset) & 0xFFFF,
		term.GetInt32(offset + FrameTermOffsetOff),
		term.GetInt32(offset + FrameTermIDOff)
}

func TestOffer_RotatesAtEndOfTerm(t *testing.T) {
	lb := newInMemLogBuffers(offerTestTermLen)
	pub := newInMemPublication(lb, 7, 1001)
	payload := make([]byte, offerTestPayloadLen)

	lastPos := int64(0)
	for i := 0; i < offerTestFramesPerTerm; i++ {
		pos := pub.Offer(payload)
		if pos <= 0 {
			t.Fatalf("offer %d: got %d, want > 0", i, pos)
		}
		if pos <= lastPos {
			t.Fatalf("offer %d: position %d not monotonic (last %d)", i, pos, lastPos)
		}
		lastPos = pos
	}

	// The next offer trips the end of the term: it must rotate and ask the
	// caller to retry.
	if got := pub.Offer(payload); got != AdminAction {
		t.Fatalf("offer at end of term: got %d, want AdminAction", got)
	}

	// A padding frame must fill the remainder of term 0.
	padOffset := offerTestFramesPerTerm * offerTestAlignedLen
	frameLen, frameType, termOffset, termID := frameAt(lb.Term(0), padOffset)
	if frameType != FrameTypePadding {
		t.Errorf("padding frame type: got %#x, want %#x", frameType, FrameTypePadding)
	}
	if frameLen != offerTestPaddingLen {
		t.Errorf("padding frame length: got %d, want %d", frameLen, offerTestPaddingLen)
	}
	if termOffset != padOffset {
		t.Errorf("padding frame termOffset: got %d, want %d", termOffset, padOffset)
	}
	if termID != 0 {
		t.Errorf("padding frame termID: got %d, want 0", termID)
	}

	if got := lb.ActiveTermCount(); got != 1 {
		t.Fatalf("activeTermCount: got %d, want 1", got)
	}

	// The retried offer must land at offset 0 of partition 1 with termID 1.
	pos := pub.Offer(payload)
	wantPos := int64(offerTestTermLen) + int64(offerTestAlignedLen)
	if pos != wantPos {
		t.Fatalf("offer after rotation: got position %d, want %d", pos, wantPos)
	}
	if pos <= lastPos {
		t.Fatalf("position %d not monotonic across boundary (last %d)", pos, lastPos)
	}
	frameLen, frameType, termOffset, termID = frameAt(lb.Term(1), 0)
	if frameType != FrameTypeData {
		t.Errorf("post-rotation frame type: got %#x, want %#x", frameType, FrameTypeData)
	}
	if frameLen != DataFrameHeaderLen+offerTestPayloadLen {
		t.Errorf("post-rotation frame length: got %d, want %d", frameLen, DataFrameHeaderLen+offerTestPayloadLen)
	}
	if termOffset != 0 {
		t.Errorf("post-rotation frame termOffset: got %d, want 0", termOffset)
	}
	if termID != 1 {
		t.Errorf("post-rotation frame termID: got %d, want 1", termID)
	}
}

func TestOffer_FullCycleThroughAllPartitions(t *testing.T) {
	lb := newInMemLogBuffers(offerTestTermLen)
	pub := newInMemPublication(lb, 7, 1001)
	payload := make([]byte, offerTestPayloadLen)
	shift := numberOfTrailingZeros(uint32(offerTestTermLen))

	const wantRotations = 4 // partitions 0 -> 1 -> 2 -> 0 -> 1

	lastPos := int64(0)
	rotations := 0
	for i := 0; lb.ActiveTermCount() < wantRotations; i++ {
		if i > 10_000 {
			t.Fatal("no progress after 10000 offers")
		}
		pos := pub.Offer(payload)
		if pos == AdminAction {
			rotations++
			continue
		}
		if pos <= 0 {
			t.Fatalf("offer: got %d, want > 0", pos)
		}
		if pos <= lastPos {
			t.Fatalf("position %d not monotonic (last %d)", pos, lastPos)
		}
		lastPos = pos

		// Verify the frame just written carries the right termID for its
		// partition.
		frameStart := pos - int64(offerTestAlignedLen)
		termCount := int32(frameStart >> shift)
		frameOffset := int32(frameStart & int64(offerTestTermLen-1))
		term := lb.Term(int(termCount % PartitionCount))
		_, frameType, termOffset, termID := frameAt(term, frameOffset)
		if frameType != FrameTypeData {
			t.Fatalf("frame at pos %d: type %#x, want data", pos, frameType)
		}
		if termID != termCount {
			t.Fatalf("frame at pos %d: termID %d, want %d", pos, termID, termCount)
		}
		if termOffset != frameOffset {
			t.Fatalf("frame at pos %d: termOffset %d, want %d", pos, termOffset, frameOffset)
		}
	}

	if rotations != wantRotations {
		t.Errorf("rotations (ADMIN_ACTION returns): got %d, want %d", rotations, wantRotations)
	}
	if got := lb.ActiveTermCount(); got != wantRotations {
		t.Errorf("activeTermCount: got %d, want %d", got, wantRotations)
	}
}

func TestOffer_ConcurrentOffersAcrossRotation(t *testing.T) {
	lb := newInMemLogBuffers(offerTestTermLen)
	pub := newInMemPublication(lb, 7, 1001)

	// 8 goroutines x 5 messages = 40 frames (6400 bytes): crosses exactly one
	// term boundary (term 0 holds 25 frames).
	const (
		goroutines  = 8
		msgsPerGoro = 5
	)

	positions := make([][]int64, goroutines)
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			payload := make([]byte, offerTestPayloadLen)
			for m := 0; m < msgsPerGoro; m++ {
				ok := false
				for attempt := 0; attempt < 1000; attempt++ {
					pos := pub.Offer(payload)
					if pos == AdminAction {
						continue // rotation: retry
					}
					if pos <= 0 {
						errs <- errOffer(pos)
						return
					}
					positions[g] = append(positions[g], pos)
					ok = true
					break
				}
				if !ok {
					errs <- errOffer(AdminAction)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Per-goroutine positions must be strictly increasing, and every position
	// unique across all goroutines.
	seen := map[int64]bool{}
	for g, ps := range positions {
		if len(ps) != msgsPerGoro {
			t.Fatalf("goroutine %d: got %d positions, want %d", g, len(ps), msgsPerGoro)
		}
		last := int64(0)
		for _, p := range ps {
			if p <= last {
				t.Errorf("goroutine %d: position %d not monotonic (last %d)", g, p, last)
			}
			last = p
			if seen[p] {
				t.Errorf("position %d claimed twice", p)
			}
			seen[p] = true
		}
	}
	if len(seen) != goroutines*msgsPerGoro {
		t.Errorf("unique positions: got %d, want %d", len(seen), goroutines*msgsPerGoro)
	}

	// Exactly one rotation, and exactly one padding frame closing term 0.
	if got := lb.ActiveTermCount(); got != 1 {
		t.Fatalf("activeTermCount: got %d, want 1", got)
	}
	term0 := lb.Term(0)
	paddingFrames := 0
	for offset := int32(0); offset < offerTestTermLen; {
		frameLen, frameType, _, _ := frameAt(term0, offset)
		if frameLen <= 0 {
			t.Fatalf("uncommitted frame at term 0 offset %d", offset)
		}
		if frameType == FrameTypePadding {
			paddingFrames++
			if frameLen != offerTestPaddingLen {
				t.Errorf("padding frame length: got %d, want %d", frameLen, offerTestPaddingLen)
			}
		}
		offset += align(frameLen, DataFrameHeaderLen)
	}
	if paddingFrames != 1 {
		t.Errorf("padding frames in term 0: got %d, want 1", paddingFrames)
	}
}

func TestOffer_BackPressuredAtPositionLimit(t *testing.T) {
	lb := newInMemLogBuffers(offerTestTermLen)
	pub := newInMemPublication(lb, 7, 1001)
	payload := make([]byte, offerTestPayloadLen)

	// position (0) >= limit (0): back-pressured.
	setInMemPosLimit(pub, 0)
	if got := pub.Offer(payload); got != BackPressured {
		t.Fatalf("offer with limit 0: got %d, want BackPressured", got)
	}

	// position (0) < limit (1): the offer proceeds.
	setInMemPosLimit(pub, 1)
	pos := pub.Offer(payload)
	if pos != int64(offerTestAlignedLen) {
		t.Fatalf("offer with limit 1: got %d, want %d", pos, offerTestAlignedLen)
	}

	// position (160) >= limit (1): back-pressured again.
	if got := pub.Offer(payload); got != BackPressured {
		t.Fatalf("offer past limit: got %d, want BackPressured", got)
	}

	// Raising the limit unblocks the publication.
	setInMemPosLimit(pub, math.MaxInt64)
	if got := pub.Offer(payload); got != pos+int64(offerTestAlignedLen) {
		t.Fatalf("offer after raising limit: got %d, want %d", got, pos+int64(offerTestAlignedLen))
	}
}

func TestOffer_MaxPositionExceeded(t *testing.T) {
	// maxPossiblePosition = termLen << 31. Doctor the log so the stream sits
	// on the final possible term: termCount = termID - initialTermID =
	// math.MaxInt32, which puts positions at (2^31 - 1) * termLen + tail.
	const maxTermCount = int32(math.MaxInt32)
	maxPos := maxPossiblePosition(offerTestTermLen)

	tests := []struct {
		name       string
		tailOffset int32
		limit      int64
		want       int64
	}{
		{
			// position + alignedFrameLen == maxPossiblePosition: the
			// back-pressure branch must report the terminal condition.
			name:       "back pressured claim reaching max position",
			tailOffset: offerTestTermLen - offerTestAlignedLen,
			limit:      0,
			want:       MaxPositionExceeded,
		},
		{
			// Same term but with room below maxPossiblePosition: plain
			// back pressure.
			name:       "back pressured claim below max position",
			tailOffset: 0,
			limit:      0,
			want:       BackPressured,
		},
		{
			// The claim trips the end of the final term: handleEndOfLog must
			// return MaxPositionExceeded instead of rotating.
			name:       "end of log on final term",
			tailOffset: offerTestTermLen - offerTestAlignedLen + 32,
			limit:      math.MaxInt64,
			want:       MaxPositionExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lb := newInMemLogBuffers(offerTestTermLen)
			pub := newInMemPublication(lb, 7, 1001)
			setInMemPosLimit(pub, tc.limit)

			index := int32(maxTermCount % PartitionCount)
			lb.meta.PutInt32Ordered(MetaActiveTermCountOff, maxTermCount)
			lb.meta.PutInt64Ordered(int32(MetaTermTailCounterOff)+index*8,
				packTail(maxTermCount, tc.tailOffset))

			payload := make([]byte, offerTestPayloadLen)
			if got := pub.Offer(payload); got != tc.want {
				t.Fatalf("offer near max position %d: got %d, want %d", maxPos, got, tc.want)
			}

			// A publication at max position is unrecoverable: the log must
			// never rotate past the final term.
			if got := lb.ActiveTermCount(); got != maxTermCount {
				t.Errorf("activeTermCount: got %d, want %d (no rotation)", got, maxTermCount)
			}
		})
	}
}

func TestPoll_FollowsPublisherAcrossTermBoundary(t *testing.T) {
	lb := newInMemLogBuffers(offerTestTermLen)
	pub := newInMemPublication(lb, 7, 1001)
	sub := newInMemSubscription(lb, 1001)

	// 30 messages: 25 fill term 0, then padding, then 5 land in term 1.
	const total = 30
	for i := 0; i < total; i++ {
		payload := make([]byte, offerTestPayloadLen)
		payload[0] = byte(i)
		ok := false
		for attempt := 0; attempt < 10; attempt++ {
			if pos := pub.Offer(payload); pos > 0 {
				ok = true
				break
			}
		}
		if !ok {
			t.Fatalf("failed to offer message %d", i)
		}
	}

	var got []byte
	var termIDs []int32
	for i := 0; i < 10 && len(got) < total; i++ {
		sub.Poll(func(buf []byte, h *Header) {
			got = append(got, buf[0])
			termIDs = append(termIDs, h.TermID)
		}, 100)
	}

	if len(got) != total {
		t.Fatalf("received %d messages, want %d", len(got), total)
	}
	for i := 0; i < total; i++ {
		if got[i] != byte(i) {
			t.Fatalf("message %d: got payload marker %d, want %d", i, got[i], i)
		}
		wantTermID := int32(0)
		if i >= offerTestFramesPerTerm {
			wantTermID = 1
		}
		if termIDs[i] != wantTermID {
			t.Errorf("message %d: got termID %d, want %d", i, termIDs[i], wantTermID)
		}
	}

	img := sub.conductor.subscriptions[sub.registrationID].images[0]
	wantPos := int64(offerTestTermLen) + int64(total-offerTestFramesPerTerm)*int64(offerTestAlignedLen)
	if img.subscriberPosition != wantPos {
		t.Errorf("subscriberPosition: got %d, want %d", img.subscriberPosition, wantPos)
	}
}

type errOffer int64

func (e errOffer) Error() string {
	return fmt.Sprintf("offer failed with %d", int64(e))
}

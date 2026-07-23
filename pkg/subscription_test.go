package aeron

import "testing"

// TestPoll_PublishesSubscriberPositionCounter verifies that every position
// advance made by Poll — fragment reads and the padding-only advance across a
// term boundary — is published to the driver's subscriber position counter.
// Without this the driver treats the subscriber as stuck at its join position
// and flow control stalls the publication at joinPosition + termLength/2.
func TestPoll_PublishesSubscriberPositionCounter(t *testing.T) {
	lb := newInMemLogBuffers(offerTestTermLen)
	pub := newInMemPublication(lb, 7, 1001)
	sub := newInMemSubscription(lb, 1001)

	const counterID = int32(3)
	counterValues := setInMemSubscriberPosCounter(sub, counterID)
	counterSlot := counterID * CounterValueLength
	img := sub.conductor.subscriptions[sub.registrationID].images[0]

	// 30 messages: 25 fill term 0, then a padding frame, then 5 land in term 1.
	const total = offerTestFramesPerTerm + 5
	payload := make([]byte, offerTestPayloadLen)
	for i := 0; i < total; i++ {
		if pos := pub.OfferWithRetry(payload, 10); pos < 0 {
			t.Fatalf("failed to offer message %d: %d", i, pos)
		}
	}

	noop := func(buf []byte, h *Header) {}

	assertPositions := func(step string, wantPos int64) {
		t.Helper()
		if img.subscriberPosition != wantPos {
			t.Fatalf("%s: subscriberPosition got %d, want %d", step, img.subscriberPosition, wantPos)
		}
		if got := counterValues.GetInt64Volatile(counterSlot); got != wantPos {
			t.Fatalf("%s: subscriber position counter got %d, want %d", step, got, wantPos)
		}
	}

	// Fragment reads within term 0.
	if got := sub.Poll(noop, 10); got != 10 {
		t.Fatalf("poll 1: got %d fragments, want 10", got)
	}
	assertPositions("after poll 1", 10*int64(offerTestAlignedLen))

	if got := sub.Poll(noop, offerTestFramesPerTerm-10); got != offerTestFramesPerTerm-10 {
		t.Fatalf("poll 2: got %d fragments, want %d", got, offerTestFramesPerTerm-10)
	}
	assertPositions("after poll 2", int64(offerTestFramesPerTerm)*int64(offerTestAlignedLen))

	// Padding-only advance across the term boundary: zero fragments, but the
	// position — and therefore the counter — must still move to term 1.
	if got := sub.Poll(noop, 100); got != 0 {
		t.Fatalf("padding-only poll: got %d fragments, want 0", got)
	}
	assertPositions("after padding-only poll", int64(offerTestTermLen))

	// Fragment reads in term 1.
	const term1Frames = total - offerTestFramesPerTerm
	if got := sub.Poll(noop, 100); got != term1Frames {
		t.Fatalf("poll 4: got %d fragments, want %d", got, term1Frames)
	}
	assertPositions("after poll 4",
		int64(offerTestTermLen)+int64(term1Frames)*int64(offerTestAlignedLen))
}

// TestPoll_NoCounterConfigured verifies that Poll still advances the internal
// position without panicking when no counter values buffer is attached (the
// in-memory fixtures) or when the counter ID is negative.
func TestPoll_NoCounterConfigured(t *testing.T) {
	noop := func(buf []byte, h *Header) {}
	const frames = 5

	t.Run("nil buffer", func(t *testing.T) {
		lb := newInMemLogBuffers(offerTestTermLen)
		pub := newInMemPublication(lb, 7, 1001)
		sub := newInMemSubscription(lb, 1001)
		img := sub.conductor.subscriptions[sub.registrationID].images[0]

		payload := make([]byte, offerTestPayloadLen)
		for i := 0; i < frames; i++ {
			if pos := pub.OfferWithRetry(payload, 10); pos < 0 {
				t.Fatalf("failed to offer message %d: %d", i, pos)
			}
		}

		if got := sub.Poll(noop, 100); got != frames {
			t.Fatalf("poll: got %d fragments, want %d", got, frames)
		}
		if want := int64(frames) * int64(offerTestAlignedLen); img.subscriberPosition != want {
			t.Fatalf("subscriberPosition: got %d, want %d", img.subscriberPosition, want)
		}
	})

	t.Run("negative counter ID", func(t *testing.T) {
		lb := newInMemLogBuffers(offerTestTermLen)
		pub := newInMemPublication(lb, 7, 1001)
		sub := newInMemSubscription(lb, 1001)
		img := sub.conductor.subscriptions[sub.registrationID].images[0]
		img.SubscriberPos = -1
		img.counterValues = NewAtomicBuffer(make([]byte, CounterValueLength))

		payload := make([]byte, offerTestPayloadLen)
		for i := 0; i < frames; i++ {
			if pos := pub.OfferWithRetry(payload, 10); pos < 0 {
				t.Fatalf("failed to offer message %d: %d", i, pos)
			}
		}

		if got := sub.Poll(noop, 100); got != frames {
			t.Fatalf("poll: got %d fragments, want %d", got, frames)
		}
		if want := int64(frames) * int64(offerTestAlignedLen); img.subscriberPosition != want {
			t.Fatalf("subscriberPosition: got %d, want %d", img.subscriberPosition, want)
		}
		if got := img.counterValues.GetInt64Volatile(0); got != 0 {
			t.Fatalf("counter slot written despite negative counter ID: got %d, want 0", got)
		}
	})
}

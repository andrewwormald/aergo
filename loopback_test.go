package aergo

import "testing"

func TestLoopback_OfferThenPoll(t *testing.T) {
	lb := NewLoopbackLogBuffers(64 * 1024)
	pub := NewLoopbackPublication(lb, 7, 1001)
	sub := NewLoopbackSubscription(lb, 1001)

	payload := []byte{1, 2, 3, 4}
	if pos := pub.Offer(payload); pos <= 0 {
		t.Fatalf("Offer: got %d, want > 0", pos)
	}

	var got []byte
	n := sub.Poll(func(buf []byte, h *Header) {
		got = append(got, buf...)
	}, 10)
	if n != 1 {
		t.Fatalf("Poll: got %d fragments, want 1", n)
	}
	if len(got) != len(payload) {
		t.Fatalf("payload length: got %d, want %d", len(got), len(payload))
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Fatalf("payload byte %d: got %d, want %d", i, got[i], payload[i])
		}
	}
}

func TestLoopback_TryClaimThenPoll(t *testing.T) {
	lb := NewLoopbackLogBuffers(64 * 1024)
	pub := NewLoopbackPublication(lb, 7, 1001)
	sub := NewLoopbackSubscription(lb, 1001)

	claim, pos := pub.TryClaim(4)
	if pos <= 0 {
		t.Fatalf("TryClaim: got %d, want > 0", pos)
	}
	copy(claim.Buffer(), []byte{9, 8, 7, 6})
	claim.Commit()

	var got []byte
	n := sub.Poll(func(buf []byte, h *Header) {
		got = append(got, buf...)
	}, 10)
	if n != 1 {
		t.Fatalf("Poll: got %d fragments, want 1", n)
	}
	want := []byte{9, 8, 7, 6}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("payload byte %d: got %d, want %d", i, got[i], want[i])
		}
	}
}

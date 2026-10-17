package aergo

import (
	"encoding/binary"
	"testing"
	"time"
)

// Poll must not take the conductor lock: two subscriptions polled from two
// goroutines (a ping-pong client) would otherwise contend on it on every call.
func TestPollDoesNotTakeTheConductorLock(t *testing.T) {
	lb := newInMemLogBuffers(64 * 1024)
	pub := newInMemPublication(lb, 1, 1001)
	sub := newInMemSubscription(lb, 1001)
	for i := 0; i < 3; i++ {
		if _, err := pub.Offer([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}

	sub.conductor.mu.Lock()
	done := make(chan int, 1)
	go func() { done <- sub.Poll(func([]byte, *Header) {}, 10) }()
	select {
	case n := <-done:
		if n != 3 {
			t.Errorf("Poll read %d fragments, want 3", n)
		}
	case <-time.After(2 * time.Second):
		t.Error("Poll blocked while the conductor lock was held")
	}
	sub.conductor.mu.Unlock()
}

// An image the conductor removes must stop being polled.
func TestPollStopsReadingAnImageOnceUnavailable(t *testing.T) {
	lb := newInMemLogBuffers(64 * 1024)
	pub := newInMemPublication(lb, 1, 1001)
	sub := newInMemSubscription(lb, 1001)
	if _, err := pub.Offer([]byte("x")); err != nil {
		t.Fatal(err)
	}

	msg := make([]byte, 24)
	binary.LittleEndian.PutUint64(msg[0:], 0) // the image's correlation ID
	binary.LittleEndian.PutUint64(msg[16:], uint64(sub.registrationID))
	sub.conductor.onUnavailableImage(msg)

	if n := sub.Poll(func([]byte, *Header) {}, 10); n != 0 {
		t.Errorf("Poll read %d fragments from a removed image", n)
	}
}

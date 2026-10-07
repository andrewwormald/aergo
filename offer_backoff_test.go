package aergo

import (
	"context"
	"testing"
	"time"
)

// A publication that is connected but whose driver has not yet raised the
// limit (position 0, limit 0) must succeed once the limit rises.
func TestOfferWithBackoffRidesOutLimitGap(t *testing.T) {
	pub := newInMemPublication(newInMemLogBuffers(offerTestTermLen), 1, 1)
	setInMemPosLimit(pub, 0)

	if got := pub.Offer([]byte("x")); got != BackPressured {
		t.Fatalf("precondition: Offer with limit 0 = %d, want BackPressured", got)
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		setInMemPosLimit(pub, int64(offerTestTermLen))
	}()

	if got := pub.OfferWithBackoff(context.Background(), []byte("x"), 2*time.Second); got <= 0 {
		t.Fatalf("OfferWithBackoff = %d, want a positive position", got)
	}
}

func TestOfferWithBackoffGivesUpAtTimeout(t *testing.T) {
	pub := newInMemPublication(newInMemLogBuffers(offerTestTermLen), 1, 1)
	setInMemPosLimit(pub, 0)

	start := time.Now()
	got := pub.OfferWithBackoff(context.Background(), []byte("x"), 50*time.Millisecond)
	if got != BackPressured {
		t.Errorf("OfferWithBackoff = %d, want BackPressured", got)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("returned after %s, before the 50ms timeout", elapsed)
	}
}

func TestOfferWithBackoffStopsWhenContextEnds(t *testing.T) {
	pub := newInMemPublication(newInMemLogBuffers(offerTestTermLen), 1, 1)
	setInMemPosLimit(pub, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if got := pub.OfferWithBackoff(ctx, []byte("x"), 10*time.Second); got != BackPressured {
		t.Errorf("OfferWithBackoff = %d, want BackPressured", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %s with a cancelled context", elapsed)
	}
}

// A closed publication is not retryable: it must return at once.
func TestOfferWithBackoffDoesNotRetryGone(t *testing.T) {
	pub := newInMemPublication(newInMemLogBuffers(offerTestTermLen), 1, 1)
	pub.closed.Store(true) // Close needs a conductor, which a loopback publication has none of

	start := time.Now()
	if got := pub.OfferWithBackoff(context.Background(), []byte("x"), 10*time.Second); got != Closed {
		t.Errorf("OfferWithBackoff = %d, want Closed", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %s on a closed publication", elapsed)
	}
}

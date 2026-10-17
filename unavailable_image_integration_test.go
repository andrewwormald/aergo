package aergo

import (
	"os"
	"testing"
	"time"
)

// TestImageGoesAwayWhenItsPublicationCloses runs against a real media driver
// (set AERGO_TEST_AERON_DIR to its directory; skipped otherwise). A subscriber
// must lose the image, and be told, when the publication it was reading closes.
// The driver keeps the publication a few seconds after the close, so this takes
// 5 to 15 seconds.
func TestImageGoesAwayWhenItsPublicationCloses(t *testing.T) {
	dir := os.Getenv("AERGO_TEST_AERON_DIR")
	if dir == "" {
		t.Skip("AERGO_TEST_AERON_DIR not set")
	}
	client, err := Connect(WithDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	pub, err := client.AddPublication("aeron:ipc", 4242)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := client.AddSubscription("aeron:ipc", 4242)
	if err != nil {
		t.Fatal(err)
	}
	ended := make(chan int32, 4)
	sub.SetUnavailableImageHandler(func(session int32) { ended <- session })

	images := func() int { return len(*sub.state.snapshot.Load()) }
	waitFor := func(what string, timeout time.Duration, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			client.DoWork()
			time.Sleep(time.Millisecond)
		}
	}

	waitFor("the image to appear", 10*time.Second, func() bool { return images() == 1 })

	pub.Close()
	waitFor("the image to go away", 30*time.Second, func() bool { return images() == 0 })

	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Error("the unavailable-image handler was not called")
	}
}

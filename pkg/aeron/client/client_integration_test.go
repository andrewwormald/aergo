// +build integration

package client_test

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/andrewwormald/aergo/pkg/aeron/client"
	"github.com/andrewwormald/aergo/pkg/aeron/driver"
)

var libPath = flag.String("aeron-lib", "", "path to libaeron.so/dylib")

func TestMain(m *testing.M) {
	flag.Parse()
	if *libPath == "" {
		// Try default paths
		for _, p := range []string{
			"./build/aeron/lib/libaeron.dylib",
			"../../../build/aeron/lib/libaeron.dylib",
			"libaeron.dylib",
			"libaeron.so",
		} {
			if _, err := os.Stat(p); err == nil {
				*libPath = p
				break
			}
		}
	}
	if *libPath == "" {
		os.Exit(0) // skip if no library found
	}
	if err := driver.Open(*libPath); err != nil {
		panic(err)
	}
	defer driver.Close()
	os.Exit(m.Run())
}

func TestPubSubRoundTrip(t *testing.T) {
	c, err := client.New()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer c.Close()

	channel := "aeron:udp?endpoint=localhost:40200"
	pub, err := c.AddPublication(channel, 2001)
	if err != nil {
		t.Fatalf("add publication: %v", err)
	}
	defer pub.Close()

	sub, err := c.AddSubscription(channel, 2001)
	if err != nil {
		t.Fatalf("add subscription: %v", err)
	}
	defer sub.Close()

	deadline := time.Now().Add(5 * time.Second)
	for !pub.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("publication connect timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}

	msg := []byte("integration-test-message")
	result := pub.OfferWithBackpressure(msg, client.BackpressureYield, 100)
	if result < 0 {
		t.Fatalf("offer failed: %d", result)
	}

	received := false
	deadline = time.Now().Add(5 * time.Second)
	for !received && time.Now().Before(deadline) {
		sub.Poll(func(buffer []byte, header *client.Header) {
			if string(buffer) != string(msg) {
				t.Errorf("message mismatch: got %q want %q", buffer, msg)
			}
			// Verify header is populated
			if header == nil {
				t.Error("header is nil")
			} else {
				if header.StreamId != 2001 {
					t.Errorf("StreamId: got %d want 2001", header.StreamId)
				}
				if header.FrameLength <= 0 {
					t.Errorf("FrameLength: got %d, want > 0", header.FrameLength)
				}
				if header.Position <= 0 {
					t.Errorf("Position: got %d, want > 0", header.Position)
				}
				if !header.IsUnfragmented() {
					t.Errorf("expected unfragmented, flags=0x%02X", header.Flags)
				}
				t.Logf("header: stream=%d session=%d pos=%d len=%d flags=0x%02X",
					header.StreamId, header.SessionId, header.Position, header.FrameLength, header.Flags)
			}
			received = true
		}, 10)
		if !received {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !received {
		t.Fatal("no message received")
	}
}

func TestTryClaimRoundTrip(t *testing.T) {
	c, err := client.New()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer c.Close()

	channel := "aeron:udp?endpoint=localhost:40201"
	pub, err := c.AddPublication(channel, 2002)
	if err != nil {
		t.Fatalf("add publication: %v", err)
	}
	defer pub.Close()

	sub, err := c.AddSubscription(channel, 2002)
	if err != nil {
		t.Fatalf("add subscription: %v", err)
	}
	defer sub.Close()

	deadline := time.Now().Add(5 * time.Second)
	for !pub.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("connect timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}

	msg := []byte("tryclaim-test")
	var claim *client.BufferClaim
	var pos int64
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		claim, pos = pub.TryClaim(len(msg))
		if pos > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if pos < 0 {
		t.Fatalf("tryclaim failed: %d", pos)
	}

	copy(claim.Buffer(), msg)
	if err := claim.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	received := false
	deadline = time.Now().Add(5 * time.Second)
	for !received && time.Now().Before(deadline) {
		sub.Poll(func(buffer []byte, header *client.Header) {
			if string(buffer) != string(msg) {
				t.Errorf("mismatch: got %q want %q", buffer, msg)
			}
			received = true
		}, 10)
		if !received {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !received {
		t.Fatal("no message received")
	}
}

func TestExclusivePublicationRoundTrip(t *testing.T) {
	c, err := client.New()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer c.Close()

	channel := "aeron:udp?endpoint=localhost:40202"
	pub, err := c.AddExclusivePublication(channel, 2003)
	if err != nil {
		t.Fatalf("add exclusive publication: %v", err)
	}
	defer pub.Close()

	sub, err := c.AddSubscription(channel, 2003)
	if err != nil {
		t.Fatalf("add subscription: %v", err)
	}
	defer sub.Close()

	deadline := time.Now().Add(5 * time.Second)
	for !pub.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("connect timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}

	msg := []byte("exclusive-pub-test")
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		result := pub.Offer(msg)
		if result > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	received := false
	deadline = time.Now().Add(5 * time.Second)
	for !received && time.Now().Before(deadline) {
		sub.Poll(func(buffer []byte, header *client.Header) {
			if string(buffer) != string(msg) {
				t.Errorf("mismatch: got %q want %q", buffer, msg)
			}
			received = true
		}, 10)
		if !received {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !received {
		t.Fatal("no message received")
	}
}

func BenchmarkOfferPoll(b *testing.B) {
	c, err := client.New()
	if err != nil {
		b.Fatalf("new client: %v", err)
	}
	defer c.Close()

	channel := "aeron:udp?endpoint=localhost:40210"
	pub, err := c.AddPublication(channel, 3001)
	if err != nil {
		b.Fatalf("add publication: %v", err)
	}
	defer pub.Close()

	sub, err := c.AddSubscription(channel, 3001)
	if err != nil {
		b.Fatalf("add subscription: %v", err)
	}
	defer sub.Close()

	for !pub.IsConnected() {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	msg := make([]byte, 64)
	copy(msg, "benchmark-message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			if pub.Offer(msg) > 0 {
				break
			}
		}
		for {
			if sub.Poll(func([]byte, *client.Header) {}, 1) > 0 {
				break
			}
		}
	}
}

func BenchmarkOfferPollBound(b *testing.B) {
	c, err := client.New()
	if err != nil {
		b.Fatalf("new client: %v", err)
	}
	defer c.Close()

	channel := "aeron:udp?endpoint=localhost:40215"
	pub, err := c.AddPublication(channel, 3005)
	if err != nil {
		b.Fatalf("add publication: %v", err)
	}
	defer pub.Close()

	sub, err := c.AddSubscription(channel, 3005)
	if err != nil {
		b.Fatalf("add subscription: %v", err)
	}
	defer sub.Close()

	for !pub.IsConnected() {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	msg := make([]byte, 64)
	copy(msg, "benchmark-bound")

	// Bind handler once -- zero-alloc polling
	sub.Bind(func([]byte, *client.Header) {})
	defer sub.Unbind()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			if pub.Offer(msg) > 0 {
				break
			}
		}
		for {
			if sub.PollBound(1) > 0 {
				break
			}
		}
	}
}

func BenchmarkTryClaim(b *testing.B) {
	c, err := client.New()
	if err != nil {
		b.Fatalf("new client: %v", err)
	}
	defer c.Close()

	channel := "aeron:udp?endpoint=localhost:40211"
	pub, err := c.AddPublication(channel, 3002)
	if err != nil {
		b.Fatalf("add publication: %v", err)
	}
	defer pub.Close()

	sub, err := c.AddSubscription(channel, 3002)
	if err != nil {
		b.Fatalf("add subscription: %v", err)
	}
	defer sub.Close()

	for !pub.IsConnected() {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	msg := []byte("benchmark-tryclaim-msg")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var claim *client.BufferClaim
		var pos int64
		for {
			claim, pos = pub.TryClaim(len(msg))
			if pos > 0 {
				break
			}
		}
		copy(claim.Buffer(), msg)
		claim.Commit()

		for {
			if sub.Poll(func([]byte, *client.Header) {}, 1) > 0 {
				break
			}
		}
	}
}

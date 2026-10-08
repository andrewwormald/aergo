package aergo

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
)

// Integration tests against a real Aeron Archive that is recording a live
// stream with traffic on it. They are skipped unless AERGO_ARCHIVE_IT=1.
//
// Environment:
//
//	AERGO_ARCHIVE_IT=1
//	AERGO_DIR                 media driver directory the client attaches to
//	AERGO_ARCHIVE_REQUEST     archive control request channel
//	AERGO_ARCHIVE_RESPONSE    control response channel (a local endpoint)
//	AERGO_LIVE_CHANNEL        live subscription channel
//	AERGO_LIVE_STREAM         live stream id
//	AERGO_LIVE_SESSION        live publication session id
//	AERGO_RECORDING_CHANNEL   channel fragment the recording was made with
//	AERGO_REPLAY_ENDPOINT     base replay channel, e.g. aeron:udp?endpoint=localhost:20127
//	                          (the tests use this stream ids 4401 and up)
type archiveIT struct {
	t         *testing.T
	ac        *Aeron
	a         *Archive
	live      *Subscription
	recording int64
	replayCh  string
}

type itFrame struct {
	payload []byte
	length  int32
}

func itEnv(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Fatalf("%s is required", key)
	}
	return v
}

func newArchiveIT(t *testing.T) *archiveIT {
	t.Helper()
	if os.Getenv("AERGO_ARCHIVE_IT") != "1" {
		t.Skip("set AERGO_ARCHIVE_IT=1 and the AERGO_* variables to run against a real archive")
	}
	ac, err := Connect(WithDir(itEnv(t, "AERGO_DIR")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ac.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, err := ConnectArchive(ctx, ac, ArchiveConfig{
		ControlRequestChannel:  itEnv(t, "AERGO_ARCHIVE_REQUEST"),
		ControlResponseChannel: itEnv(t, "AERGO_ARCHIVE_RESPONSE"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })

	stream, _ := strconv.Atoi(itEnv(t, "AERGO_LIVE_STREAM"))
	session, _ := strconv.Atoi(itEnv(t, "AERGO_LIVE_SESSION"))
	live, err := ac.AddSubscription(itEnv(t, "AERGO_LIVE_CHANNEL"), int32(stream))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := a.FindLastMatchingRecording(ctx, 0, itEnv(t, "AERGO_RECORDING_CHANNEL"), int32(stream), int32(session))
	if err != nil {
		t.Fatal(err)
	}
	return &archiveIT{t: t, ac: ac, a: a, live: live, recording: rec, replayCh: itEnv(t, "AERGO_REPLAY_ENDPOINT")}
}

// drain polls sub until done returns true or the timeout passes.
func (it *archiveIT) drain(sub *Subscription, timeout time.Duration, h FragmentHandler, done func() bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && !done() {
		it.ac.DoWork()
		if sub.Poll(h, 64) == 0 {
			time.Sleep(50 * time.Microsecond)
		}
	}
}

func frameEnd(h *Header) int64 { return h.Position }

func frameStartOf(h *Header) int64 {
	return h.Position - int64((h.FrameLength+31)&^31)
}

// Frames replayed from the archive equal the frames received live, byte for
// byte, at the same stream positions.
func TestArchiveIntegrationReplayEqualsLive(t *testing.T) {
	it := newArchiveIT(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	live := map[int64]itFrame{}
	var first, last int64 = -1, 0
	it.drain(it.live, 10*time.Second, func(b []byte, h *Header) {
		if first < 0 {
			first = frameStartOf(h)
		}
		live[frameEnd(h)] = itFrame{append([]byte(nil), b...), h.FrameLength}
		last = frameEnd(h)
	}, func() bool { return len(live) >= 1000 })
	if len(live) == 0 {
		t.Fatal("no live traffic; start a load generator")
	}
	time.Sleep(2 * time.Second) // let the archive record what we saw

	sub, id, err := it.a.Replay(ctx, it.recording, first, last-first, it.replayCh, 4401)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	defer it.a.StopReplay(ctx, id)

	replayed := map[int64]itFrame{}
	var got int64
	it.drain(sub, 30*time.Second, func(b []byte, h *Header) {
		replayed[frameEnd(h)] = itFrame{append([]byte(nil), b...), h.FrameLength}
		got = frameEnd(h)
	}, func() bool { return got >= last })

	if len(replayed) != len(live) {
		t.Fatalf("replayed %d frames, live %d", len(replayed), len(live))
	}
	for pos, lf := range live {
		rf, ok := replayed[pos]
		if !ok {
			t.Fatalf("position %d missing from replay", pos)
		}
		if rf.length != lf.length || !bytes.Equal(rf.payload, lf.payload) {
			t.Fatalf("position %d differs between replay and live", pos)
		}
	}
	t.Logf("%d frames identical", len(live))
}

// A replay that follows the live recording keeps delivering frames appended
// after the replay started.
func TestArchiveIntegrationReplayFollowsLive(t *testing.T) {
	it := newArchiveIT(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	start, err := it.a.MaxRecordedPosition(ctx, it.recording)
	if err != nil {
		t.Fatal(err)
	}
	sub, id, err := it.a.Replay(ctx, it.recording, start, ArchiveReplayAllAndFollow, it.replayCh, 4402)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	defer it.a.StopReplay(ctx, id)

	var frames int
	var prev int64 = start
	var holes int
	it.drain(sub, 15*time.Second, func(b []byte, h *Header) {
		if frameStartOf(h) != prev {
			holes++
		}
		prev = frameEnd(h)
		frames++
	}, func() bool { return frames >= 500 })

	if frames < 500 {
		t.Fatalf("followed only %d frames past the start; is traffic running?", frames)
	}
	if holes != 0 {
		t.Fatalf("%d position holes while following", holes)
	}
	end, _ := it.a.MaxRecordedPosition(ctx, it.recording)
	if prev <= start || prev > end {
		t.Fatalf("followed to %d, recording spans %d..%d", prev, start, end)
	}
}

// Start positions the archive cannot serve come back as an ArchiveError, not
// a hang.
func TestArchiveIntegrationBadReplayPositions(t *testing.T) {
	it := newArchiveIT(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	max, err := it.a.MaxRecordedPosition(ctx, it.recording)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]int64{
		"unaligned":      33,
		"beyond the end": max + 1<<30,
	}
	for name, pos := range cases {
		t.Run(name, func(t *testing.T) {
			id, err := it.a.StartReplay(ctx, it.recording, pos, 1024, it.replayCh, 4403)
			if err == nil {
				_ = it.a.StopReplay(ctx, id)
				t.Fatalf("replay from %d started", pos)
			}
			var ae *ArchiveError
			if !errors.As(err, &ae) {
				t.Fatalf("got %T %v, want *ArchiveError", err, err)
			}
			t.Logf("%s: %v", name, err)
		})
	}

	// The session stays usable after a rejected request.
	if _, err := it.a.MaxRecordedPosition(ctx, it.recording); err != nil {
		t.Fatalf("archive unusable after bad position: %v", err)
	}
}

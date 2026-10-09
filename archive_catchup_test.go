package aergo

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

// Test frames are 64 bytes of payload, so each is a 96-byte frame and frame k
// (counting from 1) ends at position 96*k. The payload starts with k, so a
// test can tell which frame it got.
const (
	testFrameBytes   = 96
	testPayloadBytes = 64
)

func testFrame(k int64) catchUpFrame {
	payload := make([]byte, testPayloadBytes)
	binary.BigEndian.PutUint64(payload, uint64(k))
	return catchUpFrame{
		header:  Header{FrameLength: DataFrameHeaderLen + testPayloadBytes, Position: k * testFrameBytes, TermOffset: int32((k - 1) * testFrameBytes)},
		payload: payload,
	}
}

// scriptedPoller hands out queued frames, at most perPoll per Poll, and lets a
// test add frames as time passes by hooking each poll.
type scriptedPoller struct {
	queue   []catchUpFrame
	perPoll int
	onPoll  func(p *scriptedPoller)
	closed  bool
}

func (p *scriptedPoller) add(k int64) { p.queue = append(p.queue, testFrame(k)) }

func (p *scriptedPoller) Poll(handler FragmentHandler, limit int) int {
	if p.onPoll != nil {
		p.onPoll(p)
	}
	n := 0
	for n < limit && n < p.perPoll && len(p.queue) > 0 {
		f := p.queue[0]
		p.queue = p.queue[1:]
		h := f.header
		handler(f.payload, &h)
		n++
	}
	return n
}

func (p *scriptedPoller) Close() { p.closed = true }

// liveSource produces one new live frame per poll, from first to last.
func liveSource(first, last int64, perPoll int) *scriptedPoller {
	p := &scriptedPoller{perPoll: 1000}
	next := first
	p.onPoll = func(p *scriptedPoller) {
		for i := 0; i < perPoll && next <= last; i++ {
			p.add(next)
			next++
		}
	}
	return p
}

func replayOf(first, last int64, perPoll int) *scriptedPoller {
	p := &scriptedPoller{perPoll: perPoll}
	for k := first; k <= last; k++ {
		p.add(k)
	}
	return p
}

type delivered struct{ ks []int64 }

func (d *delivered) handler(t *testing.T) FragmentHandler {
	return func(buf []byte, h *Header) {
		k := int64(binary.BigEndian.Uint64(buf))
		if h.Position != k*testFrameBytes {
			t.Errorf("frame %d delivered with position %d", k, h.Position)
		}
		d.ks = append(d.ks, k)
	}
}

// requireContiguous checks the frames are first, first+1, ... with no gap and
// no repeat.
func requireContiguous(t *testing.T, ks []int64, first int64) {
	t.Helper()
	for i, k := range ks {
		if k != first+int64(i) {
			t.Fatalf("frame %d of the output is %d, want %d (output so far %v)", i, k, first+int64(i), ks)
		}
	}
}

func newTestCatchUp(live, replay *scriptedPoller, cfg ArchiveCatchUpConfig, stops *int) *ArchiveCatchUp {
	return newArchiveCatchUp(live, replay, func() error { *stops++; return nil }, cfg)
}

// pollUntil polls until done() or the loop limit, failing on an error.
func pollUntil(t *testing.T, c *ArchiveCatchUp, d *delivered, done func() bool) {
	t.Helper()
	for i := 0; i < 10000 && !done(); i++ {
		if _, err := c.Poll(d.handler(t), 8); err != nil {
			t.Fatalf("poll: %v", err)
		}
	}
	if !done() {
		t.Fatalf("not done after the poll limit; delivered %d frames, merged=%v", len(d.ks), c.Merged())
	}
}

func TestCatchUpMergesTheReplayIntoTheLiveStream(t *testing.T) {
	live := liveSource(25, 90, 2) // joined at frame 25, then two new frames per poll
	replay := replayOf(1, 40, 5)
	var stops int
	c := newTestCatchUp(live, replay, ArchiveCatchUpConfig{StartPosition: 0}, &stops)
	d := &delivered{}

	pollUntil(t, c, d, func() bool { return len(d.ks) >= 80 })

	requireContiguous(t, d.ks, 1)
	if !c.Merged() || !replay.closed || stops != 1 {
		t.Errorf("merged=%v replayClosed=%v stops=%d, want merged, closed, one stop", c.Merged(), replay.closed, stops)
	}
	if c.Position() != d.ks[len(d.ks)-1]*testFrameBytes {
		t.Errorf("Position = %d", c.Position())
	}
}

func TestCatchUpAtTheLiveEdgeMergesAtOnce(t *testing.T) {
	live := liveSource(6, 20, 3)
	replay := replayOf(1, 0, 5) // nothing to replay
	var stops int
	c := newTestCatchUp(live, replay, ArchiveCatchUpConfig{StartPosition: 5 * testFrameBytes}, &stops)
	d := &delivered{}

	pollUntil(t, c, d, func() bool { return len(d.ks) >= 15 })
	requireContiguous(t, d.ks, 6)
	if !c.Merged() {
		t.Error("did not merge")
	}
}

// A replay that starts before the position already held must not repeat frames.
func TestCatchUpSkipsFramesTheConsumerAlreadyHas(t *testing.T) {
	live := liveSource(15, 40, 2)
	replay := replayOf(1, 20, 4)
	var stops int
	c := newTestCatchUp(live, replay, ArchiveCatchUpConfig{StartPosition: 10 * testFrameBytes}, &stops)
	d := &delivered{}
	pollUntil(t, c, d, func() bool { return len(d.ks) >= 25 })
	requireContiguous(t, d.ks, 11)
}

// With a buffer too small to hold the live frames produced while the replay
// runs, buffered frames are dropped and refilled; the output is still one
// contiguous stream.
func TestCatchUpSurvivesBufferOverflow(t *testing.T) {
	live := liveSource(1, 400, 4)
	replay := replayOf(1, 300, 30)
	var stops int
	c := newTestCatchUp(live, replay, ArchiveCatchUpConfig{StartPosition: 0, MaxBufferedBytes: 3 * testPayloadBytes}, &stops)
	d := &delivered{}

	pollUntil(t, c, d, func() bool { return len(d.ks) >= 350 })
	requireContiguous(t, d.ks, 1)
	if c.Restarts() == 0 {
		t.Error("expected the buffer to overflow at least once")
	}
	if !c.Merged() {
		t.Error("did not merge")
	}
}

// Live frames that skip positions while the replay is running cannot be joined
// to what came before; the buffer starts again after the gap.
func TestCatchUpDropsBufferedFramesAcrossALiveGap(t *testing.T) {
	live := &scriptedPoller{perPoll: 1000}
	for _, k := range []int64{10, 11, 12, 20, 21, 22} {
		live.add(k)
	}
	next := int64(23)
	live.onPoll = func(p *scriptedPoller) {
		if next <= 60 {
			p.add(next)
			next++
		}
	}
	replay := replayOf(1, 40, 6)
	var stops int
	c := newTestCatchUp(live, replay, ArchiveCatchUpConfig{StartPosition: 0}, &stops)
	d := &delivered{}

	pollUntil(t, c, d, func() bool { return len(d.ks) >= 55 })
	requireContiguous(t, d.ks, 1)
	if c.Restarts() == 0 {
		t.Error("the live gap should have dropped the buffer")
	}
}

func TestCatchUpReportsALiveGapAfterTheMerge(t *testing.T) {
	live := liveSource(1, 10, 1)
	replay := replayOf(1, 0, 1)
	var stops int
	c := newTestCatchUp(live, replay, ArchiveCatchUpConfig{StartPosition: 0}, &stops)
	d := &delivered{}
	pollUntil(t, c, d, func() bool { return c.Merged() && len(d.ks) >= 5 })

	// The live stream now skips frames.
	last := d.ks[len(d.ks)-1]
	live.queue = nil
	live.onPoll = nil
	live.add(last + 4)

	_, err := c.Poll(d.handler(t), 8)
	var gap *ArchiveGapError
	if !errors.As(err, &gap) {
		t.Fatalf("err = %v, want *ArchiveGapError", err)
	}
	if gap.From != last*testFrameBytes || gap.To != (last+3)*testFrameBytes {
		t.Errorf("gap = %+v, want %d to %d", gap, last*testFrameBytes, (last+3)*testFrameBytes)
	}
	if _, err := c.Poll(d.handler(t), 8); !errors.As(err, &gap) {
		t.Errorf("the error should stick, got %v", err)
	}
	if c.Position() != last*testFrameBytes {
		t.Errorf("Position moved to %d past the gap", c.Position())
	}
}

func TestCatchUpFailsWhenTheReplaySkipsFrames(t *testing.T) {
	live := &scriptedPoller{perPoll: 10}
	replay := &scriptedPoller{perPoll: 10}
	for _, k := range []int64{1, 2, 5} {
		replay.add(k)
	}
	var stops int
	c := newTestCatchUp(live, replay, ArchiveCatchUpConfig{StartPosition: 0}, &stops)
	d := &delivered{}
	var err error
	for i := 0; i < 5 && err == nil; i++ {
		_, err = c.Poll(d.handler(t), 8)
	}
	var gap *ArchiveGapError
	if !errors.As(err, &gap) || gap.From != 2*testFrameBytes || gap.To != 4*testFrameBytes {
		t.Fatalf("err = %v, want a gap from 192 to 384", err)
	}
	requireContiguous(t, d.ks, 1)
}

func TestCatchUpStallsWithoutProgress(t *testing.T) {
	c := newTestCatchUp(&scriptedPoller{}, &scriptedPoller{}, ArchiveCatchUpConfig{StartPosition: 0, StallTimeout: 20 * time.Millisecond}, new(int))
	d := &delivered{}
	deadline := time.Now().Add(2 * time.Second)
	var err error
	for time.Now().Before(deadline) && err == nil {
		_, err = c.Poll(d.handler(t), 8)
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(err, ErrArchiveCatchUpStalled) {
		t.Fatalf("err = %v, want ErrArchiveCatchUpStalled", err)
	}
}

func TestCatchUpCloseStopsTheReplayOnce(t *testing.T) {
	replay := replayOf(1, 10, 1)
	var stops int
	c := newTestCatchUp(&scriptedPoller{}, replay, ArchiveCatchUpConfig{StartPosition: 0}, &stops)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	if stops != 1 || !replay.closed {
		t.Errorf("stops=%d replayClosed=%v, want 1 and true", stops, replay.closed)
	}
	if _, err := c.Poll(func([]byte, *Header) {}, 1); err == nil {
		t.Error("Poll after Close should fail")
	}
}

func TestCatchUpStartingFromTheBeginningLearnsItsPositionFromTheFirstFrame(t *testing.T) {
	live := liveSource(8, 30, 2)
	replay := replayOf(3, 12, 4) // the recording starts at frame 3
	var stops int
	c := newTestCatchUp(live, replay, ArchiveCatchUpConfig{StartPosition: ArchiveNullPosition}, &stops)
	d := &delivered{}
	if c.Position() != -1 {
		t.Fatalf("Position before any frame = %d, want -1", c.Position())
	}
	pollUntil(t, c, d, func() bool { return len(d.ks) >= 20 })
	requireContiguous(t, d.ks, 3)
}

// ---------------------------------------------------------------------------
// The whole path through Archive.CatchUp, with real frames in loopback logs.
// ---------------------------------------------------------------------------

func TestArchiveCatchUpEndToEndOverLoopbackStreams(t *testing.T) {
	a, f := newConnectedArchive(t)

	// Two loopback streams standing for the live publication and the archive's
	// replay of it. Both carry the same 96-byte frames from position 0.
	liveLB := NewLoopbackLogBuffers(1 << 20)
	livePub := NewLoopbackPublication(liveLB, 1, 183)
	liveSub := NewLoopbackSubscription(liveLB, 183)
	replayLB := NewLoopbackLogBuffers(1 << 20)
	replayPub := NewLoopbackPublication(replayLB, 2, 4201)
	f.replaySubscription = NewLoopbackSubscription(replayLB, 4201)

	offer := func(p *Publication, k int64) {
		if r := p.Offer(testFrame(k).payload); r <= 0 {
			t.Fatalf("offer frame %d: %d", k, r)
		}
	}
	// 30 frames already recorded and published. A subscriber that joined the live
	// stream late would only see the last 20, so consume the first 10.
	for k := int64(1); k <= 30; k++ {
		offer(replayPub, k)
		offer(livePub, k)
	}
	liveSub.Poll(func([]byte, *Header) {}, 10)

	f.pub.onRequest = func(buf []byte) {
		switch templateOf(buf) {
		case TemplateIdArchiveReplayRequest:
			f.sub.push(okResponse(getInt64(buf, HeaderSize+8), 2))
		case TemplateIdArchiveStopReplayRequest:
			f.sub.push(okResponse(getInt64(buf, HeaderSize+8), 0))
		}
	}
	c, err := a.CatchUp(context.Background(), liveSub, ArchiveCatchUpConfig{
		RecordingId: 0, StartPosition: 0, ReplayChannel: "aeron:udp?endpoint=localhost:20123", ReplayStreamId: 4201,
	})
	if err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	defer c.Close()
	if f.replayChannel != "aeron:udp?endpoint=localhost:20123|session-id=2" {
		t.Errorf("replay subscription on %q", f.replayChannel)
	}

	d := &delivered{}
	next := int64(31)
	for i := 0; i < 1000 && len(d.ks) < 60; i++ {
		// New frames keep arriving on both streams while we poll.
		if next <= 60 {
			offer(replayPub, next)
			offer(livePub, next)
			next++
		}
		if _, err := c.Poll(d.handler(t), 16); err != nil {
			t.Fatalf("poll: %v", err)
		}
	}
	requireContiguous(t, d.ks, 1)
	if len(d.ks) < 60 || !c.Merged() {
		t.Fatalf("delivered %d frames, merged=%v", len(d.ks), c.Merged())
	}
}

func TestArchiveCatchUpNeedsAReplayChannel(t *testing.T) {
	a, _ := newConnectedArchive(t)
	if _, err := a.CatchUp(context.Background(), nil, ArchiveCatchUpConfig{}); err == nil {
		t.Error("a missing ReplayChannel should fail")
	}
}

// A frame of testFrameBytes that starts at start.
func frameStartingAt(start int64, termOffset int32) catchUpFrame {
	f := testFrame(start/testFrameBytes + 1)
	f.header.Position = start + testFrameBytes
	f.header.TermOffset = termOffset
	return f
}

func TestStreamGapTreatsTermEndPaddingAsContiguous(t *testing.T) {
	const term = 1 << 20
	tests := []struct {
		name string
		prev int64
		h    Header
		want int64
	}{
		{"contiguous", 960, Header{FrameLength: 64, Position: 960 + 64, TermOffset: 960}, 0},
		{"gap inside a term", 960, Header{FrameLength: 64, Position: 1344, TermOffset: 1280}, 320},
		{"padding before a term boundary", term - 64, Header{FrameLength: 64, Position: term + 64, TermOffset: 0}, 0},
		{"overlap", 960, Header{FrameLength: 64, Position: 960, TermOffset: 896}, -64},
		{"gap ending on a boundary but longer than any padding", 64, Header{FrameLength: 64, Position: term + 64, TermOffset: 0}, term - 64},
	}
	for _, tt := range tests {
		if got := StreamGap(tt.prev, &tt.h); got != tt.want {
			t.Errorf("%s: StreamGap = %d, want %d", tt.name, got, tt.want)
		}
	}
}

// Frames either side of a term boundary are not a gap, in the replay or the
// live stream.
func TestCatchUpCrossesATermBoundaryPadding(t *testing.T) {
	const term = 1 << 20
	before := frameStartingAt(term-96-64, 0) // ends 64 bytes short of the boundary
	after := frameStartingAt(term, 0)        // padding filled the 64 bytes
	after2 := frameStartingAt(term+testFrameBytes, testFrameBytes)

	replay := &scriptedPoller{perPoll: 10, queue: []catchUpFrame{before, after}}
	live := &scriptedPoller{perPoll: 10, queue: []catchUpFrame{after, after2}}
	c := newArchiveCatchUp(live, replay, nil, ArchiveCatchUpConfig{StartPosition: term - 96 - 64})

	var got []int64
	for i := 0; i < 20 && len(got) < 3; i++ {
		if _, err := c.Poll(func(_ []byte, h *Header) { got = append(got, h.Position) }, 10); err != nil {
			t.Fatalf("poll: %v", err)
		}
	}
	want := []int64{term - 64, term + testFrameBytes, term + 2*testFrameBytes}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("delivered %v, want %v", got, want)
	}
	if !c.Merged() || c.Restarts() != 0 {
		t.Fatalf("merged=%v restarts=%d", c.Merged(), c.Restarts())
	}
}

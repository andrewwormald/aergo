package aergo

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Catching up from an archive and then following the live stream.
//
// A consumer that has fallen behind, or restarted, holds a position: the end
// position of the last frame it processed. ArchiveCatchUp reads from that
// position out of the archive's recording, then hands over to the live
// subscription, so the consumer sees one contiguous stream of frames with
// nothing skipped and nothing repeated.
//
// This is not Java's ReplayMerge. That joins the replay and the live stream as
// two destinations of one multi-destination subscription, which this package
// does not support. Here the live subscription is polled the whole time and its
// frames are held in memory, up to a bound, while the replay catches up. Both
// streams carry the same stream positions (the archive records the live
// publication), so the merge is done by position: once the replay reaches the
// start of the buffered live frames, the rest of the buffer is delivered and the
// replay is stopped. A replay is much faster than live production, so the
// buffer stays small; if it does fill, it is dropped and refilled from the
// newest live frames, and the replay keeps going until it reaches them.

const (
	archiveCatchUpDefaultMaxBuffered = 64 << 20
	archiveCatchUpDefaultStall       = 15 * time.Second
)

var (
	// ErrArchiveCatchUpStalled is returned when neither the replay nor the
	// live stream has delivered anything for ArchiveCatchUpConfig.StallTimeout
	// before the catch-up merged.
	ErrArchiveCatchUpStalled = errors.New("aergo: archive catch-up made no progress")
)

// ArchiveGapError is returned when the live stream skips positions after the
// catch-up has merged: frames between From and To never arrived. The consumer
// can catch up again from Position().
type ArchiveGapError struct {
	From, To int64
}

func (e *ArchiveGapError) Error() string {
	return fmt.Sprintf("aergo: live stream skipped positions %d to %d", e.From, e.To)
}

// ArchiveCatchUpConfig configures Archive.CatchUp.
type ArchiveCatchUpConfig struct {
	// RecordingId is the recording of the stream the live subscription reads.
	RecordingId int64

	// StartPosition is the end position of the last frame already processed,
	// or ArchiveNullPosition to start from the beginning of the recording.
	StartPosition int64

	// ReplayChannel and ReplayStreamId say where the archive publishes the
	// replay. ReplayChannel must name a concrete local endpoint.
	ReplayChannel  string
	ReplayStreamId int32

	// MaxBufferedBytes bounds the live frames held while the replay catches up.
	// It defaults to 64 MiB.
	MaxBufferedBytes int

	// StallTimeout is how long the catch-up may go without delivering a frame
	// before Poll fails with ErrArchiveCatchUpStalled. It defaults to 15 seconds.
	StallTimeout time.Duration
}

// archiveFramePoller is the part of a Subscription the catch-up uses.
type archiveFramePoller interface {
	Poll(handler FragmentHandler, fragmentLimit int) int
	Close()
}

type catchUpFrame struct {
	header  Header
	payload []byte
}

// ArchiveCatchUp delivers frames from the archive, then from the live
// subscription. Create one with Archive.CatchUp. It is not safe for concurrent
// use: drive it from one goroutine, like a Subscription.
type ArchiveCatchUp struct {
	live       archiveFramePoller
	replay     archiveFramePoller
	stopReplay func() error

	maxBuffered  int
	stallTimeout time.Duration

	// position is the end position of the last frame delivered, or -1 before
	// the first frame when the start position is not known.
	position int64

	queue       []catchUpFrame
	queuedBytes int
	liveEnd     int64 // end position of the newest live frame seen, -1 if none

	merged       bool
	restarts     int
	lastProgress time.Time
	err          error
	closed       bool
}

// CatchUp reads recording cfg.RecordingId from cfg.StartPosition, then continues
// on the live subscription. live must be a subscription to the stream the
// recording was made from, and must not be polled by anything else.
func (a *Archive) CatchUp(ctx context.Context, live *Subscription, cfg ArchiveCatchUpConfig) (*ArchiveCatchUp, error) {
	if cfg.ReplayChannel == "" {
		return nil, errors.New("aergo: ArchiveCatchUpConfig.ReplayChannel is required")
	}
	sub, replayId, err := a.Replay(ctx, cfg.RecordingId, cfg.StartPosition, ArchiveReplayAllAndFollow, cfg.ReplayChannel, cfg.ReplayStreamId)
	if err != nil {
		return nil, err
	}
	stop := func() error {
		stopCtx, cancel := context.WithTimeout(context.Background(), a.cfg.MessageTimeout)
		defer cancel()
		return a.StopReplay(stopCtx, replayId)
	}
	return newArchiveCatchUp(live, sub, stop, cfg), nil
}

func newArchiveCatchUp(live, replay archiveFramePoller, stopReplay func() error, cfg ArchiveCatchUpConfig) *ArchiveCatchUp {
	if cfg.MaxBufferedBytes <= 0 {
		cfg.MaxBufferedBytes = archiveCatchUpDefaultMaxBuffered
	}
	if cfg.StallTimeout <= 0 {
		cfg.StallTimeout = archiveCatchUpDefaultStall
	}
	position := cfg.StartPosition
	if position < 0 {
		position = -1
	}
	return &ArchiveCatchUp{
		live:         live,
		replay:       replay,
		stopReplay:   stopReplay,
		maxBuffered:  cfg.MaxBufferedBytes,
		stallTimeout: cfg.StallTimeout,
		position:     position,
		liveEnd:      -1,
		lastProgress: time.Now(),
	}
}

// Position returns the end position of the last frame delivered: the position
// to save, and to catch up from again after a failure. It is -1 until a frame
// has been delivered when the catch-up started from the beginning.
func (c *ArchiveCatchUp) Position() int64 { return c.position }

// Merged reports whether the catch-up has handed over to the live stream.
func (c *ArchiveCatchUp) Merged() bool { return c.merged }

// Restarts returns how many times buffered live frames were dropped, because
// the buffer filled or the live stream skipped positions.
func (c *ArchiveCatchUp) Restarts() int { return c.restarts }

// frameStart returns where the frame ending at h.Position starts.
func frameStart(h *Header) int64 {
	return h.Position - int64((h.FrameLength+DataFrameHeaderLen-1)&^(DataFrameHeaderLen-1))
}

// Poll delivers up to fragmentLimit frames to handler and returns how many. It
// returns an error once the catch-up has failed, and keeps returning it.
func (c *ArchiveCatchUp) Poll(handler FragmentHandler, fragmentLimit int) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if c.closed {
		return 0, errors.New("aergo: archive catch-up is closed")
	}

	var n int
	if c.merged {
		n = c.deliverQueued(handler, fragmentLimit)
		if len(c.queue) == 0 && n < fragmentLimit {
			n += c.live.Poll(c.liveHandler(handler), fragmentLimit-n)
		}
		return n, c.err
	}

	// Catching up: keep reading the live stream into the buffer so it does not
	// fall behind, and deliver the replay.
	c.live.Poll(c.bufferLive, fragmentLimit)
	n = c.replay.Poll(c.replayHandler(handler), fragmentLimit)
	if c.err != nil {
		return n, c.err
	}

	if c.tryMerge() {
		n += c.deliverQueued(handler, fragmentLimit-n)
	}
	if n > 0 {
		c.lastProgress = time.Now()
	} else if time.Since(c.lastProgress) > c.stallTimeout {
		c.err = ErrArchiveCatchUpStalled
	}
	return n, c.err
}

// bufferLive holds a live frame while the replay catches up.
func (c *ArchiveCatchUp) bufferLive(buf []byte, h *Header) {
	start := frameStart(h)
	if len(c.queue) > 0 && c.liveEnd >= 0 && start != c.liveEnd {
		// The live stream skipped positions: what is buffered no longer joins
		// up with what comes next.
		c.dropQueue()
	}
	if c.queuedBytes+len(buf) > c.maxBuffered {
		c.dropQueue()
	}
	c.queue = append(c.queue, catchUpFrame{header: *h, payload: append([]byte(nil), buf...)})
	c.queuedBytes += len(buf)
	c.liveEnd = h.Position
}

func (c *ArchiveCatchUp) dropQueue() {
	c.queue = c.queue[:0]
	c.queuedBytes = 0
	c.restarts++
}

// replayHandler delivers a replayed frame, skipping any the consumer already
// has and failing if the replay skips ahead.
func (c *ArchiveCatchUp) replayHandler(handler FragmentHandler) FragmentHandler {
	return func(buf []byte, h *Header) {
		if c.err != nil {
			return
		}
		start := frameStart(h)
		if c.position >= 0 {
			if h.Position <= c.position {
				return // already delivered
			}
			if start > c.position {
				c.err = &ArchiveGapError{From: c.position, To: start}
				return
			}
		}
		handler(buf, h)
		c.position = h.Position
	}
}

// tryMerge hands over to the live stream when the replay has reached the start
// of the buffered live frames.
func (c *ArchiveCatchUp) tryMerge() bool {
	if len(c.queue) == 0 || c.position < 0 {
		return false
	}
	if c.position < frameStart(&c.queue[0].header) {
		return false // the replay has not got there yet
	}

	// Drop live frames the replay already delivered.
	drop := 0
	for drop < len(c.queue) && c.queue[drop].header.Position <= c.position {
		drop++
	}
	if drop == len(c.queue) {
		// The replay is level with the newest live frame: join at its end.
		if c.position != c.liveEnd {
			return false
		}
	} else if frameStart(&c.queue[drop].header) != c.position {
		// The buffered frames do not start where the replay stopped. Start
		// the buffer again from newer live frames.
		c.dropQueue()
		return false
	}
	c.queue = c.queue[drop:]
	c.queuedBytes = 0
	for i := range c.queue {
		c.queuedBytes += len(c.queue[i].payload)
	}

	c.merged = true
	if c.replay != nil {
		if c.stopReplay != nil {
			_ = c.stopReplay() // best effort: the replay ends by itself if the archive is gone
		}
		c.replay.Close()
		c.replay = nil
	}
	return true
}

// deliverQueued hands the buffered live frames to handler, oldest first.
func (c *ArchiveCatchUp) deliverQueued(handler FragmentHandler, fragmentLimit int) int {
	n := 0
	for len(c.queue) > 0 && n < fragmentLimit {
		f := &c.queue[0]
		handler(f.payload, &f.header)
		c.position = f.header.Position
		c.queuedBytes -= len(f.payload)
		c.queue[0] = catchUpFrame{}
		c.queue = c.queue[1:]
		n++
	}
	if len(c.queue) == 0 {
		c.queue = nil
		c.queuedBytes = 0
	}
	return n
}

// liveHandler delivers a live frame after the merge, failing if frames are
// missing.
func (c *ArchiveCatchUp) liveHandler(handler FragmentHandler) FragmentHandler {
	return func(buf []byte, h *Header) {
		if c.err != nil {
			return
		}
		if h.Position <= c.position {
			return
		}
		if start := frameStart(h); start > c.position {
			c.err = &ArchiveGapError{From: c.position, To: start}
			return
		}
		handler(buf, h)
		c.position = h.Position
	}
}

// Close stops the replay if it is still running. It does not close the live
// subscription, which the caller owns. It is safe to call more than once.
func (c *ArchiveCatchUp) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if c.replay != nil {
		if c.stopReplay != nil {
			_ = c.stopReplay()
		}
		c.replay.Close()
		c.replay = nil
	}
	return nil
}

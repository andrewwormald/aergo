package aergo

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// Replays: ask the archive to play a recording back onto a channel, and stop
// it again. A replay is published by the archive to a channel and stream you
// choose; you read it with an ordinary Subscription on that channel.

// ArchiveReplayImageSessionId returns the session id of the image a replay
// arrives on: the lower 32 bits of the replay session id. A subscription that
// should see only that replay filters on it. All 64 bits identify the replay
// to StopReplay.
func ArchiveReplayImageSessionId(replaySessionId int64) int32 {
	return int32(replaySessionId)
}

// StartReplay asks the archive to replay recordingId onto replayChannel and
// replayStreamId, and returns the replay session id.
//
// position is where to start, frame aligned, or ArchiveNullPosition for the
// start of the recording. length is how many bytes to replay, or
// ArchiveReplayAllAndFollow to replay the whole recording and follow it while
// it is live. The archive publishes to replayChannel, so it must name an
// endpoint a Subscription of yours is listening on (see Replay, which sets
// that up).
func (a *Archive) StartReplay(ctx context.Context, recordingId, position, length int64, replayChannel string, replayStreamId int32) (int64, error) {
	resp, err := a.call(ctx, func(controlSessionId, correlationId int64) []byte {
		req := &ArchiveReplayRequest{
			ControlSessionId: controlSessionId,
			CorrelationId:    correlationId,
			RecordingId:      recordingId,
			Position:         position,
			Length:           length,
			ReplayStreamId:   replayStreamId,
			FileIoMaxLength:  int32(ArchiveNullValue),
			ReplayToken:      ArchiveNullValue,
			ReplayChannel:    replayChannel,
		}
		buf := make([]byte, req.EncodedLength())
		req.Encode(buf, 0)
		return buf
	})
	if err != nil {
		return 0, err
	}
	if resp.Code != ArchiveControlResponseOK {
		return 0, archiveErrorFor(resp)
	}
	return resp.RelevantId, nil
}

// StopReplay stops one replay, identified by the id StartReplay returned.
func (a *Archive) StopReplay(ctx context.Context, replaySessionId int64) error {
	resp, err := a.call(ctx, func(controlSessionId, correlationId int64) []byte {
		req := &ArchiveStopReplayRequest{ControlSessionId: controlSessionId, CorrelationId: correlationId, ReplaySessionId: replaySessionId}
		buf := make([]byte, req.EncodedLength())
		req.Encode(buf, 0)
		return buf
	})
	if err != nil {
		return err
	}
	if resp.Code != ArchiveControlResponseOK {
		return archiveErrorFor(resp)
	}
	return nil
}

// StopAllReplays stops every replay of recordingId on this control session, or
// of all recordings when recordingId is ArchiveNullValue.
func (a *Archive) StopAllReplays(ctx context.Context, recordingId int64) error {
	resp, err := a.call(ctx, func(controlSessionId, correlationId int64) []byte {
		req := &ArchiveStopAllReplaysRequest{ControlSessionId: controlSessionId, CorrelationId: correlationId, RecordingId: recordingId}
		buf := make([]byte, req.EncodedLength())
		req.Encode(buf, 0)
		return buf
	})
	if err != nil {
		return err
	}
	if resp.Code != ArchiveControlResponseOK {
		return archiveErrorFor(resp)
	}
	return nil
}

// Replay starts a replay and returns a Subscription that reads it, with the
// replay session id for StopReplay. It does what Java's AeronArchive.replay
// does: start the replay on replayChannel, then add a subscription on the same
// channel filtered to the replay's session id.
//
// replayChannel must name a concrete local endpoint, for example
// "aeron:udp?endpoint=localhost:20123". Close the Subscription and call
// StopReplay when finished.
func (a *Archive) Replay(ctx context.Context, recordingId, position, length int64, replayChannel string, replayStreamId int32) (*Subscription, int64, error) {
	replaySessionId, err := a.StartReplay(ctx, recordingId, position, length, replayChannel, replayStreamId)
	if err != nil {
		return nil, 0, err
	}

	channel := archiveChannelWithSessionId(replayChannel, ArchiveReplayImageSessionId(replaySessionId))
	sub, err := a.ac.addReplaySubscription(channel, replayStreamId)
	if err != nil {
		// Do not leave a replay running that nothing will read.
		_ = a.StopReplay(ctx, replaySessionId)
		return nil, 0, fmt.Errorf("aergo: add replay subscription: %w", err)
	}
	return sub, replaySessionId, nil
}

// archiveChannelWithSessionId returns channel with its session-id parameter set
// to sessionId, replacing one that is already there. Channel parameters follow
// the "?" and are separated by "|".
func archiveChannelWithSessionId(channel string, sessionId int32) string {
	const key = "session-id"
	param := key + "=" + strconv.FormatInt(int64(sessionId), 10)

	base, query, hasQuery := strings.Cut(channel, "?")
	if !hasQuery || query == "" {
		return base + "?" + param
	}
	parts := strings.Split(query, "|")
	for i, p := range parts {
		if strings.HasPrefix(p, key+"=") {
			parts[i] = param
			return base + "?" + strings.Join(parts, "|")
		}
	}
	return base + "?" + query + "|" + param
}

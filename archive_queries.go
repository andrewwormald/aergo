package aergo

import (
	"context"
	"fmt"
	"time"
)

// Queries on an Archive: find a recording and read its positions. Each call
// sends one request and waits for the archive's answer, up to
// ArchiveConfig.MessageTimeout or the context's deadline, whichever is first.

// call sends the request built by build for a fresh correlation id and waits
// for the ControlResponse that answers it.
func (a *Archive) call(ctx context.Context, build func(controlSessionId, correlationId int64) []byte) (ArchiveControlResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ArchiveControlResponse{}, ErrArchiveClosed
	}

	ctx, cancel := context.WithTimeout(ctx, a.cfg.MessageTimeout)
	defer cancel()

	correlationId := a.ac.NextCorrelationId()
	if err := a.offer(ctx, build(a.controlSessionId, correlationId)); err != nil {
		return ArchiveControlResponse{}, err
	}
	resp, _, err := a.awaitMessage(ctx, correlationId, false)
	return resp, err
}

// FindLastMatchingRecording returns the id of the most recent recording at or
// after minRecordingId whose channel contains channelFragment and whose stream
// and session ids both match exactly. To match on channel and stream alone, use
// ListRecordingsForUri and take the last result. It returns ArchiveNullValue
// and no error when nothing matches.
func (a *Archive) FindLastMatchingRecording(ctx context.Context, minRecordingId int64, channelFragment string, streamId, sessionId int32) (int64, error) {
	resp, err := a.call(ctx, func(controlSessionId, correlationId int64) []byte {
		req := &ArchiveFindLastMatchingRecordingRequest{
			ControlSessionId: controlSessionId,
			CorrelationId:    correlationId,
			MinRecordingId:   minRecordingId,
			SessionId:        sessionId,
			StreamId:         streamId,
			Channel:          channelFragment,
		}
		buf := make([]byte, req.EncodedLength())
		req.Encode(buf, 0)
		return buf
	})
	if err != nil {
		return 0, err
	}
	switch resp.Code {
	case ArchiveControlResponseOK:
		return resp.RelevantId, nil
	case ArchiveControlResponseRecordingUnknown:
		return ArchiveNullValue, nil
	default:
		return 0, archiveErrorFor(resp)
	}
}

// MaxRecordedPosition returns how far the recording can be replayed. While the
// recording is active this is its live position, so it keeps growing.
func (a *Archive) MaxRecordedPosition(ctx context.Context, recordingId int64) (int64, error) {
	return a.recordingPosition(ctx, recordingId, func(controlSessionId, correlationId int64) []byte {
		req := &ArchiveMaxRecordedPositionRequest{ControlSessionId: controlSessionId, CorrelationId: correlationId, RecordingId: recordingId}
		buf := make([]byte, req.EncodedLength())
		req.Encode(buf, 0)
		return buf
	})
}

// RecordingPosition returns the position of an active recording, or
// ArchiveNullValue when the recording is not active.
func (a *Archive) RecordingPosition(ctx context.Context, recordingId int64) (int64, error) {
	return a.recordingPosition(ctx, recordingId, func(controlSessionId, correlationId int64) []byte {
		req := &ArchiveRecordingPositionRequest{ControlSessionId: controlSessionId, CorrelationId: correlationId, RecordingId: recordingId}
		buf := make([]byte, req.EncodedLength())
		req.Encode(buf, 0)
		return buf
	})
}

func (a *Archive) recordingPosition(ctx context.Context, recordingId int64, build func(controlSessionId, correlationId int64) []byte) (int64, error) {
	resp, err := a.call(ctx, build)
	if err != nil {
		return 0, err
	}
	if resp.Code != ArchiveControlResponseOK {
		return 0, archiveErrorFor(resp)
	}
	return resp.RelevantId, nil
}

// ListRecordingsForUri returns up to recordCount recordings, starting at
// fromRecordingId, whose channel contains channelFragment and whose stream id
// is streamId. The result is empty when none match.
func (a *Archive) ListRecordingsForUri(ctx context.Context, fromRecordingId int64, recordCount int32, channelFragment string, streamId int32) ([]ArchiveRecordingDescriptor, error) {
	if recordCount <= 0 {
		return nil, fmt.Errorf("aergo: ListRecordingsForUri: recordCount must be positive, got %d", recordCount)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, ErrArchiveClosed
	}

	ctx, cancel := context.WithTimeout(ctx, a.cfg.MessageTimeout)
	defer cancel()

	correlationId := a.ac.NextCorrelationId()
	req := &ArchiveListRecordingsForUriRequest{
		ControlSessionId: a.controlSessionId,
		CorrelationId:    correlationId,
		FromRecordingId:  fromRecordingId,
		RecordCount:      recordCount,
		StreamId:         streamId,
		Channel:          channelFragment,
	}
	buf := make([]byte, req.EncodedLength())
	req.Encode(buf, 0)
	if err := a.offer(ctx, buf); err != nil {
		return nil, err
	}
	return a.awaitDescriptors(ctx, correlationId, int(recordCount))
}

// awaitDescriptors collects RecordingDescriptor messages for correlationId. The
// archive ends the list either by sending want descriptors or, when fewer
// match, with a ControlResponse (RECORDING_UNKNOWN) that closes it.
func (a *Archive) awaitDescriptors(ctx context.Context, correlationId int64, want int) ([]ArchiveRecordingDescriptor, error) {
	a.pending = archivePending{collectDescriptors: true}
	handler := func(buf []byte, _ *Header) { a.onFragment(buf, correlationId, false) }

	for {
		a.ac.DoWork()
		if a.sub.Poll(handler, archiveResponseFragmentLimit) == 0 {
			select {
			case <-ctx.Done():
				return nil, ErrArchiveTimeout
			case <-time.After(50 * time.Microsecond):
			}
		}
		if a.pending.hasResponse {
			resp := a.pending.response
			if resp.Code == ArchiveControlResponseError {
				return nil, archiveErrorFor(resp)
			}
			return a.pending.descriptors, nil
		}
		if len(a.pending.descriptors) >= want {
			return a.pending.descriptors, nil
		}
		if ctx.Err() != nil {
			return nil, ErrArchiveTimeout
		}
	}
}

package aergo

import (
	"context"
	"errors"
	"testing"
	"time"
)

const testControlSessionId = 77

func encodeArchiveRecordingDescriptor(m ArchiveRecordingDescriptor) []byte {
	size := HeaderSize + archiveRecordingDescriptorBlockLength + 12 + len(m.StrippedChannel) + len(m.OriginalChannel) + len(m.SourceIdentity)
	buf := make([]byte, size)
	n := archiveHeader(buf, 0, archiveRecordingDescriptorBlockLength, TemplateIdArchiveRecordingDescriptor)
	putInt64(buf, n+0, m.ControlSessionId)
	putInt64(buf, n+8, m.CorrelationId)
	putInt64(buf, n+16, m.RecordingId)
	putInt64(buf, n+24, m.StartTimestamp)
	putInt64(buf, n+32, m.StopTimestamp)
	putInt64(buf, n+40, m.StartPosition)
	putInt64(buf, n+48, m.StopPosition)
	putInt32(buf, n+56, m.InitialTermId)
	putInt32(buf, n+60, m.SegmentFileLength)
	putInt32(buf, n+64, m.TermBufferLength)
	putInt32(buf, n+68, m.MtuLength)
	putInt32(buf, n+72, m.SessionId)
	putInt32(buf, n+76, m.StreamId)
	off := n + archiveRecordingDescriptorBlockLength
	off += putVarString(buf, off, m.StrippedChannel)
	off += putVarString(buf, off, m.OriginalChannel)
	putVarString(buf, off, m.SourceIdentity)
	return buf
}

func TestFakeRecordingDescriptorEncoderRoundTrips(t *testing.T) {
	want := ArchiveRecordingDescriptor{RecordingId: 5, StopPosition: ArchiveNullValue, StreamId: 183, StrippedChannel: "a", OriginalChannel: "bb", SourceIdentity: "ccc", TermBufferLength: 16 << 20}
	buf := encodeArchiveRecordingDescriptor(want)
	var got ArchiveRecordingDescriptor
	if n, err := got.Decode(buf, HeaderSize, archiveRecordingDescriptorBlockLength); err != nil || got != want || HeaderSize+n != len(buf) {
		t.Fatalf("round trip: %+v n=%d err=%v", got, n, err)
	}
}

// newConnectedArchive returns an Archive connected to a fake archive whose
// session id is testControlSessionId. Tests set f.pub.onRequest to script the
// archive's answers to later requests.
func newConnectedArchive(t *testing.T) (*Archive, *fakeArchiveAeron) {
	t.Helper()
	f := newFakeArchiveAeron()
	f.pub.onRequest = func(buf []byte) {
		if templateOf(buf) == TemplateIdArchiveConnectRequest {
			f.sub.push(encodeArchiveControlResponse(ArchiveControlResponse{ControlSessionId: testControlSessionId, CorrelationId: correlationOf(buf), Code: ArchiveControlResponseOK}))
		}
	}
	cfg := archiveTestConfig()
	a, err := connectArchive(context.Background(), f, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	f.pub.mu.Lock()
	f.pub.sent = nil
	f.pub.mu.Unlock()
	return a, f
}

// reply makes the fake archive answer the next request of the given template
// with the messages build returns for its correlation id.
func reply(f *fakeArchiveAeron, template uint16, build func(correlationId int64) [][]byte) {
	f.pub.onRequest = func(buf []byte) {
		if templateOf(buf) != template {
			return
		}
		for _, m := range build(getInt64(buf, HeaderSize+8)) {
			f.sub.push(m)
		}
	}
}

func okResponse(corr, relevant int64) []byte {
	return encodeArchiveControlResponse(ArchiveControlResponse{ControlSessionId: testControlSessionId, CorrelationId: corr, RelevantId: relevant, Code: ArchiveControlResponseOK})
}

func codeResponse(corr int64, code ArchiveControlResponseCode, msg string) []byte {
	return encodeArchiveControlResponse(ArchiveControlResponse{ControlSessionId: testControlSessionId, CorrelationId: corr, Code: code, ErrorMessage: msg})
}

func TestFindLastMatchingRecording(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveFindLastMatchingRecordingRequest, func(corr int64) [][]byte { return [][]byte{okResponse(corr, 42)} })

	id, err := a.FindLastMatchingRecording(context.Background(), 5, "alias=egress", 183, 0)
	if err != nil || id != 42 {
		t.Fatalf("id=%d err=%v, want 42", id, err)
	}

	req := f.pub.requests()[0]
	if got := getInt64(req, HeaderSize+0); got != testControlSessionId {
		t.Errorf("ControlSessionId = %d", got)
	}
	if got := getInt64(req, HeaderSize+16); got != 5 {
		t.Errorf("MinRecordingId = %d", got)
	}
	if got := getInt32(req, HeaderSize+28); got != 183 {
		t.Errorf("StreamId = %d", got)
	}
	if ch, _, _ := archiveVarString(req, HeaderSize+archiveFindLastMatchingRecordingRequestBlockLength); ch != "alias=egress" {
		t.Errorf("Channel = %q", ch)
	}
}

func TestFindLastMatchingRecordingNoMatchIsNullNotError(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveFindLastMatchingRecordingRequest, func(corr int64) [][]byte {
		return [][]byte{codeResponse(corr, ArchiveControlResponseRecordingUnknown, "")}
	})
	id, err := a.FindLastMatchingRecording(context.Background(), 0, "x", 1, 0)
	if err != nil || id != ArchiveNullValue {
		t.Fatalf("id=%d err=%v, want null and no error", id, err)
	}
}

func TestMaxRecordedPosition(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveMaxRecordedPositionRequest, func(corr int64) [][]byte { return [][]byte{okResponse(corr, 1<<40)} })
	pos, err := a.MaxRecordedPosition(context.Background(), 3)
	if err != nil || pos != 1<<40 {
		t.Fatalf("pos=%d err=%v", pos, err)
	}
	if got := getInt64(f.pub.requests()[0], HeaderSize+16); got != 3 {
		t.Errorf("RecordingId = %d, want 3", got)
	}
}

func TestMaxRecordedPositionUnknownRecordingIsAnError(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveMaxRecordedPositionRequest, func(corr int64) [][]byte {
		return [][]byte{codeResponse(corr, ArchiveControlResponseRecordingUnknown, "unknown recording 9")}
	})
	_, err := a.MaxRecordedPosition(context.Background(), 9)
	var ae *ArchiveError
	if !errors.As(err, &ae) || ae.Code != ArchiveControlResponseRecordingUnknown {
		t.Fatalf("err = %v, want RECORDING_UNKNOWN ArchiveError", err)
	}
}

func TestRecordingPositionNullWhenNotActive(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveRecordingPositionRequest, func(corr int64) [][]byte { return [][]byte{okResponse(corr, ArchiveNullValue)} })
	pos, err := a.RecordingPosition(context.Background(), 3)
	if err != nil || pos != ArchiveNullValue {
		t.Fatalf("pos=%d err=%v, want null", pos, err)
	}
}

func descriptor(corr, id int64) []byte {
	return encodeArchiveRecordingDescriptor(ArchiveRecordingDescriptor{
		ControlSessionId: testControlSessionId, CorrelationId: corr, RecordingId: id, StopPosition: ArchiveNullValue, StreamId: 183,
		StrippedChannel: "aeron:udp", OriginalChannel: "aeron:udp?alias=egress", SourceIdentity: "127.0.0.1:1",
	})
}

func TestListRecordingsEndedByTheArchive(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveListRecordingsForUriRequest, func(corr int64) [][]byte {
		return [][]byte{descriptor(corr, 1), descriptor(corr, 2), codeResponse(corr, ArchiveControlResponseRecordingUnknown, "")}
	})
	got, err := a.ListRecordingsForUri(context.Background(), 0, 10, "alias=egress", 183)
	if err != nil || len(got) != 2 || got[0].RecordingId != 1 || got[1].RecordingId != 2 {
		t.Fatalf("got %+v err=%v", got, err)
	}

	req := f.pub.requests()[0]
	if got := getInt32(req, HeaderSize+24); got != 10 {
		t.Errorf("RecordCount = %d", got)
	}
	if got := getInt32(req, HeaderSize+28); got != 183 {
		t.Errorf("StreamId = %d", got)
	}
}

func TestListRecordingsCompleteWhenCountReached(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveListRecordingsForUriRequest, func(corr int64) [][]byte {
		return [][]byte{descriptor(corr, 1), descriptor(corr, 2)} // no terminator
	})
	got, err := a.ListRecordingsForUri(context.Background(), 0, 2, "x", 1)
	if err != nil || len(got) != 2 {
		t.Fatalf("got %d err=%v", len(got), err)
	}
}

func TestListRecordingsNoneMatch(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveListRecordingsForUriRequest, func(corr int64) [][]byte {
		return [][]byte{codeResponse(corr, ArchiveControlResponseRecordingUnknown, "")}
	})
	got, err := a.ListRecordingsForUri(context.Background(), 0, 5, "x", 1)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %d err=%v, want empty", len(got), err)
	}
}

func TestListRecordingsError(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveListRecordingsForUriRequest, func(corr int64) [][]byte {
		return [][]byte{codeResponse(corr, ArchiveControlResponseError, "bad request")}
	})
	_, err := a.ListRecordingsForUri(context.Background(), 0, 5, "x", 1)
	var ae *ArchiveError
	if !errors.As(err, &ae) || ae.Message != "bad request" {
		t.Fatalf("err = %v", err)
	}
}

func TestListRecordingsRejectsNonPositiveCount(t *testing.T) {
	a, _ := newConnectedArchive(t)
	if _, err := a.ListRecordingsForUri(context.Background(), 0, 0, "x", 1); err == nil {
		t.Error("recordCount 0 should fail")
	}
}

// Responses addressed to a different control session on the same response
// stream are not ours.
func TestQueriesIgnoreOtherControlSessions(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveMaxRecordedPositionRequest, func(corr int64) [][]byte {
		other := encodeArchiveControlResponse(ArchiveControlResponse{ControlSessionId: 999, CorrelationId: corr, RelevantId: 1, Code: ArchiveControlResponseOK})
		return [][]byte{other, okResponse(corr, 2)}
	})
	pos, err := a.MaxRecordedPosition(context.Background(), 3)
	if err != nil || pos != 2 {
		t.Fatalf("pos=%d err=%v, want 2 (our session's answer)", pos, err)
	}
}

func TestQueriesTimeOut(t *testing.T) {
	a, f := newConnectedArchive(t)
	f.pub.onRequest = nil
	a.cfg.MessageTimeout = 30 * time.Millisecond
	if _, err := a.MaxRecordedPosition(context.Background(), 1); !errors.Is(err, ErrArchiveTimeout) {
		t.Errorf("MaxRecordedPosition err = %v", err)
	}
	if _, err := a.ListRecordingsForUri(context.Background(), 0, 1, "x", 1); !errors.Is(err, ErrArchiveTimeout) {
		t.Errorf("ListRecordingsForUri err = %v", err)
	}
}

func TestQueriesAfterCloseFail(t *testing.T) {
	a, _ := newConnectedArchive(t)
	_ = a.Close()
	ctx := context.Background()
	if _, err := a.MaxRecordedPosition(ctx, 1); !errors.Is(err, ErrArchiveClosed) {
		t.Errorf("MaxRecordedPosition err = %v", err)
	}
	if _, err := a.RecordingPosition(ctx, 1); !errors.Is(err, ErrArchiveClosed) {
		t.Errorf("RecordingPosition err = %v", err)
	}
	if _, err := a.FindLastMatchingRecording(ctx, 0, "x", 1, 0); !errors.Is(err, ErrArchiveClosed) {
		t.Errorf("FindLastMatchingRecording err = %v", err)
	}
	if _, err := a.ListRecordingsForUri(ctx, 0, 1, "x", 1); !errors.Is(err, ErrArchiveClosed) {
		t.Errorf("ListRecordingsForUri err = %v", err)
	}
}

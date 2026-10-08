package aergo

import (
	"context"
	"errors"
	"testing"
)

func TestStartReplaySendsTheRequestAndReturnsTheSessionId(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveReplayRequest, func(corr int64) [][]byte { return [][]byte{okResponse(corr, 0x1234_0000_0042)} })

	id, err := a.StartReplay(context.Background(), 3, 4096, ArchiveReplayAllAndFollow, "aeron:udp?endpoint=localhost:20123", 183)
	if err != nil || id != 0x1234_0000_0042 {
		t.Fatalf("id=%#x err=%v", id, err)
	}

	req := f.pub.requests()[0]
	base := HeaderSize
	checks := []struct {
		name      string
		got, want int64
	}{
		{"ControlSessionId", getInt64(req, base+0), testControlSessionId},
		{"RecordingId", getInt64(req, base+16), 3},
		{"Position", getInt64(req, base+24), 4096},
		{"Length", getInt64(req, base+32), ArchiveReplayAllAndFollow},
		{"ReplayStreamId", int64(getInt32(req, base+40)), 183},
		{"FileIoMaxLength", int64(getInt32(req, base+44)), -1},
		{"ReplayToken", getInt64(req, base+48), -1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
	if ch, _, _ := archiveVarString(req, base+archiveReplayRequestBlockLength); ch != "aeron:udp?endpoint=localhost:20123" {
		t.Errorf("ReplayChannel = %q", ch)
	}
}

func TestStartReplayErrors(t *testing.T) {
	for _, code := range []ArchiveControlResponseCode{ArchiveControlResponseError, ArchiveControlResponseRecordingUnknown} {
		a, f := newConnectedArchive(t)
		reply(f, TemplateIdArchiveReplayRequest, func(corr int64) [][]byte { return [][]byte{codeResponse(corr, code, "nope")} })
		_, err := a.StartReplay(context.Background(), 3, 0, ArchiveReplayAllAndFollow, "aeron:udp?endpoint=localhost:1", 1)
		var ae *ArchiveError
		if !errors.As(err, &ae) || ae.Code != code {
			t.Errorf("code %s: err = %v", code, err)
		}
	}
}

func TestStopReplay(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveStopReplayRequest, func(corr int64) [][]byte { return [][]byte{okResponse(corr, 0)} })
	if err := a.StopReplay(context.Background(), 0x7777_0000_0001); err != nil {
		t.Fatal(err)
	}
	if got := getInt64(f.pub.requests()[0], HeaderSize+16); got != 0x7777_0000_0001 {
		t.Errorf("ReplaySessionId = %#x, want all 64 bits", got)
	}

	reply(f, TemplateIdArchiveStopReplayRequest, func(corr int64) [][]byte {
		return [][]byte{codeResponse(corr, ArchiveControlResponseError, "unknown replay")}
	})
	var ae *ArchiveError
	if err := a.StopReplay(context.Background(), 9); !errors.As(err, &ae) {
		t.Errorf("err = %v, want *ArchiveError", err)
	}
}

func TestStopAllReplays(t *testing.T) {
	a, f := newConnectedArchive(t)
	reply(f, TemplateIdArchiveStopAllReplaysRequest, func(corr int64) [][]byte { return [][]byte{okResponse(corr, 0)} })
	if err := a.StopAllReplays(context.Background(), ArchiveNullValue); err != nil {
		t.Fatal(err)
	}
	if got := getInt64(f.pub.requests()[0], HeaderSize+16); got != ArchiveNullValue {
		t.Errorf("RecordingId = %d, want ArchiveNullValue", got)
	}
}

// Replay subscribes on the replay channel filtered to the lower 32 bits of the
// replay session id, as the Java client does.
func TestReplaySubscribesOnTheReplaysSessionId(t *testing.T) {
	a, f := newConnectedArchive(t)
	const replayId = int64(0x0000_0007_0000_002A) // low 32 bits: 42
	reply(f, TemplateIdArchiveReplayRequest, func(corr int64) [][]byte { return [][]byte{okResponse(corr, replayId)} })

	sub, id, err := a.Replay(context.Background(), 0, ArchiveNullPosition, ArchiveReplayAllAndFollow, "aeron:udp?endpoint=localhost:20123", 183)
	if err != nil || sub == nil || id != replayId {
		t.Fatalf("sub=%v id=%#x err=%v", sub, id, err)
	}
	if f.replayChannel != "aeron:udp?endpoint=localhost:20123|session-id=42" || f.replayStream != 183 {
		t.Errorf("subscribed on %q stream %d", f.replayChannel, f.replayStream)
	}
}

func TestReplayStopsTheReplayWhenTheSubscriptionFails(t *testing.T) {
	a, f := newConnectedArchive(t)
	f.replayErr = errors.New("driver said no")
	f.pub.onRequest = func(buf []byte) {
		switch templateOf(buf) {
		case TemplateIdArchiveReplayRequest:
			f.sub.push(okResponse(getInt64(buf, HeaderSize+8), 5))
		case TemplateIdArchiveStopReplayRequest:
			f.sub.push(okResponse(getInt64(buf, HeaderSize+8), 0))
		}
	}
	if _, _, err := a.Replay(context.Background(), 0, 0, ArchiveReplayAllAndFollow, "aeron:udp?endpoint=localhost:1", 1); err == nil {
		t.Fatal("expected an error")
	}
	var stops int
	for _, r := range f.pub.requests() {
		if templateOf(r) == TemplateIdArchiveStopReplayRequest {
			stops++
			if got := getInt64(r, HeaderSize+16); got != 5 {
				t.Errorf("stopped replay %d, want 5", got)
			}
		}
	}
	if stops != 1 {
		t.Errorf("sent %d StopReplay requests, want 1", stops)
	}
}

func TestArchiveReplayImageSessionId(t *testing.T) {
	for id, want := range map[int64]int32{
		42:                    42,
		0x0000_0007_0000_002A: 42,
		-1:                    -1,
		0x1_0000_0000:         0,
	} {
		if got := ArchiveReplayImageSessionId(id); got != want {
			t.Errorf("ArchiveReplayImageSessionId(%#x) = %d, want %d", id, got, want)
		}
	}
}

func TestArchiveChannelWithSessionId(t *testing.T) {
	for _, tc := range []struct {
		in   string
		id   int32
		want string
	}{
		{"aeron:udp?endpoint=localhost:20123", 42, "aeron:udp?endpoint=localhost:20123|session-id=42"},
		{"aeron:udp?endpoint=localhost:20123|session-id=7", 42, "aeron:udp?endpoint=localhost:20123|session-id=42"},
		{"aeron:udp?session-id=7|endpoint=h:1", -5, "aeron:udp?session-id=-5|endpoint=h:1"},
		{"aeron:ipc", 9, "aeron:ipc?session-id=9"},
		{"aeron:udp?", 9, "aeron:udp?session-id=9"},
	} {
		if got := archiveChannelWithSessionId(tc.in, tc.id); got != tc.want {
			t.Errorf("archiveChannelWithSessionId(%q, %d) = %q, want %q", tc.in, tc.id, got, tc.want)
		}
	}
}

func TestReplayAfterCloseFails(t *testing.T) {
	a, _ := newConnectedArchive(t)
	_ = a.Close()
	ctx := context.Background()
	if _, err := a.StartReplay(ctx, 0, 0, ArchiveReplayAllAndFollow, "aeron:udp?endpoint=h:1", 1); !errors.Is(err, ErrArchiveClosed) {
		t.Errorf("StartReplay err = %v", err)
	}
	if err := a.StopReplay(ctx, 1); !errors.Is(err, ErrArchiveClosed) {
		t.Errorf("StopReplay err = %v", err)
	}
	if err := a.StopAllReplays(ctx, 1); !errors.Is(err, ErrArchiveClosed) {
		t.Errorf("StopAllReplays err = %v", err)
	}
}

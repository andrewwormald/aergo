package aergo

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Test doubles: a fake Aeron client and a fake archive that answers the
// control requests it receives.
// ---------------------------------------------------------------------------

// encodeArchiveControlResponse builds a ControlResponse for the fake archive.
func encodeArchiveControlResponse(m ArchiveControlResponse) []byte {
	buf := make([]byte, HeaderSize+archiveControlResponseBlockLength+4+len(m.ErrorMessage))
	n := archiveHeader(buf, 0, archiveControlResponseBlockLength, TemplateIdArchiveControlResponse)
	putInt64(buf, n+0, m.ControlSessionId)
	putInt64(buf, n+8, m.CorrelationId)
	putInt64(buf, n+16, m.RelevantId)
	putInt32(buf, n+24, int32(m.Code))
	putInt32(buf, n+28, m.Version)
	putVarString(buf, n+archiveControlResponseBlockLength, m.ErrorMessage)
	return buf
}

func encodeArchiveChallenge(m ArchiveChallenge) []byte {
	buf := make([]byte, HeaderSize+archiveChallengeBlockLength+4+len(m.EncodedChallenge))
	n := archiveHeader(buf, 0, archiveChallengeBlockLength, TemplateIdArchiveChallenge)
	putInt64(buf, n+0, m.ControlSessionId)
	putInt64(buf, n+8, m.CorrelationId)
	putInt32(buf, n+16, m.Version)
	putVarBytes(buf, n+archiveChallengeBlockLength, m.EncodedChallenge)
	return buf
}

// The fake codecs must agree with the decoders the client uses.
func TestFakeArchiveEncodersRoundTrip(t *testing.T) {
	want := ArchiveControlResponse{ControlSessionId: 1, CorrelationId: 2, RelevantId: 3, Code: ArchiveControlResponseError, Version: 4, ErrorMessage: "x"}
	buf := encodeArchiveControlResponse(want)
	var got ArchiveControlResponse
	if _, err := got.Decode(buf, HeaderSize, archiveControlResponseBlockLength); err != nil || got != want {
		t.Fatalf("control response: %+v %v", got, err)
	}
	ch := ArchiveChallenge{ControlSessionId: 1, CorrelationId: 2, Version: 3, EncodedChallenge: []byte{4, 5}}
	var gch ArchiveChallenge
	if _, err := gch.Decode(encodeArchiveChallenge(ch), HeaderSize, archiveChallengeBlockLength); err != nil || gch.CorrelationId != 2 || !bytes.Equal(gch.EncodedChallenge, ch.EncodedChallenge) {
		t.Fatalf("challenge: %+v %v", gch, err)
	}
}

type fakeArchivePub struct {
	mu        sync.Mutex
	connected bool
	sent      [][]byte
	closed    bool
	onRequest func(buf []byte) // lets the fake archive react
}

func (p *fakeArchivePub) IsConnected() bool { return p.connected }

func (p *fakeArchivePub) OfferWithBackoff(_ context.Context, buf []byte, _ time.Duration) int64 {
	p.mu.Lock()
	cp := append([]byte(nil), buf...)
	p.sent = append(p.sent, cp)
	hook := p.onRequest
	p.mu.Unlock()
	if hook != nil {
		hook(cp)
	}
	return int64(len(buf))
}

func (p *fakeArchivePub) Close() { p.closed = true }

func (p *fakeArchivePub) requests() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]byte(nil), p.sent...)
}

type fakeArchiveSub struct {
	mu     sync.Mutex
	queue  [][]byte
	closed bool
}

func (s *fakeArchiveSub) push(buf []byte) {
	s.mu.Lock()
	s.queue = append(s.queue, buf)
	s.mu.Unlock()
}

func (s *fakeArchiveSub) Poll(handler FragmentHandler, limit int) int {
	s.mu.Lock()
	var batch [][]byte
	for len(s.queue) > 0 && len(batch) < limit {
		batch = append(batch, s.queue[0])
		s.queue = s.queue[1:]
	}
	s.mu.Unlock()
	for _, b := range batch {
		handler(b, &Header{})
	}
	return len(batch)
}

func (s *fakeArchiveSub) Close() { s.closed = true }

type fakeArchiveAeron struct {
	pub    *fakeArchivePub
	sub    *fakeArchiveSub
	nextId int64

	requestChannel, responseChannel string
	requestStream, responseStream   int32

	// Set by addReplaySubscription.
	replayChannel string
	replayStream  int32
	replayErr     error
}

func newFakeArchiveAeron() *fakeArchiveAeron {
	return &fakeArchiveAeron{pub: &fakeArchivePub{connected: true}, sub: &fakeArchiveSub{}, nextId: 1000}
}

func (f *fakeArchiveAeron) addRequestPublication(channel string, streamID int32) (archivePublication, error) {
	f.requestChannel, f.requestStream = channel, streamID
	return f.pub, nil
}

func (f *fakeArchiveAeron) addResponseSubscription(channel string, streamID int32) (archiveSubscription, error) {
	f.responseChannel, f.responseStream = channel, streamID
	return f.sub, nil
}

func (f *fakeArchiveAeron) addReplaySubscription(channel string, streamID int32) (*Subscription, error) {
	f.replayChannel, f.replayStream = channel, streamID
	if f.replayErr != nil {
		return nil, f.replayErr
	}
	return NewLoopbackSubscription(NewLoopbackLogBuffers(64*1024), streamID), nil
}

func (f *fakeArchiveAeron) NextCorrelationId() int64 { f.nextId++; return f.nextId }
func (f *fakeArchiveAeron) DoWork() int              { return 0 }

func archiveTestConfig() ArchiveConfig {
	return ArchiveConfig{
		ControlRequestChannel:  "aeron:udp?endpoint=archive:10001",
		ControlResponseChannel: "aeron:udp?endpoint=client:20001",
		MessageTimeout:         500 * time.Millisecond,
	}
}

// templateOf returns the template id of an encoded request.
func templateOf(buf []byte) uint16 {
	var h MessageHeader
	h.Decode(buf, 0)
	return h.TemplateId
}

func correlationOf(buf []byte) int64 { return getInt64(buf, HeaderSize+0) }

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestConnectArchiveSucceeds(t *testing.T) {
	f := newFakeArchiveAeron()
	f.pub.onRequest = func(buf []byte) {
		if templateOf(buf) == TemplateIdArchiveConnectRequest {
			f.sub.push(encodeArchiveControlResponse(ArchiveControlResponse{
				ControlSessionId: 77, CorrelationId: getInt64(buf, HeaderSize+0), Code: ArchiveControlResponseOK,
			}))
		}
	}

	a, err := connectArchive(context.Background(), f, archiveTestConfig())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if got := a.ControlSessionId(); got != 77 {
		t.Errorf("ControlSessionId = %d, want 77", got)
	}

	// The channels and stream ids default to the archive's defaults.
	if f.requestChannel != "aeron:udp?endpoint=archive:10001" || f.requestStream != ArchiveControlStreamIdDefault {
		t.Errorf("request channel/stream = %q/%d", f.requestChannel, f.requestStream)
	}
	if f.responseChannel != "aeron:udp?endpoint=client:20001" || f.responseStream != ArchiveControlResponseStreamIdDefault {
		t.Errorf("response channel/stream = %q/%d", f.responseChannel, f.responseStream)
	}

	// It sent a ConnectRequest telling the archive where to reply.
	reqs := f.pub.requests()
	if len(reqs) != 1 || templateOf(reqs[0]) != TemplateIdArchiveConnectRequest {
		t.Fatalf("requests = %d, first template %d", len(reqs), templateOf(reqs[0]))
	}
	if got := getInt32(reqs[0], HeaderSize+8); got != ArchiveControlResponseStreamIdDefault {
		t.Errorf("ResponseStreamId = %d", got)
	}
	if got := getInt32(reqs[0], HeaderSize+12); got != ArchiveProtocolSemanticVersion {
		t.Errorf("Version = %#x", got)
	}
	ch, _, _ := archiveVarString(reqs[0], HeaderSize+archiveConnectRequestBlockLength)
	if ch != "aeron:udp?endpoint=client:20001" {
		t.Errorf("ResponseChannel = %q", ch)
	}
}

func TestConnectArchiveWithCredentialsUsesAuthConnect(t *testing.T) {
	f := newFakeArchiveAeron()
	f.pub.onRequest = func(buf []byte) {
		if templateOf(buf) == TemplateIdArchiveAuthConnectRequest {
			f.sub.push(encodeArchiveControlResponse(ArchiveControlResponse{ControlSessionId: 5, CorrelationId: correlationOf(buf), Code: ArchiveControlResponseOK}))
		}
	}
	cfg := archiveTestConfig()
	cfg.Credentials = []byte("secret")
	if _, err := connectArchive(context.Background(), f, cfg); err != nil {
		t.Fatalf("connect: %v", err)
	}
	reqs := f.pub.requests()
	if len(reqs) != 1 || templateOf(reqs[0]) != TemplateIdArchiveAuthConnectRequest {
		t.Fatalf("want one AuthConnectRequest, got %d requests", len(reqs))
	}
	off := HeaderSize + archiveConnectRequestBlockLength
	_, n, _ := archiveVarString(reqs[0], off)
	creds, _, _ := archiveVarBytes(reqs[0], off+n)
	if string(creds) != "secret" {
		t.Errorf("credentials = %q", creds)
	}
}

func TestConnectArchiveAnswersChallenge(t *testing.T) {
	f := newFakeArchiveAeron()
	var challengeCorr int64
	f.pub.onRequest = func(buf []byte) {
		switch templateOf(buf) {
		case TemplateIdArchiveConnectRequest:
			challengeCorr = correlationOf(buf)
			f.sub.push(encodeArchiveChallenge(ArchiveChallenge{ControlSessionId: 42, CorrelationId: challengeCorr, EncodedChallenge: []byte("nonce")}))
		case TemplateIdArchiveChallengeResponse:
			f.sub.push(encodeArchiveControlResponse(ArchiveControlResponse{ControlSessionId: 42, CorrelationId: getInt64(buf, HeaderSize+8), Code: ArchiveControlResponseOK}))
		}
	}
	var seen []byte
	cfg := archiveTestConfig()
	cfg.ChallengeResponder = func(c []byte) ([]byte, error) { seen = append([]byte(nil), c...); return []byte("answer"), nil }

	a, err := connectArchive(context.Background(), f, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if string(seen) != "nonce" {
		t.Errorf("responder saw %q", seen)
	}
	if a.ControlSessionId() != 42 {
		t.Errorf("ControlSessionId = %d", a.ControlSessionId())
	}
	reqs := f.pub.requests()
	if len(reqs) != 2 || templateOf(reqs[1]) != TemplateIdArchiveChallengeResponse {
		t.Fatalf("want connect then challenge response, got %d requests", len(reqs))
	}
	if got := getInt64(reqs[1], HeaderSize+0); got != 42 {
		t.Errorf("ChallengeResponse.ControlSessionId = %d, want 42", got)
	}
	if got := getInt64(reqs[1], HeaderSize+8); got == challengeCorr {
		t.Errorf("ChallengeResponse reused the connect correlation id %d", got)
	}
	creds, _, _ := archiveVarBytes(reqs[1], HeaderSize+archiveChallengeResponseBlockLength)
	if string(creds) != "answer" {
		t.Errorf("credentials = %q", creds)
	}
}

func TestConnectArchiveChallengeWithoutResponderFails(t *testing.T) {
	f := newFakeArchiveAeron()
	f.pub.onRequest = func(buf []byte) {
		f.sub.push(encodeArchiveChallenge(ArchiveChallenge{ControlSessionId: 1, CorrelationId: correlationOf(buf), EncodedChallenge: []byte("x")}))
	}
	if _, err := connectArchive(context.Background(), f, archiveTestConfig()); !errors.Is(err, ErrArchiveChallenge) {
		t.Fatalf("err = %v, want ErrArchiveChallenge", err)
	}
	if !f.pub.closed || !f.sub.closed {
		t.Errorf("channels left open after a failed connect")
	}
}

func TestConnectArchiveReturnsArchiveError(t *testing.T) {
	f := newFakeArchiveAeron()
	f.pub.onRequest = func(buf []byte) {
		f.sub.push(encodeArchiveControlResponse(ArchiveControlResponse{
			CorrelationId: correlationOf(buf), Code: ArchiveControlResponseError, RelevantId: 7, ErrorMessage: "authentication rejected",
		}))
	}
	_, err := connectArchive(context.Background(), f, archiveTestConfig())
	var ae *ArchiveError
	if !errors.As(err, &ae) {
		t.Fatalf("err = %v, want *ArchiveError", err)
	}
	if ae.Code != ArchiveControlResponseError || ae.ErrorCode != 7 || ae.Message != "authentication rejected" {
		t.Errorf("ArchiveError = %+v", ae)
	}
	if !f.pub.closed || !f.sub.closed {
		t.Errorf("channels left open after a failed connect")
	}
}

func TestConnectArchiveTimesOutWithoutResponse(t *testing.T) {
	f := newFakeArchiveAeron()
	cfg := archiveTestConfig()
	cfg.MessageTimeout = 50 * time.Millisecond
	start := time.Now()
	_, err := connectArchive(context.Background(), f, cfg)
	if !errors.Is(err, ErrArchiveTimeout) {
		t.Fatalf("err = %v, want ErrArchiveTimeout", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("took %s, want about the 50ms timeout", time.Since(start))
	}
	if !f.pub.closed || !f.sub.closed {
		t.Errorf("channels left open after a failed connect")
	}
}

func TestConnectArchiveFailsWhenPublicationNeverConnects(t *testing.T) {
	f := newFakeArchiveAeron()
	f.pub.connected = false
	cfg := archiveTestConfig()
	cfg.MessageTimeout = 50 * time.Millisecond
	if _, err := connectArchive(context.Background(), f, cfg); !errors.Is(err, ErrArchiveNotConnected) {
		t.Fatalf("err = %v, want ErrArchiveNotConnected", err)
	}
	if len(f.pub.requests()) != 0 {
		t.Errorf("sent a request on a publication that was never connected")
	}
}

// A response for another correlation id (a late answer to an earlier request)
// must not be taken for ours.
func TestConnectArchiveIgnoresOtherCorrelationIds(t *testing.T) {
	f := newFakeArchiveAeron()
	f.pub.onRequest = func(buf []byte) {
		corr := correlationOf(buf)
		f.sub.push(encodeArchiveControlResponse(ArchiveControlResponse{ControlSessionId: 999, CorrelationId: corr + 1, Code: ArchiveControlResponseOK}))
		f.sub.push(encodeArchiveControlResponse(ArchiveControlResponse{ControlSessionId: 123, CorrelationId: corr, Code: ArchiveControlResponseOK}))
	}
	a, err := connectArchive(context.Background(), f, archiveTestConfig())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if a.ControlSessionId() != 123 {
		t.Errorf("ControlSessionId = %d, want 123 (the matching response)", a.ControlSessionId())
	}
}

func TestArchiveCloseSendsCloseSessionAndIsIdempotent(t *testing.T) {
	f := newFakeArchiveAeron()
	f.pub.onRequest = func(buf []byte) {
		if templateOf(buf) == TemplateIdArchiveConnectRequest {
			f.sub.push(encodeArchiveControlResponse(ArchiveControlResponse{ControlSessionId: 77, CorrelationId: correlationOf(buf), Code: ArchiveControlResponseOK}))
		}
	}
	a, err := connectArchive(context.Background(), f, archiveTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	reqs := f.pub.requests()
	last := reqs[len(reqs)-1]
	if templateOf(last) != TemplateIdArchiveCloseSessionRequest || getInt64(last, HeaderSize) != 77 {
		t.Errorf("last request template %d session %d, want CloseSession for 77", templateOf(last), getInt64(last, HeaderSize))
	}
	closeRequests := 0
	for _, r := range reqs {
		if templateOf(r) == TemplateIdArchiveCloseSessionRequest {
			closeRequests++
		}
	}
	if closeRequests != 1 {
		t.Errorf("sent %d CloseSession requests, want 1", closeRequests)
	}
	if !f.pub.closed || !f.sub.closed {
		t.Errorf("channels left open after Close")
	}
}

func TestArchiveConfigValidation(t *testing.T) {
	if _, err := (ArchiveConfig{ControlResponseChannel: "x"}).withDefaults(); err == nil {
		t.Error("missing ControlRequestChannel should fail")
	}
	if _, err := (ArchiveConfig{ControlRequestChannel: "x"}).withDefaults(); err == nil {
		t.Error("missing ControlResponseChannel should fail")
	}
	c, err := (ArchiveConfig{ControlRequestChannel: "a", ControlResponseChannel: "b"}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if c.ControlRequestStreamId != 10 || c.ControlResponseStreamId != 20 || c.MessageTimeout != 10*time.Second {
		t.Errorf("defaults = %+v", c)
	}
}

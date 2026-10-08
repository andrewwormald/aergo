package aergo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Archive is a control session with an Aeron Archive. Requests go out on the
// control request publication and responses come back on a subscription this
// client owns. Calls are serialised: an Archive is safe for use from several
// goroutines, but they take turns.
//
// Archive is poll-driven like the rest of the package: while a call waits for
// its response it drives the Aeron client's conductor itself, so the caller
// does not need to call DoWork for it.

// Default archive control channel settings (see the aeron.archive.control.*
// properties).
const (
	archiveDefaultMessageTimeout = 10 * time.Second

	// archiveResponseFragmentLimit bounds the fragments read per poll while
	// waiting for a response.
	archiveResponseFragmentLimit = 10
)

var (
	// ErrArchiveTimeout is returned when the archive does not answer in time.
	ErrArchiveTimeout = errors.New("aergo: archive request timed out")

	// ErrArchiveNotConnected is returned when the control request
	// publication never connects to the archive.
	ErrArchiveNotConnected = errors.New("aergo: archive control request publication is not connected")

	// ErrArchiveClosed is returned for a call on a closed Archive.
	ErrArchiveClosed = errors.New("aergo: archive client is closed")

	// ErrArchiveChallenge is returned when the archive challenges the client
	// and ArchiveConfig.ChallengeResponder is not set.
	ErrArchiveChallenge = errors.New("aergo: archive sent an authentication challenge and no ChallengeResponder is configured")
)

// ArchiveError is an error response from the archive.
type ArchiveError struct {
	CorrelationId int64
	Code          ArchiveControlResponseCode
	// ErrorCode is the archive's own error code, carried in the response's
	// RelevantId for Code ERROR.
	ErrorCode int64
	Message   string
}

func (e *ArchiveError) Error() string {
	return fmt.Sprintf("aergo: archive responded %s (error code %d): %s", e.Code, e.ErrorCode, e.Message)
}

// ArchiveConfig configures ConnectArchive.
type ArchiveConfig struct {
	// ControlRequestChannel is where the archive listens for control
	// requests, for example "aeron:udp?endpoint=archive-host:10001".
	ControlRequestChannel string

	// ControlRequestStreamId defaults to ArchiveControlStreamIdDefault (10).
	ControlRequestStreamId int32

	// ControlResponseChannel is this client's own endpoint, which the archive
	// connects back to, for example "aeron:udp?endpoint=client-host:20001".
	// It must name a concrete port: the archive is told this address.
	ControlResponseChannel string

	// ControlResponseStreamId defaults to ArchiveControlResponseStreamIdDefault (20).
	ControlResponseStreamId int32

	// MessageTimeout bounds each request and the connect handshake. It
	// defaults to 10 seconds.
	MessageTimeout time.Duration

	// Credentials, when set, are sent with the connect request
	// (ArchiveAuthConnectRequest) for an archive with authentication enabled.
	Credentials []byte

	// ChallengeResponder answers an authentication challenge. It receives the
	// archive's encoded challenge and returns the credentials to send back.
	// When nil, a challenge fails the connect with ErrArchiveChallenge.
	ChallengeResponder func(encodedChallenge []byte) ([]byte, error)
}

func (c ArchiveConfig) withDefaults() (ArchiveConfig, error) {
	if c.ControlRequestChannel == "" {
		return c, errors.New("aergo: ArchiveConfig.ControlRequestChannel is required")
	}
	if c.ControlResponseChannel == "" {
		return c, errors.New("aergo: ArchiveConfig.ControlResponseChannel is required")
	}
	if c.ControlRequestStreamId == 0 {
		c.ControlRequestStreamId = ArchiveControlStreamIdDefault
	}
	if c.ControlResponseStreamId == 0 {
		c.ControlResponseStreamId = ArchiveControlResponseStreamIdDefault
	}
	if c.MessageTimeout <= 0 {
		c.MessageTimeout = archiveDefaultMessageTimeout
	}
	return c, nil
}

// The parts of the Aeron client, a publication and a subscription that Archive
// uses. *Aeron, *Publication and *Subscription provide them (see
// realArchiveAeron); tests substitute fakes so no driver is needed.
type archivePublication interface {
	IsConnected() bool
	OfferWithBackoff(ctx context.Context, buf []byte, timeout time.Duration) int64
	Close()
}

type archiveSubscription interface {
	Poll(handler FragmentHandler, fragmentLimit int) int
	Close()
}

// archiveEndpoints is what Archive needs from the Aeron client: it creates
// the two control channels, issues correlation ids and drives the conductor.
type archiveEndpoints interface {
	addRequestPublication(channel string, streamID int32) (archivePublication, error)
	addResponseSubscription(channel string, streamID int32) (archiveSubscription, error)
	addReplaySubscription(channel string, streamID int32) (*Subscription, error)
	NextCorrelationId() int64
	DoWork() int
}

// realArchiveAeron wraps *Aeron.
type realArchiveAeron struct{ *Aeron }

func (r realArchiveAeron) addRequestPublication(channel string, streamID int32) (archivePublication, error) {
	return r.Aeron.AddPublication(channel, streamID)
}

func (r realArchiveAeron) addReplaySubscription(channel string, streamID int32) (*Subscription, error) {
	return r.Aeron.AddSubscription(channel, streamID)
}

func (r realArchiveAeron) addResponseSubscription(channel string, streamID int32) (archiveSubscription, error) {
	return r.Aeron.AddSubscription(channel, streamID)
}

// Archive is a control session. Create one with ConnectArchive.
type Archive struct {
	mu sync.Mutex

	ac  archiveEndpoints
	cfg ArchiveConfig
	pub archivePublication
	sub archiveSubscription

	controlSessionId int64
	closed           bool

	// filterSession drops responses for other control sessions once ours is
	// known. It is zero while connecting, when the session id is not yet known.
	filterSession int64

	// The most recent response the handler decoded, for awaitResponse.
	pending archivePending
}

type archivePending struct {
	response     ArchiveControlResponse
	hasResponse  bool
	challenge    ArchiveChallenge
	hasChallenge bool

	// collectDescriptors makes onFragment keep RecordingDescriptor messages
	// for the awaited correlation id, for the list queries.
	collectDescriptors bool
	descriptors        []ArchiveRecordingDescriptor
}

// ConnectArchive opens a control session with the archive. It blocks until the
// archive accepts the session, rejects it, or the timeout passes.
func ConnectArchive(ctx context.Context, ac *Aeron, cfg ArchiveConfig) (*Archive, error) {
	return connectArchive(ctx, realArchiveAeron{ac}, cfg)
}

func connectArchive(ctx context.Context, ac archiveEndpoints, cfg ArchiveConfig) (*Archive, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}

	pub, err := ac.addRequestPublication(cfg.ControlRequestChannel, cfg.ControlRequestStreamId)
	if err != nil {
		return nil, fmt.Errorf("aergo: add archive control request publication: %w", err)
	}
	sub, err := ac.addResponseSubscription(cfg.ControlResponseChannel, cfg.ControlResponseStreamId)
	if err != nil {
		pub.Close()
		return nil, fmt.Errorf("aergo: add archive control response subscription: %w", err)
	}

	a := &Archive{ac: ac, cfg: cfg, pub: pub, sub: sub}
	if err := a.connect(ctx); err != nil {
		pub.Close()
		sub.Close()
		return nil, err
	}
	return a, nil
}

func (a *Archive) connect(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, a.cfg.MessageTimeout)
	defer cancel()

	if err := a.awaitPublicationConnected(ctx); err != nil {
		return err
	}

	correlationId := a.ac.NextCorrelationId()
	if len(a.cfg.Credentials) > 0 {
		req := &ArchiveAuthConnectRequest{
			CorrelationId:      correlationId,
			ResponseStreamId:   a.cfg.ControlResponseStreamId,
			Version:            ArchiveProtocolSemanticVersion,
			ResponseChannel:    a.cfg.ControlResponseChannel,
			EncodedCredentials: a.cfg.Credentials,
		}
		buf := make([]byte, req.EncodedLength())
		req.Encode(buf, 0)
		if err := a.offer(ctx, buf); err != nil {
			return err
		}
	} else {
		req := &ArchiveConnectRequest{
			CorrelationId:    correlationId,
			ResponseStreamId: a.cfg.ControlResponseStreamId,
			Version:          ArchiveProtocolSemanticVersion,
			ResponseChannel:  a.cfg.ControlResponseChannel,
		}
		buf := make([]byte, req.EncodedLength())
		req.Encode(buf, 0)
		if err := a.offer(ctx, buf); err != nil {
			return err
		}
	}

	resp, err := a.awaitConnectResponse(ctx, correlationId)
	if err != nil {
		return err
	}
	a.controlSessionId = resp.ControlSessionId
	a.filterSession = resp.ControlSessionId
	return nil
}

// awaitConnectResponse waits for the answer to the connect request, answering
// any authentication challenges on the way.
func (a *Archive) awaitConnectResponse(ctx context.Context, correlationId int64) (ArchiveControlResponse, error) {
	for {
		resp, challenge, err := a.awaitMessage(ctx, correlationId, true)
		if err != nil {
			return ArchiveControlResponse{}, err
		}
		if challenge == nil {
			if resp.Code != ArchiveControlResponseOK {
				return ArchiveControlResponse{}, archiveErrorFor(resp)
			}
			return resp, nil
		}

		if a.cfg.ChallengeResponder == nil {
			return ArchiveControlResponse{}, ErrArchiveChallenge
		}
		credentials, err := a.cfg.ChallengeResponder(challenge.EncodedChallenge)
		if err != nil {
			return ArchiveControlResponse{}, fmt.Errorf("aergo: archive challenge responder: %w", err)
		}
		correlationId = a.ac.NextCorrelationId()
		reply := &ArchiveChallengeResponse{
			ControlSessionId:   challenge.ControlSessionId,
			CorrelationId:      correlationId,
			EncodedCredentials: credentials,
		}
		buf := make([]byte, reply.EncodedLength())
		reply.Encode(buf, 0)
		if err := a.offer(ctx, buf); err != nil {
			return ArchiveControlResponse{}, err
		}
	}
}

func archiveErrorFor(resp ArchiveControlResponse) *ArchiveError {
	return &ArchiveError{
		CorrelationId: resp.CorrelationId,
		Code:          resp.Code,
		ErrorCode:     resp.RelevantId,
		Message:       resp.ErrorMessage,
	}
}

// awaitPublicationConnected waits for the control request publication to
// connect to the archive.
func (a *Archive) awaitPublicationConnected(ctx context.Context) error {
	for !a.pub.IsConnected() {
		a.ac.DoWork()
		select {
		case <-ctx.Done():
			return ErrArchiveNotConnected
		case <-time.After(time.Millisecond):
		}
	}
	return nil
}

// offer sends one encoded request, retrying while the publication is busy.
func (a *Archive) offer(ctx context.Context, buf []byte) error {
	result := a.pub.OfferWithBackoff(ctx, buf, a.cfg.MessageTimeout)
	switch {
	case result >= 0:
		return nil
	case result == NotConnected:
		return ErrArchiveNotConnected
	default:
		return fmt.Errorf("aergo: archive request offer failed: %s (%d)", OfferResultName(result), result)
	}
}

// awaitMessage polls the control response subscription until the archive sends
// a ControlResponse for correlationId (or, when allowChallenge, a Challenge for
// it). Messages for other correlation ids are skipped. When the answer is a
// challenge, resp is the zero value and challenge is non-nil.
func (a *Archive) awaitMessage(ctx context.Context, correlationId int64, allowChallenge bool) (ArchiveControlResponse, *ArchiveChallenge, error) {
	a.pending = archivePending{}
	handler := func(buf []byte, _ *Header) {
		a.onFragment(buf, correlationId, allowChallenge)
	}

	for {
		a.ac.DoWork()
		if a.sub.Poll(handler, archiveResponseFragmentLimit) == 0 {
			select {
			case <-ctx.Done():
				return ArchiveControlResponse{}, nil, ErrArchiveTimeout
			case <-time.After(50 * time.Microsecond):
			}
		}
		switch {
		case a.pending.hasResponse:
			return a.pending.response, nil, nil
		case a.pending.hasChallenge:
			challenge := a.pending.challenge
			return ArchiveControlResponse{}, &challenge, nil
		}
		if ctx.Err() != nil {
			return ArchiveControlResponse{}, nil, ErrArchiveTimeout
		}
	}
}

// onFragment decodes one message off the control response stream and records
// it if it answers correlationId. Anything it cannot decode, or that belongs
// to another schema or correlation id, is ignored.
func (a *Archive) onFragment(buf []byte, correlationId int64, allowChallenge bool) {
	if len(buf) < HeaderSize {
		return
	}
	var h MessageHeader
	h.Decode(buf, 0)
	if h.SchemaId != ArchiveSchemaId {
		return
	}
	body := HeaderSize
	switch h.TemplateId {
	case TemplateIdArchiveControlResponse:
		var resp ArchiveControlResponse
		if _, err := resp.Decode(buf, body, int(h.BlockLength)); err != nil {
			return
		}
		if resp.CorrelationId == correlationId && a.forThisSession(resp.ControlSessionId) {
			a.pending.response = resp
			a.pending.hasResponse = true
		}
	case TemplateIdArchiveRecordingDescriptor:
		if !a.pending.collectDescriptors {
			return
		}
		var rd ArchiveRecordingDescriptor
		if _, err := rd.Decode(buf, body, int(h.BlockLength)); err != nil {
			return
		}
		if rd.CorrelationId == correlationId && a.forThisSession(rd.ControlSessionId) {
			a.pending.descriptors = append(a.pending.descriptors, rd)
		}
	case TemplateIdArchiveChallenge:
		if !allowChallenge {
			return
		}
		var ch ArchiveChallenge
		if _, err := ch.Decode(buf, body, int(h.BlockLength)); err != nil {
			return
		}
		if ch.CorrelationId == correlationId {
			a.pending.challenge = ch
			a.pending.hasChallenge = true
		}
	}
}

// forThisSession reports whether a response's session id is ours, or whether we
// do not know ours yet.
func (a *Archive) forThisSession(controlSessionId int64) bool {
	return a.filterSession == 0 || controlSessionId == a.filterSession
}

// ControlSessionId returns the id the archive assigned to this session.
func (a *Archive) ControlSessionId() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.controlSessionId
}

// Close ends the control session. It is safe to call more than once.
func (a *Archive) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true

	// Best effort: the session expires on its own if the archive is gone.
	req := &ArchiveCloseSessionRequest{ControlSessionId: a.controlSessionId}
	buf := make([]byte, req.EncodedLength())
	req.Encode(buf, 0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a.pub.OfferWithBackoff(ctx, buf, time.Second)

	a.pub.Close()
	a.sub.Close()
	return nil
}

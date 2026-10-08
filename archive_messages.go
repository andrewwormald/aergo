package aergo

import (
	"errors"
	"fmt"
)

// Aeron Archive control protocol (schema 101), the subset a client needs to
// find a recording and replay it: connect, authenticate, list recordings,
// read positions, and start and stop a replay.
//
// Field layouts match the SBE codecs in io.aeron:aeron-archive 1.52.2
// (schema version 13). The golden vectors in archive_messages_test.go were
// produced by those Java encoders.
//
// Requests are encoded onto the control request publication. Responses
// (ArchiveControlResponse, ArchiveChallenge, ArchiveRecordingDescriptor and
// ArchiveRecordingSignalEvent) arrive on the control response subscription
// and are decoded. Every message is an SBE message header followed by a fixed
// block and then length-prefixed variable-length fields.

// Archive protocol schema constants.
const (
	ArchiveSchemaId      uint16 = 101
	ArchiveSchemaVersion uint16 = 13

	// ArchiveProtocolSemanticVersion is sent in ArchiveConnectRequest.Version:
	// the archive client protocol semantic version, major 1, minor 12, patch 0
	// (io.aeron.archive.client.AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION).
	// It is distinct from ArchiveSchemaVersion, the SBE schema version.
	ArchiveProtocolSemanticVersion int32 = 1<<16 | 12<<8

	// Default stream ids for the control request and response channels
	// (aeron.archive.control.stream.id and
	// aeron.archive.control.response.stream.id).
	ArchiveControlStreamIdDefault         int32 = 10
	ArchiveControlResponseStreamIdDefault int32 = 20
)

// Archive control protocol template ids.
const (
	TemplateIdArchiveControlResponse             = 1
	TemplateIdArchiveConnectRequest              = 2
	TemplateIdArchiveCloseSessionRequest         = 3
	TemplateIdArchiveReplayRequest               = 6
	TemplateIdArchiveStopReplayRequest           = 7
	TemplateIdArchiveListRecordingsForUriRequest = 9
	TemplateIdArchiveRecordingPositionRequest    = 12
	TemplateIdArchiveStopAllReplaysRequest       = 19
	TemplateIdArchiveRecordingDescriptor         = 22
	TemplateIdArchiveRecordingSignalEvent        = 24
	TemplateIdArchiveChallenge                   = 59
	TemplateIdArchiveChallengeResponse           = 60
	TemplateIdArchiveMaxRecordedPositionRequest  = 67
)

// ArchiveNullValue is the SBE null for int64 fields such as a replay length
// that means "to the end of the recording" or a position not yet known.
const ArchiveNullValue int64 = -1

// ArchiveControlResponseCode is the result code in an ArchiveControlResponse.
type ArchiveControlResponseCode int32

const (
	ArchiveControlResponseOK                  ArchiveControlResponseCode = 0
	ArchiveControlResponseError               ArchiveControlResponseCode = 1
	ArchiveControlResponseRecordingUnknown    ArchiveControlResponseCode = 2
	ArchiveControlResponseSubscriptionUnknown ArchiveControlResponseCode = 3
)

// String returns the code's name as the Java client prints it.
func (c ArchiveControlResponseCode) String() string {
	switch c {
	case ArchiveControlResponseOK:
		return "OK"
	case ArchiveControlResponseError:
		return "ERROR"
	case ArchiveControlResponseRecordingUnknown:
		return "RECORDING_UNKNOWN"
	case ArchiveControlResponseSubscriptionUnknown:
		return "SUBSCRIPTION_UNKNOWN"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int32(c))
	}
}

// ArchiveRecordingSignal is the signal in an ArchiveRecordingSignalEvent.
type ArchiveRecordingSignal int32

const (
	ArchiveRecordingSignalStart        ArchiveRecordingSignal = 0
	ArchiveRecordingSignalStop         ArchiveRecordingSignal = 1
	ArchiveRecordingSignalExtend       ArchiveRecordingSignal = 2
	ArchiveRecordingSignalReplicate    ArchiveRecordingSignal = 3
	ArchiveRecordingSignalMerge        ArchiveRecordingSignal = 4
	ArchiveRecordingSignalSync         ArchiveRecordingSignal = 5
	ArchiveRecordingSignalDelete       ArchiveRecordingSignal = 6
	ArchiveRecordingSignalReplicateEnd ArchiveRecordingSignal = 7
)

// ErrArchiveShortMessage is returned when a decoded message is shorter than
// its block length or a variable-length field runs past the end of the buffer.
var ErrArchiveShortMessage = errors.New("aergo: archive message is truncated")

// archiveVarBytes reads a length-prefixed variable-length field, checking it
// lies within buf. It returns the bytes of the field and the bytes consumed
// including the 4-byte length.
func archiveVarBytes(buf []byte, offset int) ([]byte, int, error) {
	if offset < 0 || offset+4 > len(buf) {
		return nil, 0, ErrArchiveShortMessage
	}
	length := int(getUint32(buf, offset))
	end := offset + 4 + length
	if length < 0 || end > len(buf) {
		return nil, 0, ErrArchiveShortMessage
	}
	return buf[offset+4 : end], 4 + length, nil
}

// archiveVarString is archiveVarBytes for a string field.
func archiveVarString(buf []byte, offset int) (string, int, error) {
	b, n, err := archiveVarBytes(buf, offset)
	if err != nil {
		return "", 0, err
	}
	return string(b), n, nil
}

func putVarBytes(buf []byte, offset int, b []byte) int {
	putUint32(buf, offset, uint32(len(b)))
	copy(buf[offset+4:], b)
	return 4 + len(b)
}

// archiveHeader writes the SBE message header for the given template and
// block length and returns its size.
func archiveHeader(buf []byte, offset int, blockLength int, templateId uint16) int {
	h := MessageHeader{
		BlockLength: uint16(blockLength),
		TemplateId:  templateId,
		SchemaId:    ArchiveSchemaId,
		Version:     ArchiveSchemaVersion,
	}
	return h.Encode(buf, offset)
}

// ---------------------------------------------------------------------------
// ArchiveConnectRequest (Template 2)
//
// Fixed fields (16 bytes): CorrelationId int64 @0, ResponseStreamId int32 @8,
// Version int32 @12; then ResponseChannel (var string).
// ---------------------------------------------------------------------------

const archiveConnectRequestBlockLength = 16

// ArchiveConnectRequest opens a control session. The archive answers on
// ResponseChannel and ResponseStreamId with an ArchiveControlResponse whose
// ControlSessionId identifies the session, or with an ArchiveChallenge when
// authentication is enabled.
type ArchiveConnectRequest struct {
	CorrelationId    int64
	ResponseStreamId int32
	Version          int32
	ResponseChannel  string
}

// EncodedLength returns the total encoded size, including the header.
func (m *ArchiveConnectRequest) EncodedLength() int {
	return HeaderSize + archiveConnectRequestBlockLength + 4 + len(m.ResponseChannel)
}

func (m *ArchiveConnectRequest) Encode(buf []byte, offset int) int {
	n := archiveHeader(buf, offset, archiveConnectRequestBlockLength, TemplateIdArchiveConnectRequest)
	base := offset + n
	putInt64(buf, base+0, m.CorrelationId)
	putInt32(buf, base+8, m.ResponseStreamId)
	putInt32(buf, base+12, m.Version)
	varN := putVarString(buf, base+archiveConnectRequestBlockLength, m.ResponseChannel)
	return n + archiveConnectRequestBlockLength + varN
}

// ---------------------------------------------------------------------------
// ArchiveCloseSessionRequest (Template 3)
//
// Fixed fields (8 bytes): ControlSessionId int64 @0.
// ---------------------------------------------------------------------------

const archiveCloseSessionRequestBlockLength = 8

// ArchiveCloseSessionRequest ends a control session and releases its replays.
type ArchiveCloseSessionRequest struct {
	ControlSessionId int64
}

func (m *ArchiveCloseSessionRequest) EncodedLength() int {
	return HeaderSize + archiveCloseSessionRequestBlockLength
}

func (m *ArchiveCloseSessionRequest) Encode(buf []byte, offset int) int {
	n := archiveHeader(buf, offset, archiveCloseSessionRequestBlockLength, TemplateIdArchiveCloseSessionRequest)
	putInt64(buf, offset+n+0, m.ControlSessionId)
	return n + archiveCloseSessionRequestBlockLength
}

// ---------------------------------------------------------------------------
// ArchiveChallengeResponse (Template 60)
//
// Fixed fields (16 bytes): ControlSessionId int64 @0, CorrelationId int64 @8;
// then EncodedCredentials (var bytes).
// ---------------------------------------------------------------------------

const archiveChallengeResponseBlockLength = 16

// ArchiveChallengeResponse answers an ArchiveChallenge during authentication.
type ArchiveChallengeResponse struct {
	ControlSessionId   int64
	CorrelationId      int64
	EncodedCredentials []byte
}

func (m *ArchiveChallengeResponse) EncodedLength() int {
	return HeaderSize + archiveChallengeResponseBlockLength + 4 + len(m.EncodedCredentials)
}

func (m *ArchiveChallengeResponse) Encode(buf []byte, offset int) int {
	n := archiveHeader(buf, offset, archiveChallengeResponseBlockLength, TemplateIdArchiveChallengeResponse)
	base := offset + n
	putInt64(buf, base+0, m.ControlSessionId)
	putInt64(buf, base+8, m.CorrelationId)
	varN := putVarBytes(buf, base+archiveChallengeResponseBlockLength, m.EncodedCredentials)
	return n + archiveChallengeResponseBlockLength + varN
}

// ---------------------------------------------------------------------------
// ArchiveListRecordingsForUriRequest (Template 9)
//
// Fixed fields (32 bytes): ControlSessionId int64 @0, CorrelationId int64 @8,
// FromRecordingId int64 @16, RecordCount int32 @24, StreamId int32 @28;
// then Channel (var string), a substring the recording's channel must contain.
// ---------------------------------------------------------------------------

const archiveListRecordingsForUriRequestBlockLength = 32

// ArchiveListRecordingsForUriRequest asks for up to RecordCount recordings
// from FromRecordingId whose channel contains Channel and whose stream id is
// StreamId. The archive sends one ArchiveRecordingDescriptor per match and
// then an ArchiveControlResponse.
type ArchiveListRecordingsForUriRequest struct {
	ControlSessionId int64
	CorrelationId    int64
	FromRecordingId  int64
	RecordCount      int32
	StreamId         int32
	Channel          string
}

func (m *ArchiveListRecordingsForUriRequest) EncodedLength() int {
	return HeaderSize + archiveListRecordingsForUriRequestBlockLength + 4 + len(m.Channel)
}

func (m *ArchiveListRecordingsForUriRequest) Encode(buf []byte, offset int) int {
	n := archiveHeader(buf, offset, archiveListRecordingsForUriRequestBlockLength, TemplateIdArchiveListRecordingsForUriRequest)
	base := offset + n
	putInt64(buf, base+0, m.ControlSessionId)
	putInt64(buf, base+8, m.CorrelationId)
	putInt64(buf, base+16, m.FromRecordingId)
	putInt32(buf, base+24, m.RecordCount)
	putInt32(buf, base+28, m.StreamId)
	varN := putVarString(buf, base+archiveListRecordingsForUriRequestBlockLength, m.Channel)
	return n + archiveListRecordingsForUriRequestBlockLength + varN
}

// ---------------------------------------------------------------------------
// ArchiveMaxRecordedPositionRequest (Template 67),
// ArchiveRecordingPositionRequest (Template 12),
// ArchiveStopAllReplaysRequest (Template 19)
//
// Fixed fields (24 bytes): ControlSessionId int64 @0, CorrelationId int64 @8,
// RecordingId int64 @16.
// ---------------------------------------------------------------------------

const archiveRecordingIdRequestBlockLength = 24

// ArchiveMaxRecordedPositionRequest asks how far a recording can be replayed,
// including data still being recorded. The position comes back in
// ArchiveControlResponse.RelevantId.
type ArchiveMaxRecordedPositionRequest struct {
	ControlSessionId int64
	CorrelationId    int64
	RecordingId      int64
}

func (m *ArchiveMaxRecordedPositionRequest) EncodedLength() int {
	return HeaderSize + archiveRecordingIdRequestBlockLength
}

func (m *ArchiveMaxRecordedPositionRequest) Encode(buf []byte, offset int) int {
	return encodeArchiveRecordingIdRequest(buf, offset, TemplateIdArchiveMaxRecordedPositionRequest,
		m.ControlSessionId, m.CorrelationId, m.RecordingId)
}

// ArchiveRecordingPositionRequest asks for the current recording position of
// a recording that is still active. The position comes back in
// ArchiveControlResponse.RelevantId.
type ArchiveRecordingPositionRequest struct {
	ControlSessionId int64
	CorrelationId    int64
	RecordingId      int64
}

func (m *ArchiveRecordingPositionRequest) EncodedLength() int {
	return HeaderSize + archiveRecordingIdRequestBlockLength
}

func (m *ArchiveRecordingPositionRequest) Encode(buf []byte, offset int) int {
	return encodeArchiveRecordingIdRequest(buf, offset, TemplateIdArchiveRecordingPositionRequest,
		m.ControlSessionId, m.CorrelationId, m.RecordingId)
}

// ArchiveStopAllReplaysRequest stops every replay of a recording on this
// control session.
type ArchiveStopAllReplaysRequest struct {
	ControlSessionId int64
	CorrelationId    int64
	RecordingId      int64
}

func (m *ArchiveStopAllReplaysRequest) EncodedLength() int {
	return HeaderSize + archiveRecordingIdRequestBlockLength
}

func (m *ArchiveStopAllReplaysRequest) Encode(buf []byte, offset int) int {
	return encodeArchiveRecordingIdRequest(buf, offset, TemplateIdArchiveStopAllReplaysRequest,
		m.ControlSessionId, m.CorrelationId, m.RecordingId)
}

func encodeArchiveRecordingIdRequest(buf []byte, offset int, templateId uint16, controlSessionId, correlationId, recordingId int64) int {
	n := archiveHeader(buf, offset, archiveRecordingIdRequestBlockLength, templateId)
	base := offset + n
	putInt64(buf, base+0, controlSessionId)
	putInt64(buf, base+8, correlationId)
	putInt64(buf, base+16, recordingId)
	return n + archiveRecordingIdRequestBlockLength
}

// ---------------------------------------------------------------------------
// ArchiveReplayRequest (Template 6)
//
// Fixed fields (56 bytes): ControlSessionId int64 @0, CorrelationId int64 @8,
// RecordingId int64 @16, Position int64 @24, Length int64 @32,
// ReplayStreamId int32 @40, FileIoMaxLength int32 @44, ReplayToken int64 @48;
// then ReplayChannel (var string).
// ---------------------------------------------------------------------------

const archiveReplayRequestBlockLength = 56

// ArchiveReplayRequest starts a replay of a recording. The archive publishes
// the replayed data on ReplayChannel and ReplayStreamId, and answers with an
// ArchiveControlResponse whose RelevantId is the replay session id.
//
// Position is the stream position to start from (it must be frame aligned).
// Length is the number of bytes to replay; ArchiveNullValue replays to the end
// of the recording and keeps following it while it is active. FileIoMaxLength
// of ArchiveNullValue (as an int32, math.MinInt32) uses the archive default.
type ArchiveReplayRequest struct {
	ControlSessionId int64
	CorrelationId    int64
	RecordingId      int64
	Position         int64
	Length           int64
	ReplayStreamId   int32
	FileIoMaxLength  int32
	ReplayToken      int64
	ReplayChannel    string
}

func (m *ArchiveReplayRequest) EncodedLength() int {
	return HeaderSize + archiveReplayRequestBlockLength + 4 + len(m.ReplayChannel)
}

func (m *ArchiveReplayRequest) Encode(buf []byte, offset int) int {
	n := archiveHeader(buf, offset, archiveReplayRequestBlockLength, TemplateIdArchiveReplayRequest)
	base := offset + n
	putInt64(buf, base+0, m.ControlSessionId)
	putInt64(buf, base+8, m.CorrelationId)
	putInt64(buf, base+16, m.RecordingId)
	putInt64(buf, base+24, m.Position)
	putInt64(buf, base+32, m.Length)
	putInt32(buf, base+40, m.ReplayStreamId)
	putInt32(buf, base+44, m.FileIoMaxLength)
	putInt64(buf, base+48, m.ReplayToken)
	varN := putVarString(buf, base+archiveReplayRequestBlockLength, m.ReplayChannel)
	return n + archiveReplayRequestBlockLength + varN
}

// ---------------------------------------------------------------------------
// ArchiveStopReplayRequest (Template 7)
//
// Fixed fields (24 bytes): ControlSessionId int64 @0, CorrelationId int64 @8,
// ReplaySessionId int64 @16.
// ---------------------------------------------------------------------------

const archiveStopReplayRequestBlockLength = 24

// ArchiveStopReplayRequest stops one replay by the session id returned from
// an ArchiveReplayRequest.
type ArchiveStopReplayRequest struct {
	ControlSessionId int64
	CorrelationId    int64
	ReplaySessionId  int64
}

func (m *ArchiveStopReplayRequest) EncodedLength() int {
	return HeaderSize + archiveStopReplayRequestBlockLength
}

func (m *ArchiveStopReplayRequest) Encode(buf []byte, offset int) int {
	n := archiveHeader(buf, offset, archiveStopReplayRequestBlockLength, TemplateIdArchiveStopReplayRequest)
	base := offset + n
	putInt64(buf, base+0, m.ControlSessionId)
	putInt64(buf, base+8, m.CorrelationId)
	putInt64(buf, base+16, m.ReplaySessionId)
	return n + archiveStopReplayRequestBlockLength
}

// ---------------------------------------------------------------------------
// ArchiveControlResponse (Template 1)
//
// Fixed fields (32 bytes): ControlSessionId int64 @0, CorrelationId int64 @8,
// RelevantId int64 @16, Code int32 @24, Version int32 @28; then ErrorMessage
// (var string).
// ---------------------------------------------------------------------------

const archiveControlResponseBlockLength = 32

// ArchiveControlResponse is the archive's answer to a request. RelevantId
// carries the request's result: a replay session id, a position, or the error
// code for Code ERROR.
type ArchiveControlResponse struct {
	ControlSessionId int64
	CorrelationId    int64
	RelevantId       int64
	Code             ArchiveControlResponseCode
	Version          int32
	ErrorMessage     string
}

// Decode reads the message body that follows the SBE header. blockLength is
// the header's BlockLength, which may exceed ours when the archive is newer.
// It returns the bytes consumed.
func (m *ArchiveControlResponse) Decode(buf []byte, offset int, blockLength int) (int, error) {
	if blockLength < archiveControlResponseBlockLength || offset+blockLength > len(buf) {
		return 0, ErrArchiveShortMessage
	}
	m.ControlSessionId = getInt64(buf, offset+0)
	m.CorrelationId = getInt64(buf, offset+8)
	m.RelevantId = getInt64(buf, offset+16)
	m.Code = ArchiveControlResponseCode(getInt32(buf, offset+24))
	m.Version = getInt32(buf, offset+28)
	msg, varN, err := archiveVarString(buf, offset+blockLength)
	if err != nil {
		return 0, err
	}
	m.ErrorMessage = msg
	return blockLength + varN, nil
}

// ---------------------------------------------------------------------------
// ArchiveChallenge (Template 59)
//
// Fixed fields (20 bytes): ControlSessionId int64 @0, CorrelationId int64 @8,
// Version int32 @16; then EncodedChallenge (var bytes).
// ---------------------------------------------------------------------------

const archiveChallengeBlockLength = 20

// ArchiveChallenge is sent during authentication. The client answers with an
// ArchiveChallengeResponse carrying credentials built from EncodedChallenge.
type ArchiveChallenge struct {
	ControlSessionId int64
	CorrelationId    int64
	Version          int32
	EncodedChallenge []byte
}

func (m *ArchiveChallenge) Decode(buf []byte, offset int, blockLength int) (int, error) {
	if blockLength < archiveChallengeBlockLength || offset+blockLength > len(buf) {
		return 0, ErrArchiveShortMessage
	}
	m.ControlSessionId = getInt64(buf, offset+0)
	m.CorrelationId = getInt64(buf, offset+8)
	m.Version = getInt32(buf, offset+16)
	b, varN, err := archiveVarBytes(buf, offset+blockLength)
	if err != nil {
		return 0, err
	}
	m.EncodedChallenge = append(m.EncodedChallenge[:0], b...)
	return blockLength + varN, nil
}

// ---------------------------------------------------------------------------
// ArchiveRecordingDescriptor (Template 22)
//
// Fixed fields (80 bytes): ControlSessionId int64 @0, CorrelationId int64 @8,
// RecordingId int64 @16, StartTimestamp int64 @24, StopTimestamp int64 @32,
// StartPosition int64 @40, StopPosition int64 @48, InitialTermId int32 @56,
// SegmentFileLength int32 @60, TermBufferLength int32 @64, MtuLength int32 @68,
// SessionId int32 @72, StreamId int32 @76; then StrippedChannel,
// OriginalChannel and SourceIdentity (var strings).
// ---------------------------------------------------------------------------

const archiveRecordingDescriptorBlockLength = 80

// ArchiveRecordingDescriptor describes one recording. StopPosition is
// ArchiveNullValue while the recording is active.
type ArchiveRecordingDescriptor struct {
	ControlSessionId  int64
	CorrelationId     int64
	RecordingId       int64
	StartTimestamp    int64
	StopTimestamp     int64
	StartPosition     int64
	StopPosition      int64
	InitialTermId     int32
	SegmentFileLength int32
	TermBufferLength  int32
	MtuLength         int32
	SessionId         int32
	StreamId          int32
	StrippedChannel   string
	OriginalChannel   string
	SourceIdentity    string
}

func (m *ArchiveRecordingDescriptor) Decode(buf []byte, offset int, blockLength int) (int, error) {
	if blockLength < archiveRecordingDescriptorBlockLength || offset+blockLength > len(buf) {
		return 0, ErrArchiveShortMessage
	}
	m.ControlSessionId = getInt64(buf, offset+0)
	m.CorrelationId = getInt64(buf, offset+8)
	m.RecordingId = getInt64(buf, offset+16)
	m.StartTimestamp = getInt64(buf, offset+24)
	m.StopTimestamp = getInt64(buf, offset+32)
	m.StartPosition = getInt64(buf, offset+40)
	m.StopPosition = getInt64(buf, offset+48)
	m.InitialTermId = getInt32(buf, offset+56)
	m.SegmentFileLength = getInt32(buf, offset+60)
	m.TermBufferLength = getInt32(buf, offset+64)
	m.MtuLength = getInt32(buf, offset+68)
	m.SessionId = getInt32(buf, offset+72)
	m.StreamId = getInt32(buf, offset+76)

	pos := offset + blockLength
	var err error
	var n int
	if m.StrippedChannel, n, err = archiveVarString(buf, pos); err != nil {
		return 0, err
	}
	pos += n
	if m.OriginalChannel, n, err = archiveVarString(buf, pos); err != nil {
		return 0, err
	}
	pos += n
	if m.SourceIdentity, n, err = archiveVarString(buf, pos); err != nil {
		return 0, err
	}
	pos += n
	return pos - offset, nil
}

// ---------------------------------------------------------------------------
// ArchiveRecordingSignalEvent (Template 24)
//
// Fixed fields (44 bytes): ControlSessionId int64 @0, CorrelationId int64 @8,
// RecordingId int64 @16, SubscriptionId int64 @24, Position int64 @32,
// Signal int32 @40.
// ---------------------------------------------------------------------------

const archiveRecordingSignalEventBlockLength = 44

// ArchiveRecordingSignalEvent is an asynchronous notice about a recording,
// such as it starting or stopping.
type ArchiveRecordingSignalEvent struct {
	ControlSessionId int64
	CorrelationId    int64
	RecordingId      int64
	SubscriptionId   int64
	Position         int64
	Signal           ArchiveRecordingSignal
}

func (m *ArchiveRecordingSignalEvent) Decode(buf []byte, offset int, blockLength int) (int, error) {
	if blockLength < archiveRecordingSignalEventBlockLength || offset+blockLength > len(buf) {
		return 0, ErrArchiveShortMessage
	}
	m.ControlSessionId = getInt64(buf, offset+0)
	m.CorrelationId = getInt64(buf, offset+8)
	m.RecordingId = getInt64(buf, offset+16)
	m.SubscriptionId = getInt64(buf, offset+24)
	m.Position = getInt64(buf, offset+32)
	m.Signal = ArchiveRecordingSignal(getInt32(buf, offset+40))
	return blockLength, nil
}

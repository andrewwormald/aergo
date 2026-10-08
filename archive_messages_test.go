package aergo

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

// Golden vectors produced by the Java encoders in io.aeron:aeron-archive 1.52.2
// (schema 101, version 13), so the codecs are checked against the real wire
// format and not only against themselves.
var archiveVectors = map[string]string{
	"AuthConnectRequest":          "10003a0065000d00887766554433221114000000000c01001e0000006165726f6e3a7564703f656e64706f696e743d6c6f63616c686f73743a3003000000050607",
	"ConnectRequest":              "1000020065000d00887766554433221114000000000c01001e0000006165726f6e3a7564703f656e64706f696e743d6c6f63616c686f73743a30",
	"CloseSessionRequest":         "0800030065000d000807060504030201",
	"ChallengeResponse":           "10003c0065000d000b0000000000000016000000000000000400000001020304",
	"ListRecordingsForUriRequest": "2000090065000d000700000000000000080000000000000009000000000000000a000000b70000000c000000616c6961733d656772657373",
	"MaxRecordedPositionRequest":  "1800430065000d00010000000000000002000000000000000300000000000000",
	"RecordingPositionRequest":    "18000c0065000d00040000000000000005000000000000000600000000000000",
	"ReplayRequest":               "3800060065000d000100000000000000020000000000000003000000000000000010000000000000ffffffffffffffffb7000000050000000900000000000000220000006165726f6e3a7564703f656e64706f696e743d6c6f63616c686f73743a3230313233",
	"StopReplayRequest":           "1800070065000d00010000000000000002000000000000002100000000000000",
	"StopAllReplaysRequest":       "1800130065000d00010000000000000002000000000000000300000000000000",
	"ControlResponse":             "2000010065000d006400000000000000c8000000000000002c0100000000000001000000000c010004000000626f6f6d",
	"Challenge":                   "14003b0065000d0005000000000000000600000000000000000c010003000000090807",
	"RecordingDescriptor":         "5000160065000d00010000000000000002000000000000000300000000000000040000000000000005000000000000000600000000000000070000000000000008000000090000000a0000000b0000000c000000b7000000090000006165726f6e3a7564702e0000006165726f6e3a7564703f616c6961733d6567726573737c636f6e74726f6c3d6c6f63616c686f73743a31303030390e0000003132372e302e302e313a31323334",
	"RecordingSignalEvent":        "2c00180065000d000100000000000000020000000000000003000000000000000400000000000000050000000000000001000000",
}

func archiveVector(t *testing.T, name string) []byte {
	t.Helper()
	b, err := hex.DecodeString(archiveVectors[name])
	if err != nil || len(b) == 0 {
		t.Fatalf("vector %s: %v", name, err)
	}
	return b
}

// checkEncode encodes with fn and compares to the Java bytes.
func checkEncode(t *testing.T, name string, encodedLength int, fn func(buf []byte) int) {
	t.Helper()
	want := archiveVector(t, name)
	buf := make([]byte, 4096)
	n := fn(buf)
	if !bytes.Equal(buf[:n], want) {
		t.Errorf("%s: encoded bytes differ\n got %x\nwant %x", name, buf[:n], want)
	}
	if encodedLength != len(want) {
		t.Errorf("%s: EncodedLength = %d, want %d", name, encodedLength, len(want))
	}
}

func TestArchiveRequestsMatchJavaEncoders(t *testing.T) {
	c := &ArchiveConnectRequest{CorrelationId: 0x1122334455667788, ResponseStreamId: 20, Version: ArchiveProtocolSemanticVersion, ResponseChannel: "aeron:udp?endpoint=localhost:0"}
	checkEncode(t, "ConnectRequest", c.EncodedLength(), func(b []byte) int { return c.Encode(b, 0) })

	ac := &ArchiveAuthConnectRequest{CorrelationId: 0x1122334455667788, ResponseStreamId: 20, Version: ArchiveProtocolSemanticVersion, ResponseChannel: "aeron:udp?endpoint=localhost:0", EncodedCredentials: []byte{5, 6, 7}}
	checkEncode(t, "AuthConnectRequest", ac.EncodedLength(), func(b []byte) int { return ac.Encode(b, 0) })

	cs := &ArchiveCloseSessionRequest{ControlSessionId: 0x0102030405060708}
	checkEncode(t, "CloseSessionRequest", cs.EncodedLength(), func(b []byte) int { return cs.Encode(b, 0) })

	cr := &ArchiveChallengeResponse{ControlSessionId: 11, CorrelationId: 22, EncodedCredentials: []byte{1, 2, 3, 4}}
	checkEncode(t, "ChallengeResponse", cr.EncodedLength(), func(b []byte) int { return cr.Encode(b, 0) })

	l := &ArchiveListRecordingsForUriRequest{ControlSessionId: 7, CorrelationId: 8, FromRecordingId: 9, RecordCount: 10, StreamId: 183, Channel: "alias=egress"}
	checkEncode(t, "ListRecordingsForUriRequest", l.EncodedLength(), func(b []byte) int { return l.Encode(b, 0) })

	m := &ArchiveMaxRecordedPositionRequest{ControlSessionId: 1, CorrelationId: 2, RecordingId: 3}
	checkEncode(t, "MaxRecordedPositionRequest", m.EncodedLength(), func(b []byte) int { return m.Encode(b, 0) })

	rp := &ArchiveRecordingPositionRequest{ControlSessionId: 4, CorrelationId: 5, RecordingId: 6}
	checkEncode(t, "RecordingPositionRequest", rp.EncodedLength(), func(b []byte) int { return rp.Encode(b, 0) })

	r := &ArchiveReplayRequest{ControlSessionId: 1, CorrelationId: 2, RecordingId: 3, Position: 4096, Length: ArchiveNullValue, ReplayStreamId: 183, FileIoMaxLength: 5, ReplayToken: 9, ReplayChannel: "aeron:udp?endpoint=localhost:20123"}
	checkEncode(t, "ReplayRequest", r.EncodedLength(), func(b []byte) int { return r.Encode(b, 0) })

	sr := &ArchiveStopReplayRequest{ControlSessionId: 1, CorrelationId: 2, ReplaySessionId: 33}
	checkEncode(t, "StopReplayRequest", sr.EncodedLength(), func(b []byte) int { return sr.Encode(b, 0) })

	sa := &ArchiveStopAllReplaysRequest{ControlSessionId: 1, CorrelationId: 2, RecordingId: 3}
	checkEncode(t, "StopAllReplaysRequest", sa.EncodedLength(), func(b []byte) int { return sa.Encode(b, 0) })
}

// header checks the SBE header of a vector and returns the offset of the body
// and the block length.
func archiveBody(t *testing.T, name string, template uint16) ([]byte, int, int) {
	t.Helper()
	buf := archiveVector(t, name)
	var h MessageHeader
	h.Decode(buf, 0)
	if h.SchemaId != ArchiveSchemaId || h.Version != ArchiveSchemaVersion || h.TemplateId != template {
		t.Fatalf("%s: header = %+v, want schema %d version %d template %d", name, h, ArchiveSchemaId, ArchiveSchemaVersion, template)
	}
	return buf, HeaderSize, int(h.BlockLength)
}

func TestArchiveResponsesDecodeJavaEncoders(t *testing.T) {
	buf, off, bl := archiveBody(t, "ControlResponse", TemplateIdArchiveControlResponse)
	var cr ArchiveControlResponse
	if n, err := cr.Decode(buf, off, bl); err != nil || off+n != len(buf) {
		t.Fatalf("ControlResponse decode: n=%d err=%v len=%d", n, err, len(buf))
	}
	want := ArchiveControlResponse{ControlSessionId: 100, CorrelationId: 200, RelevantId: 300, Code: ArchiveControlResponseError, Version: ArchiveProtocolSemanticVersion, ErrorMessage: "boom"}
	if cr != want {
		t.Errorf("ControlResponse = %+v, want %+v", cr, want)
	}

	buf, off, bl = archiveBody(t, "Challenge", TemplateIdArchiveChallenge)
	var ch ArchiveChallenge
	if n, err := ch.Decode(buf, off, bl); err != nil || off+n != len(buf) {
		t.Fatalf("Challenge decode: n=%d err=%v", n, err)
	}
	if ch.ControlSessionId != 5 || ch.CorrelationId != 6 || ch.Version != ArchiveProtocolSemanticVersion || !bytes.Equal(ch.EncodedChallenge, []byte{9, 8, 7}) {
		t.Errorf("Challenge = %+v", ch)
	}

	buf, off, bl = archiveBody(t, "RecordingDescriptor", TemplateIdArchiveRecordingDescriptor)
	var rd ArchiveRecordingDescriptor
	if n, err := rd.Decode(buf, off, bl); err != nil || off+n != len(buf) {
		t.Fatalf("RecordingDescriptor decode: n=%d err=%v", n, err)
	}
	wantRD := ArchiveRecordingDescriptor{
		ControlSessionId: 1, CorrelationId: 2, RecordingId: 3, StartTimestamp: 4, StopTimestamp: 5,
		StartPosition: 6, StopPosition: 7, InitialTermId: 8, SegmentFileLength: 9, TermBufferLength: 10,
		MtuLength: 11, SessionId: 12, StreamId: 183,
		StrippedChannel: "aeron:udp", OriginalChannel: "aeron:udp?alias=egress|control=localhost:10009", SourceIdentity: "127.0.0.1:1234",
	}
	if rd != wantRD {
		t.Errorf("RecordingDescriptor = %+v, want %+v", rd, wantRD)
	}

	buf, off, bl = archiveBody(t, "RecordingSignalEvent", TemplateIdArchiveRecordingSignalEvent)
	var rs ArchiveRecordingSignalEvent
	if n, err := rs.Decode(buf, off, bl); err != nil || off+n != len(buf) {
		t.Fatalf("RecordingSignalEvent decode: n=%d err=%v", n, err)
	}
	wantRS := ArchiveRecordingSignalEvent{ControlSessionId: 1, CorrelationId: 2, RecordingId: 3, SubscriptionId: 4, Position: 5, Signal: ArchiveRecordingSignalStop}
	if rs != wantRS {
		t.Errorf("RecordingSignalEvent = %+v, want %+v", rs, wantRS)
	}
}

// A response that is cut short, or whose variable-length field claims more
// bytes than arrived, must be an error and never a panic: these bytes come off
// the network.
func TestArchiveDecodeRejectsTruncatedInput(t *testing.T) {
	cases := []struct {
		name     string
		template uint16
		decode   func(buf []byte, off, bl int) (int, error)
	}{
		{"ControlResponse", TemplateIdArchiveControlResponse, func(b []byte, o, bl int) (int, error) { var m ArchiveControlResponse; return m.Decode(b, o, bl) }},
		{"Challenge", TemplateIdArchiveChallenge, func(b []byte, o, bl int) (int, error) { var m ArchiveChallenge; return m.Decode(b, o, bl) }},
		{"RecordingDescriptor", TemplateIdArchiveRecordingDescriptor, func(b []byte, o, bl int) (int, error) { var m ArchiveRecordingDescriptor; return m.Decode(b, o, bl) }},
		{"RecordingSignalEvent", TemplateIdArchiveRecordingSignalEvent, func(b []byte, o, bl int) (int, error) { var m ArchiveRecordingSignalEvent; return m.Decode(b, o, bl) }},
	}
	for _, tc := range cases {
		buf, off, bl := archiveBody(t, tc.name, tc.template)
		for cut := 0; cut < len(buf); cut++ {
			_, err := tc.decode(buf[:cut], off, bl)
			if cut < len(buf) && err == nil && cut < off+bl {
				t.Errorf("%s: decoded %d of %d bytes without error", tc.name, cut, len(buf))
			}
			if err != nil && !errors.Is(err, ErrArchiveShortMessage) {
				t.Errorf("%s: cut %d: err = %v", tc.name, cut, err)
			}
		}
	}

	// A length prefix larger than the rest of the buffer.
	buf, off, bl := archiveBody(t, "ControlResponse", TemplateIdArchiveControlResponse)
	putUint32(buf, off+bl, 0xffffff)
	var m ArchiveControlResponse
	if _, err := m.Decode(buf, off, bl); !errors.Is(err, ErrArchiveShortMessage) {
		t.Errorf("oversized var-data length: err = %v", err)
	}
}

func TestArchiveControlResponseCodeString(t *testing.T) {
	for code, want := range map[ArchiveControlResponseCode]string{
		ArchiveControlResponseOK:                  "OK",
		ArchiveControlResponseError:               "ERROR",
		ArchiveControlResponseRecordingUnknown:    "RECORDING_UNKNOWN",
		ArchiveControlResponseSubscriptionUnknown: "SUBSCRIPTION_UNKNOWN",
		42: "UNKNOWN(42)",
	} {
		if got := code.String(); got != want {
			t.Errorf("code %d: %q, want %q", int32(code), got, want)
		}
	}
}

func TestArchiveProtocolSemanticVersion(t *testing.T) {
	// 1.12.0, as io.aeron.archive.client.AeronArchive.Configuration sends.
	if ArchiveProtocolSemanticVersion != 0x010C00 {
		t.Errorf("ArchiveProtocolSemanticVersion = %#x, want 0x010C00", ArchiveProtocolSemanticVersion)
	}
}

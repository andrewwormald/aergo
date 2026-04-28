package cluster

import (
	"bytes"
	"testing"

	"github.com/andrewwormald/aergo/pkg/codec/sbe"
)

func TestSessionMessageHeaderRoundTrip(t *testing.T) {
	buf := make([]byte, 256)
	msg := SessionMessageHeader{
		LeadershipTermId: 42,
		ClusterSessionId: 7,
		Timestamp:        1234567890,
	}
	n := msg.Encode(buf, 0)
	if n != sbe.HeaderSize+sessionMessageHeaderBlockLength {
		t.Fatalf("expected %d bytes, got %d", sbe.HeaderSize+sessionMessageHeaderBlockLength, n)
	}

	var hdr sbe.MessageHeader
	hdr.Decode(buf, 0)
	if hdr.TemplateId != TemplateIdSessionMessageHeader {
		t.Fatalf("expected template %d, got %d", TemplateIdSessionMessageHeader, hdr.TemplateId)
	}
	if hdr.SchemaId != SchemaId {
		t.Fatalf("expected schema %d, got %d", SchemaId, hdr.SchemaId)
	}
	if hdr.BlockLength != sessionMessageHeaderBlockLength {
		t.Fatalf("expected block length %d, got %d", sessionMessageHeaderBlockLength, hdr.BlockLength)
	}

	var decoded SessionMessageHeader
	decoded.Decode(buf, sbe.HeaderSize)
	if decoded.LeadershipTermId != 42 {
		t.Fatalf("LeadershipTermId: expected 42, got %d", decoded.LeadershipTermId)
	}
	if decoded.ClusterSessionId != 7 {
		t.Fatalf("ClusterSessionId: expected 7, got %d", decoded.ClusterSessionId)
	}
	if decoded.Timestamp != 1234567890 {
		t.Fatalf("Timestamp: expected 1234567890, got %d", decoded.Timestamp)
	}
}

func TestSessionConnectRequestRoundTrip(t *testing.T) {
	buf := make([]byte, 256)
	msg := SessionConnectRequest{
		CorrelationId:    99,
		ResponseStreamId: 102,
		Version:          8,
		ResponseChannel:  "aeron:udp?endpoint=localhost:0",
	}
	n := msg.Encode(buf, 0)

	var hdr sbe.MessageHeader
	hdr.Decode(buf, 0)
	if hdr.TemplateId != TemplateIdSessionConnectRequest {
		t.Fatalf("expected template %d, got %d", TemplateIdSessionConnectRequest, hdr.TemplateId)
	}

	var decoded SessionConnectRequest
	consumed := decoded.Decode(buf, sbe.HeaderSize)
	if consumed != n-sbe.HeaderSize {
		t.Fatalf("consumed %d, expected %d", consumed, n-sbe.HeaderSize)
	}
	if decoded.CorrelationId != 99 {
		t.Fatalf("CorrelationId: expected 99, got %d", decoded.CorrelationId)
	}
	if decoded.ResponseStreamId != 102 {
		t.Fatalf("ResponseStreamId: expected 102, got %d", decoded.ResponseStreamId)
	}
	if decoded.Version != 8 {
		t.Fatalf("Version: expected 8, got %d", decoded.Version)
	}
	if decoded.ResponseChannel != "aeron:udp?endpoint=localhost:0" {
		t.Fatalf("ResponseChannel: expected 'aeron:udp?endpoint=localhost:0', got '%s'", decoded.ResponseChannel)
	}
}

func TestSessionEventRoundTrip(t *testing.T) {
	buf := make([]byte, 256)
	msg := SessionEvent{
		ClusterSessionId: 12,
		CorrelationId:    99,
		LeadershipTermId: 1,
		LeaderMemberId:   0,
		Code:             EventCodeOK,
		Detail:           "session opened",
	}
	n := msg.Encode(buf, 0)

	var hdr sbe.MessageHeader
	hdr.Decode(buf, 0)
	if hdr.TemplateId != TemplateIdSessionEvent {
		t.Fatalf("expected template %d, got %d", TemplateIdSessionEvent, hdr.TemplateId)
	}

	var decoded SessionEvent
	consumed := decoded.Decode(buf, sbe.HeaderSize)
	if consumed != n-sbe.HeaderSize {
		t.Fatalf("consumed %d, expected %d", consumed, n-sbe.HeaderSize)
	}
	if decoded.ClusterSessionId != 12 {
		t.Fatalf("ClusterSessionId: expected 12, got %d", decoded.ClusterSessionId)
	}
	if decoded.CorrelationId != 99 {
		t.Fatalf("CorrelationId: expected 99, got %d", decoded.CorrelationId)
	}
	if decoded.Code != EventCodeOK {
		t.Fatalf("Code: expected %d, got %d", EventCodeOK, decoded.Code)
	}
	if decoded.Detail != "session opened" {
		t.Fatalf("Detail: expected 'session opened', got '%s'", decoded.Detail)
	}
}

func TestSessionCloseRequestRoundTrip(t *testing.T) {
	buf := make([]byte, 256)
	msg := SessionCloseRequest{
		ClusterSessionId: 55,
		LeadershipTermId: 3,
	}
	n := msg.Encode(buf, 0)

	var decoded SessionCloseRequest
	decoded.Decode(buf, sbe.HeaderSize)
	if decoded.ClusterSessionId != 55 {
		t.Fatalf("ClusterSessionId: expected 55, got %d", decoded.ClusterSessionId)
	}
	if decoded.LeadershipTermId != 3 {
		t.Fatalf("LeadershipTermId: expected 3, got %d", decoded.LeadershipTermId)
	}
	_ = n
}

func TestSessionKeepAliveRoundTrip(t *testing.T) {
	buf := make([]byte, 256)
	msg := SessionKeepAlive{
		LeadershipTermId: 7,
		ClusterSessionId: 88,
	}
	msg.Encode(buf, 0)

	var decoded SessionKeepAlive
	decoded.Decode(buf, sbe.HeaderSize)
	if decoded.LeadershipTermId != 7 {
		t.Fatalf("LeadershipTermId: expected 7, got %d", decoded.LeadershipTermId)
	}
	if decoded.ClusterSessionId != 88 {
		t.Fatalf("ClusterSessionId: expected 88, got %d", decoded.ClusterSessionId)
	}
}

func TestNewLeaderEventRoundTrip(t *testing.T) {
	buf := make([]byte, 256)
	msg := NewLeaderEvent{
		ClusterSessionId: 10,
		LeadershipTermId: 2,
		LeaderMemberId:   1,
		IngressEndpoints: "0=localhost:10000,1=localhost:10001,2=localhost:10002",
	}
	n := msg.Encode(buf, 0)

	var decoded NewLeaderEvent
	consumed := decoded.Decode(buf, sbe.HeaderSize)
	if consumed != n-sbe.HeaderSize {
		t.Fatalf("consumed %d, expected %d", consumed, n-sbe.HeaderSize)
	}
	if decoded.LeaderMemberId != 1 {
		t.Fatalf("LeaderMemberId: expected 1, got %d", decoded.LeaderMemberId)
	}
	if decoded.IngressEndpoints != "0=localhost:10000,1=localhost:10001,2=localhost:10002" {
		t.Fatalf("IngressEndpoints mismatch: got '%s'", decoded.IngressEndpoints)
	}
}

func TestChallengeRoundTrip(t *testing.T) {
	buf := make([]byte, 256)
	challengeData := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	msg := Challenge{
		ClusterSessionId: 33,
		CorrelationId:    44,
		LeadershipTermId: 5,
		ChallengeData:    challengeData,
	}
	n := msg.Encode(buf, 0)

	var decoded Challenge
	consumed := decoded.Decode(buf, sbe.HeaderSize)
	if consumed != n-sbe.HeaderSize {
		t.Fatalf("consumed %d, expected %d", consumed, n-sbe.HeaderSize)
	}
	if decoded.ClusterSessionId != 33 {
		t.Fatalf("ClusterSessionId: expected 33, got %d", decoded.ClusterSessionId)
	}
	if !bytes.Equal(decoded.ChallengeData, challengeData) {
		t.Fatalf("ChallengeData mismatch: got %v", decoded.ChallengeData)
	}
}

func TestChallengeResponseRoundTrip(t *testing.T) {
	buf := make([]byte, 256)
	respData := []byte{0xCA, 0xFE, 0xBA, 0xBE}
	msg := ChallengeResponse{
		CorrelationId:    77,
		ClusterSessionId: 33,
		ChallengeData:    respData,
	}
	n := msg.Encode(buf, 0)

	var decoded ChallengeResponse
	consumed := decoded.Decode(buf, sbe.HeaderSize)
	if consumed != n-sbe.HeaderSize {
		t.Fatalf("consumed %d, expected %d", consumed, n-sbe.HeaderSize)
	}
	if decoded.CorrelationId != 77 {
		t.Fatalf("CorrelationId: expected 77, got %d", decoded.CorrelationId)
	}
	if !bytes.Equal(decoded.ChallengeData, respData) {
		t.Fatalf("ChallengeData mismatch: got %v", decoded.ChallengeData)
	}
}

func TestMessageHeaderDispatch(t *testing.T) {
	// Test that we can dispatch on template ID
	buf := make([]byte, 256)

	templates := []struct {
		name     string
		expected uint16
		encode   func()
	}{
		{"SessionMessageHeader", TemplateIdSessionMessageHeader, func() {
			m := SessionMessageHeader{LeadershipTermId: 1, ClusterSessionId: 2, Timestamp: 3}
			m.Encode(buf, 0)
		}},
		{"SessionEvent", TemplateIdSessionEvent, func() {
			m := SessionEvent{Code: EventCodeOK}
			m.Encode(buf, 0)
		}},
		{"SessionConnectRequest", TemplateIdSessionConnectRequest, func() {
			m := SessionConnectRequest{CorrelationId: 1, ResponseChannel: "x"}
			m.Encode(buf, 0)
		}},
		{"SessionCloseRequest", TemplateIdSessionCloseRequest, func() {
			m := SessionCloseRequest{ClusterSessionId: 1}
			m.Encode(buf, 0)
		}},
		{"SessionKeepAlive", TemplateIdSessionKeepAlive, func() {
			m := SessionKeepAlive{LeadershipTermId: 1}
			m.Encode(buf, 0)
		}},
		{"NewLeaderEvent", TemplateIdNewLeaderEvent, func() {
			m := NewLeaderEvent{LeaderMemberId: 1, IngressEndpoints: "x"}
			m.Encode(buf, 0)
		}},
	}

	for _, tc := range templates {
		t.Run(tc.name, func(t *testing.T) {
			tc.encode()
			var hdr sbe.MessageHeader
			hdr.Decode(buf, 0)
			if hdr.TemplateId != tc.expected {
				t.Fatalf("expected template %d, got %d", tc.expected, hdr.TemplateId)
			}
			if hdr.SchemaId != SchemaId {
				t.Fatalf("expected schema %d, got %d", SchemaId, hdr.SchemaId)
			}
		})
	}
}

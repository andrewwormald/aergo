package sbe

import "testing"

func TestPutGetUint16(t *testing.T) {
	buf := make([]byte, 16)
	PutUint16(buf, 0, 0xCAFE)
	if got := GetUint16(buf, 0); got != 0xCAFE {
		t.Fatalf("expected 0xCAFE, got 0x%X", got)
	}
}

func TestPutGetInt32(t *testing.T) {
	buf := make([]byte, 16)
	PutInt32(buf, 4, -12345)
	if got := GetInt32(buf, 4); got != -12345 {
		t.Fatalf("expected -12345, got %d", got)
	}
}

func TestPutGetInt64(t *testing.T) {
	buf := make([]byte, 16)
	PutInt64(buf, 0, 0x7FFFFFFFFFFFFFFF)
	if got := GetInt64(buf, 0); got != 0x7FFFFFFFFFFFFFFF {
		t.Fatalf("expected max int64, got %d", got)
	}
}

func TestPutGetVarString(t *testing.T) {
	buf := make([]byte, 128)
	s := "aeron:udp?endpoint=localhost:0"
	n := PutVarString(buf, 10, s)
	if n != 4+len(s) {
		t.Fatalf("expected %d bytes, got %d", 4+len(s), n)
	}
	got, consumed := GetVarString(buf, 10)
	if got != s {
		t.Fatalf("expected %q, got %q", s, got)
	}
	if consumed != n {
		t.Fatalf("consumed %d, expected %d", consumed, n)
	}
}

func TestPutGetVarStringEmpty(t *testing.T) {
	buf := make([]byte, 16)
	n := PutVarString(buf, 0, "")
	if n != 4 {
		t.Fatalf("expected 4 bytes for empty string, got %d", n)
	}
	got, consumed := GetVarString(buf, 0)
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
	if consumed != 4 {
		t.Fatalf("consumed %d, expected 4", consumed)
	}
}

func TestMessageHeaderRoundTrip(t *testing.T) {
	buf := make([]byte, 16)
	hdr := MessageHeader{
		BlockLength: 24,
		TemplateId:  1,
		SchemaId:    111,
		Version:     8,
	}
	n := hdr.Encode(buf, 0)
	if n != HeaderSize {
		t.Fatalf("expected %d, got %d", HeaderSize, n)
	}

	var decoded MessageHeader
	decoded.Decode(buf, 0)
	if decoded.BlockLength != 24 {
		t.Fatalf("BlockLength: expected 24, got %d", decoded.BlockLength)
	}
	if decoded.TemplateId != 1 {
		t.Fatalf("TemplateId: expected 1, got %d", decoded.TemplateId)
	}
	if decoded.SchemaId != 111 {
		t.Fatalf("SchemaId: expected 111, got %d", decoded.SchemaId)
	}
	if decoded.Version != 8 {
		t.Fatalf("Version: expected 8, got %d", decoded.Version)
	}
}

func TestMessageHeaderAtOffset(t *testing.T) {
	buf := make([]byte, 32)
	hdr := MessageHeader{
		BlockLength: 16,
		TemplateId:  3,
		SchemaId:    111,
		Version:     8,
	}
	hdr.Encode(buf, 12)

	var decoded MessageHeader
	decoded.Decode(buf, 12)
	if decoded.TemplateId != 3 {
		t.Fatalf("TemplateId: expected 3, got %d", decoded.TemplateId)
	}
}

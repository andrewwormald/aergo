package cluster

import "testing"

func BenchmarkMessageHeaderEncode(b *testing.B) {
	buf := make([]byte, HeaderSize)
	h := MessageHeader{BlockLength: 24, TemplateId: 1, SchemaId: SchemaId, Version: SchemaVersion}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Encode(buf, 0)
	}
}

func BenchmarkMessageHeaderDecode(b *testing.B) {
	buf := make([]byte, HeaderSize)
	(&MessageHeader{BlockLength: 24, TemplateId: 1, SchemaId: SchemaId, Version: SchemaVersion}).Encode(buf, 0)
	var h MessageHeader
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Decode(buf, 0)
	}
}

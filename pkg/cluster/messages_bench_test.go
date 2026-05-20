package cluster

import "testing"

func BenchmarkSessionMessageHeaderEncode(b *testing.B) {
	buf := make([]byte, HeaderSize+sessionMessageHeaderBlockLength)
	m := SessionMessageHeader{LeadershipTermId: 1, ClusterSessionId: 42, Timestamp: 1_700_000_000}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Encode(buf, 0)
	}
}

func BenchmarkSessionMessageHeaderDecode(b *testing.B) {
	buf := make([]byte, HeaderSize+sessionMessageHeaderBlockLength)
	(&SessionMessageHeader{LeadershipTermId: 1, ClusterSessionId: 42, Timestamp: 1_700_000_000}).Encode(buf, 0)
	var m SessionMessageHeader
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.DecodeWithBlockLength(buf, HeaderSize, sessionMessageHeaderBlockLength)
	}
}

package aeron

import "testing"

func BenchmarkTermAppenderAppend(b *testing.B) {
	lb := newInMemLogBuffers(64 * 1024)
	app := NewTermAppender(lb, 0)
	payload := make([]byte, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if app.Append(0, 1, 1001, payload) < 0 {
			b.StopTimer()
			resetTermTail(lb, 0)
			zeroTerm(lb, 0, 64*1024)
			b.StartTimer()
		}
	}
}

func BenchmarkReadTerm(b *testing.B) {
	lb := newInMemLogBuffers(64 * 1024)
	app := NewTermAppender(lb, 0)
	payload := make([]byte, 32)
	const frames = 256
	for i := 0; i < frames; i++ {
		app.Append(0, 1, 1001, payload)
	}
	term := lb.Term(0)
	noop := func(buf *AtomicBuffer, offset, length int32, hdr *DataFrameHeader) {}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ReadTerm(term, 0, noop, frames)
	}
}

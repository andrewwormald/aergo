package aeron

import "testing"

func BenchmarkAtomicBufferPutInt32(b *testing.B) {
	buf := NewAtomicBuffer(make([]byte, 64))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.PutInt32(0, int32(i))
	}
}

func BenchmarkAtomicBufferPutInt64(b *testing.B) {
	buf := NewAtomicBuffer(make([]byte, 64))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.PutInt64(0, int64(i))
	}
}

func BenchmarkAtomicBufferPutBytes(b *testing.B) {
	buf := NewAtomicBuffer(make([]byte, 256))
	src := make([]byte, 24)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.PutBytes(0, src)
	}
}

func BenchmarkAtomicBufferPutInt32Ordered(b *testing.B) {
	buf := NewAtomicBuffer(make([]byte, 64))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.PutInt32Ordered(0, int32(i))
	}
}

func BenchmarkAtomicBufferPutInt64Ordered(b *testing.B) {
	buf := NewAtomicBuffer(make([]byte, 64))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.PutInt64Ordered(0, int64(i))
	}
}

func BenchmarkAtomicBufferGetAndAddInt64(b *testing.B) {
	buf := NewAtomicBuffer(make([]byte, 64))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.GetAndAddInt64(0, 1)
	}
}

func BenchmarkAtomicBufferCompareAndSetInt64(b *testing.B) {
	buf := NewAtomicBuffer(make([]byte, 64))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.CompareAndSetInt64(0, int64(i), int64(i+1))
	}
}

func BenchmarkAtomicBufferGetInt32Volatile(b *testing.B) {
	buf := NewAtomicBuffer(make([]byte, 64))
	buf.PutInt32(0, 42)
	var sink int32
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = buf.GetInt32Volatile(0)
	}
	_ = sink
}

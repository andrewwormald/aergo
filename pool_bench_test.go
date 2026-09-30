package aergo

import (
	"runtime"
	"sync"
	"testing"
)

func newPoolBuf() *[]byte { b := make([]byte, 0, 512); return &b }

// BenchmarkPoolGetPut compares the options for a pool reached from one
// goroutine: the stdlib cache, a buffered channel (which takes a lock),
// and the unsynchronised stack.
func BenchmarkPoolGetPut(b *testing.B) {
	b.Run("sync.Pool", func(b *testing.B) {
		p := sync.Pool{New: func() any { return newPoolBuf() }}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			p.Put(p.Get().(*[]byte))
		}
	})
	b.Run("channel", func(b *testing.B) {
		ch := make(chan *[]byte, 256)
		for i := 0; i < 256; i++ {
			ch <- newPoolBuf()
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var v *[]byte
			select {
			case v = <-ch:
			default:
				v = newPoolBuf()
			}
			select {
			case ch <- v:
			default:
			}
		}
	})
	b.Run("aergo.Pool", func(b *testing.B) {
		p := NewPool(256, newPoolBuf, nil)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			p.Put(p.Get())
		}
	})
}

// BenchmarkPoolAcrossGC is the case the type exists for: sync.Pool is
// cleared by the collector, so its allocations recur.
func BenchmarkPoolAcrossGC(b *testing.B) {
	const batch = 64
	b.Run("sync.Pool", func(b *testing.B) {
		p := sync.Pool{New: func() any { return newPoolBuf() }}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			runtime.GC()
			runtime.GC()
			for j := 0; j < batch; j++ {
				p.Put(p.Get().(*[]byte))
			}
		}
	})
	b.Run("aergo.Pool", func(b *testing.B) {
		p := NewPool(batch, newPoolBuf, nil)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			runtime.GC()
			runtime.GC()
			for j := 0; j < batch; j++ {
				p.Put(p.Get())
			}
		}
	})
}

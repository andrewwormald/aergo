//go:build !race

package aergo

import (
	"sync"
	"testing"
)

// This test reads data while the writer is overwriting it with newer values, on
// purpose: it checks only that a reader that sees a flag never sees data older
// than that flag. That is a statement about hardware ordering, which the race
// detector cannot judge (it would rightly report the overlap), so it does not
// run under -race.

// Message passing, the property a log commit depends on: if a reader sees the
// ordered store, it must also see every plain store made before it. On a weakly
// ordered CPU (arm64) a plain store in place of the release store fails this;
// on amd64 it holds because x86 never reorders stores with each other.
func TestOrderedStoreReleasesEarlierStores(t *testing.T) {
	const rounds = 200000
	buf := NewAtomicBuffer(make([]byte, 256))
	const flag, data1, data2 = 0, 64, 128 // separate cache lines

	var wg sync.WaitGroup
	wg.Add(2)
	done := make(chan struct{})
	go func() { // writer: data first, then publish with a release store
		defer wg.Done()
		for i := int32(1); i <= rounds; i++ {
			buf.PutInt64(data1, int64(i))
			buf.PutInt64(data2, int64(i))
			buf.PutInt32Ordered(flag, i)
		}
		close(done)
	}()
	var bad int
	go func() { // reader: an acquire load of the flag, then the data
		defer wg.Done()
		for {
			f := buf.GetInt32Volatile(flag)
			if f != 0 {
				if d1, d2 := buf.GetInt64(data1), buf.GetInt64(data2); d1 < int64(f) || d2 < int64(f) {
					bad++
				}
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	wg.Wait()
	if bad != 0 {
		t.Errorf("%d reads saw the flag before the data it publishes", bad)
	}
}

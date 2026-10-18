package aergo

import "testing"

// The release store is the one piece of platform-specific code (assembly on
// amd64, sync/atomic elsewhere), so this runs on every architecture and with
// each implementation (-tags aergo_noasm selects the pure-Go one on amd64).

func TestOrderedStoresRoundTrip(t *testing.T) {
	buf := NewAtomicBuffer(make([]byte, 64))
	for _, v := range []int32{0, 1, -1, 1 << 30, -(1 << 30)} {
		buf.PutInt32Ordered(8, v)
		if got := buf.GetInt32Volatile(8); got != v {
			t.Errorf("int32: stored %d, read %d", v, got)
		}
	}
	for _, v := range []int64{0, 1, -1, 1 << 62, -(1 << 62)} {
		buf.PutInt64Ordered(16, v)
		if got := buf.GetInt64Volatile(16); got != v {
			t.Errorf("int64: stored %d, read %d", v, got)
		}
	}
	// Neighbouring bytes are untouched.
	buf.PutInt64(0, 0x1111111111111111)
	buf.PutInt32Ordered(8, 7)
	buf.PutInt64(24, 0x2222222222222222)
	buf.PutInt64Ordered(16, 9)
	if buf.GetInt64(0) != 0x1111111111111111 || buf.GetInt64(24) != 0x2222222222222222 {
		t.Error("an ordered store wrote outside its own bytes")
	}
}

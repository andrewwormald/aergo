package native

import "testing"

func TestAtomicBufferBasicOps(t *testing.T) {
	data := make([]byte, 256)
	buf := NewAtomicBuffer(data)

	if buf.Capacity() != 256 {
		t.Fatalf("capacity: got %d, want 256", buf.Capacity())
	}

	buf.PutInt32(0, 42)
	if v := buf.GetInt32(0); v != 42 {
		t.Errorf("GetInt32: got %d, want 42", v)
	}

	buf.PutInt64(8, 123456789012345)
	if v := buf.GetInt64(8); v != 123456789012345 {
		t.Errorf("GetInt64: got %d, want 123456789012345", v)
	}

	buf.PutUint8(16, 0xFF)
	if v := buf.GetUint8(16); v != 0xFF {
		t.Errorf("GetUint8: got %d", v)
	}
}

func TestAtomicBufferVolatileOps(t *testing.T) {
	data := make([]byte, 64)
	buf := NewAtomicBuffer(data)

	buf.PutInt32Ordered(0, 99)
	if v := buf.GetInt32Volatile(0); v != 99 {
		t.Errorf("volatile int32: got %d, want 99", v)
	}

	buf.PutInt64Ordered(8, 12345)
	if v := buf.GetInt64Volatile(8); v != 12345 {
		t.Errorf("volatile int64: got %d, want 12345", v)
	}
}

func TestAtomicBufferCAS(t *testing.T) {
	data := make([]byte, 64)
	buf := NewAtomicBuffer(data)

	buf.PutInt64(0, 100)

	// Should fail: expected doesn't match
	if buf.CompareAndSetInt64(0, 99, 200) {
		t.Error("CAS should fail with wrong expected")
	}
	if v := buf.GetInt64(0); v != 100 {
		t.Errorf("value should be unchanged: got %d", v)
	}

	// Should succeed
	if !buf.CompareAndSetInt64(0, 100, 200) {
		t.Error("CAS should succeed")
	}
	if v := buf.GetInt64(0); v != 200 {
		t.Errorf("value should be 200: got %d", v)
	}
}

func TestAtomicBufferGetAndAdd(t *testing.T) {
	data := make([]byte, 64)
	buf := NewAtomicBuffer(data)

	buf.PutInt64(0, 10)
	prev := buf.GetAndAddInt64(0, 5)
	if prev != 10 {
		t.Errorf("prev: got %d, want 10", prev)
	}
	if v := buf.GetInt64(0); v != 15 {
		t.Errorf("new value: got %d, want 15", v)
	}
}

func TestAtomicBufferBytes(t *testing.T) {
	data := make([]byte, 64)
	buf := NewAtomicBuffer(data)

	src := []byte("hello world")
	buf.PutBytes(10, src)

	dst := make([]byte, len(src))
	buf.GetBytes(10, dst)
	if string(dst) != "hello world" {
		t.Errorf("got %q, want %q", dst, src)
	}
}

func TestAtomicBufferSlice(t *testing.T) {
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	buf := NewAtomicBuffer(data)

	s := buf.Slice(2, 4)
	if len(s) != 4 || s[0] != 2 || s[3] != 5 {
		t.Errorf("slice: got %v", s)
	}
}

func TestAtomicBufferFill(t *testing.T) {
	data := make([]byte, 16)
	buf := NewAtomicBuffer(data)
	buf.Fill(0xFF)
	for i, b := range data {
		if b != 0xFF {
			t.Errorf("data[%d] = %d, want 0xFF", i, b)
		}
	}
}

func TestNewAtomicBufferEmpty(t *testing.T) {
	buf := NewAtomicBuffer(nil)
	if buf.Capacity() != 0 {
		t.Errorf("empty capacity: got %d", buf.Capacity())
	}
}

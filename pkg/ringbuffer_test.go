package aeron

import "testing"

func newTestRingBuffer(capacity int) *ManyToOneRingBuffer {
	totalLen := capacity + rbTrailerLength
	data := make([]byte, totalLen)
	buf := NewAtomicBuffer(data)
	rb, err := NewManyToOneRingBuffer(buf)
	if err != nil {
		panic(err)
	}
	return rb
}

func TestRingBufferWrite(t *testing.T) {
	rb := newTestRingBuffer(1024)

	msg := []byte("hello")
	if !rb.Write(1, msg) {
		t.Fatal("write failed")
	}
}

func TestRingBufferWriteMultiple(t *testing.T) {
	rb := newTestRingBuffer(1024)

	for i := 0; i < 20; i++ {
		msg := []byte("test message payload")
		if !rb.Write(int32(i+1), msg) {
			t.Fatalf("write %d failed", i)
		}
	}
}

func TestRingBufferWriteFull(t *testing.T) {
	rb := newTestRingBuffer(64) // tiny buffer

	// Fill with a large message
	msg := make([]byte, 48) // 48 + 8 header = 56, aligned to 56. Capacity = 64.
	if !rb.Write(1, msg) {
		t.Fatal("first write should succeed")
	}

	// Second write should fail (no space, no consumer draining)
	if rb.Write(2, msg) {
		t.Fatal("second write should fail (buffer full)")
	}
}

func TestRingBufferCorrelationID(t *testing.T) {
	rb := newTestRingBuffer(1024)

	id1 := rb.NextCorrelationID()
	id2 := rb.NextCorrelationID()
	id3 := rb.NextCorrelationID()

	if id2 != id1+1 || id3 != id2+1 {
		t.Errorf("IDs not sequential: %d, %d, %d", id1, id2, id3)
	}
}

func TestRingBufferInvalidCapacity(t *testing.T) {
	// Not power of two
	data := make([]byte, 100+rbTrailerLength)
	buf := NewAtomicBuffer(data)
	_, err := NewManyToOneRingBuffer(buf)
	if err == nil {
		t.Error("expected error for non-power-of-two capacity")
	}
}

func TestIsPowerOfTwo(t *testing.T) {
	tests := []struct {
		n    int
		want bool
	}{
		{0, false}, {1, true}, {2, true}, {3, false},
		{4, true}, {64, true}, {1024, true}, {1023, false},
	}
	for _, tt := range tests {
		if got := isPowerOfTwo(tt.n); got != tt.want {
			t.Errorf("isPowerOfTwo(%d) = %v, want %v", tt.n, got, tt.want)
		}
	}
}

func TestAlign(t *testing.T) {
	tests := []struct {
		value, alignment, want int32
	}{
		{0, 8, 0}, {1, 8, 8}, {8, 8, 8}, {9, 8, 16},
		{32, 32, 32}, {33, 32, 64},
	}
	for _, tt := range tests {
		if got := align(tt.value, tt.alignment); got != tt.want {
			t.Errorf("align(%d, %d) = %d, want %d", tt.value, tt.alignment, got, tt.want)
		}
	}
}

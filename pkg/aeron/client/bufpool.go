package client

// BufferPool provides pre-allocated byte buffers in a fixed-size ring.
// Not thread-safe -- use one pool per goroutine.
type BufferPool struct {
	buffers [][]byte
	size    int
	idx     int
}

// NewBufferPool creates a pool of count buffers, each of the given size.
func NewBufferPool(count, size int) *BufferPool {
	buffers := make([][]byte, count)
	for i := range buffers {
		buffers[i] = make([]byte, size)
	}
	return &BufferPool{
		buffers: buffers,
		size:    size,
	}
}

// Get returns the next buffer from the ring. The caller must not retain
// the buffer past the next call to Get -- it will be reused.
func (p *BufferPool) Get() []byte {
	buf := p.buffers[p.idx]
	p.idx = (p.idx + 1) % len(p.buffers)
	return buf
}

// Size returns the individual buffer size.
func (p *BufferPool) Size() int {
	return p.size
}

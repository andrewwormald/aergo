// Package native implements the Aeron media driver shared memory protocol
// in pure Go, eliminating the need for the C client library (libaeron).
//
// It communicates with the Aeron media driver (aeronmd) via memory-mapped
// files: the CnC file for commands/responses and log buffer files for
// publication/subscription data.
package native

import (
	"sync/atomic"
	"unsafe"
)

// CacheLineLength is the CPU cache line size used for padding in Aeron data structures.
const CacheLineLength = 64

// AtomicBuffer wraps a region of memory and provides atomic operations.
// The backing memory is typically mmap'd from the Aeron media driver's shared files.
type AtomicBuffer struct {
	ptr    unsafe.Pointer
	length int32
}

// NewAtomicBuffer creates a buffer backed by a byte slice.
func NewAtomicBuffer(data []byte) *AtomicBuffer {
	if len(data) == 0 {
		return &AtomicBuffer{}
	}
	return &AtomicBuffer{
		ptr:    unsafe.Pointer(&data[0]),
		length: int32(len(data)),
	}
}

// WrapPtr creates a buffer backed by raw memory.
func WrapPtr(ptr unsafe.Pointer, length int32) *AtomicBuffer {
	return &AtomicBuffer{ptr: ptr, length: length}
}

// Ptr returns the raw pointer to the buffer.
func (b *AtomicBuffer) Ptr() unsafe.Pointer { return b.ptr }

// Capacity returns the buffer length in bytes.
func (b *AtomicBuffer) Capacity() int32 { return b.length }

func (b *AtomicBuffer) ptrAt(offset int32) unsafe.Pointer {
	return unsafe.Add(b.ptr, uintptr(offset))
}

// --- Non-atomic reads ---

func (b *AtomicBuffer) GetInt32(offset int32) int32 {
	return *(*int32)(b.ptrAt(offset))
}

func (b *AtomicBuffer) GetInt64(offset int32) int64 {
	return *(*int64)(b.ptrAt(offset))
}

func (b *AtomicBuffer) GetUint8(offset int32) uint8 {
	return *(*uint8)(b.ptrAt(offset))
}

func (b *AtomicBuffer) GetBytes(offset int32, dst []byte) {
	src := unsafe.Slice((*byte)(b.ptrAt(offset)), len(dst))
	copy(dst, src)
}

// --- Non-atomic writes ---

func (b *AtomicBuffer) PutInt32(offset int32, value int32) {
	*(*int32)(b.ptrAt(offset)) = value
}

func (b *AtomicBuffer) PutInt64(offset int32, value int64) {
	*(*int64)(b.ptrAt(offset)) = value
}

func (b *AtomicBuffer) PutUint8(offset int32, value uint8) {
	*(*uint8)(b.ptrAt(offset)) = value
}

func (b *AtomicBuffer) PutBytes(offset int32, src []byte) {
	dst := unsafe.Slice((*byte)(b.ptrAt(offset)), len(src))
	copy(dst, src)
}

// --- Atomic (volatile) reads ---

func (b *AtomicBuffer) GetInt32Volatile(offset int32) int32 {
	return atomic.LoadInt32((*int32)(b.ptrAt(offset)))
}

func (b *AtomicBuffer) GetInt64Volatile(offset int32) int64 {
	return atomic.LoadInt64((*int64)(b.ptrAt(offset)))
}

// --- Atomic (ordered) writes ---

func (b *AtomicBuffer) PutInt32Ordered(offset int32, value int32) {
	atomic.StoreInt32((*int32)(b.ptrAt(offset)), value)
}

func (b *AtomicBuffer) PutInt64Ordered(offset int32, value int64) {
	atomic.StoreInt64((*int64)(b.ptrAt(offset)), value)
}

// --- CAS ---

func (b *AtomicBuffer) CompareAndSetInt64(offset int32, expected, update int64) bool {
	return atomic.CompareAndSwapInt64((*int64)(b.ptrAt(offset)), expected, update)
}

func (b *AtomicBuffer) CompareAndSetInt32(offset int32, expected, update int32) bool {
	return atomic.CompareAndSwapInt32((*int32)(b.ptrAt(offset)), expected, update)
}

// --- Fetch-and-Add ---

func (b *AtomicBuffer) GetAndAddInt64(offset int32, delta int64) int64 {
	return atomic.AddInt64((*int64)(b.ptrAt(offset)), delta) - delta
}

// --- Slice view ---

// Slice returns a Go byte slice view of the buffer region. The slice shares
// the underlying memory -- do not hold references past the buffer's lifetime.
func (b *AtomicBuffer) Slice(offset, length int32) []byte {
	return unsafe.Slice((*byte)(b.ptrAt(offset)), length)
}

// Fill sets all bytes in the buffer to the given value.
func (b *AtomicBuffer) Fill(value byte) {
	s := unsafe.Slice((*byte)(b.ptr), b.length)
	for i := range s {
		s[i] = value
	}
}

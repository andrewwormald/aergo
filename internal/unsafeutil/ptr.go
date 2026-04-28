package unsafeutil

import "unsafe"

// BytesToPointer returns an unsafe.Pointer to the first byte of the slice.
// The caller must ensure the slice is not garbage collected while the pointer is in use.
func BytesToPointer(b []byte) unsafe.Pointer {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Pointer(&b[0])
}

// PointerToBytes creates a Go byte slice from a C pointer and length.
// The returned slice shares memory with the C pointer -- do not retain
// past the lifetime of the C buffer.
func PointerToBytes(ptr unsafe.Pointer, length int) []byte {
	if ptr == nil || length == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(ptr), length)
}

package client

import "unsafe"

// uintptrToClientd converts a uintptr ID to an unsafe.Pointer for use as
// a C clientd argument. This is not a real Go pointer -- it's an opaque
// integer value passed through C and back to us in callbacks.
//
//go:nosplit
//go:nocheckptr
func uintptrToClientd(id uintptr) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&id))
}

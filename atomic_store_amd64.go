//go:build amd64 && !race

package aergo

// storeRelease32 and storeRelease64 store with release semantics, as Java's
// putIntOrdered and putLongOrdered do: all earlier stores are visible before
// this one. p must be aligned to the size of the value.
//
//go:noescape
func storeRelease32(p *int32, v int32)

//go:noescape
func storeRelease64(p *int64, v int64)

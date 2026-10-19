//go:build !amd64 || race || aergo_noasm

package aergo

import "sync/atomic"

// Everywhere except amd64 the release store is sync/atomic's, on purpose:
//
//   - arm64: sync/atomic compiles to a single inlined STLR (STLRW), a release
//     store with no barrier. That is what Java's putOrdered emits on arm64, so
//     there is nothing to win with assembly; a hand-written routine could not
//     be inlined and would only add a call.
//   - -race builds: the stores must be ones the race detector can see, or it
//     reports the publisher/subscriber handoff as a race.
//   - -tags aergo_noasm forces this path on amd64, to test and benchmark the
//     pure-Go implementation (on amd64 sync/atomic is an XCHG).

func storeRelease32(p *int32, v int32) { atomic.StoreInt32(p, v) }
func storeRelease64(p *int64, v int64) { atomic.StoreInt64(p, v) }

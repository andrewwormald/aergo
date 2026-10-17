//go:build !amd64 || race

package aergo

import "sync/atomic"

// On other architectures sync/atomic stores are already release stores or
// cheap enough (arm64 uses STLR). Under the race detector the stores must be
// ones it can see, or it reports the publisher/subscriber handoff as a race.

func storeRelease32(p *int32, v int32) { atomic.StoreInt32(p, v) }
func storeRelease64(p *int64, v int64) { atomic.StoreInt64(p, v) }

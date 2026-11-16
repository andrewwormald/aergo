//go:build !linux

package aergo

import "time"

// PreciseSleep sleeps for d. Only Linux has a sleep more precise than time.Sleep
// here: the rounding up to 1ms that it avoids is Go's epoll wait on Linux, so
// elsewhere this is time.Sleep. See the Linux version.
func PreciseSleep(d time.Duration) { time.Sleep(d) }

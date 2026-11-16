//go:build !linux

package aergo

import "time"

// PreciseSleep sleeps for d. Only Linux has a sleep more precise than time.Sleep
// here: the epoll rounding it avoids (see BackoffIdleStrategy.WithSleep) is
// Linux's, so elsewhere this is time.Sleep.
func PreciseSleep(d time.Duration) { time.Sleep(d) }

package aergo

import (
	"syscall"
	"time"
)

// PreciseSleep sleeps for d, or a little over. Unlike time.Sleep it does not
// round a sleep shorter than 1ms up to 1ms (see BackoffIdleStrategy.WithSleep).
// In a Linux VM a request for 64µs took about 150µs and one for 512µs about
// 700µs, against 1.2ms for both with time.Sleep.
//
// It blocks the operating system thread for the length of the sleep, where
// time.Sleep parks only the goroutine, so use it for the one goroutine that polls
// for frames, and not for many goroutines at once. It is Java's
// LockSupport.parkNanos.
func PreciseSleep(d time.Duration) {
	if d <= 0 {
		return
	}
	ts := syscall.NsecToTimespec(int64(d))
	for {
		var left syscall.Timespec
		if err := syscall.Nanosleep(&ts, &left); err != syscall.EINTR {
			return
		}
		// A signal, such as the runtime's preemption, cut the sleep short.
		ts = left
	}
}

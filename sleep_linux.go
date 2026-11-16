package aergo

import (
	"syscall"
	"time"
)

// PreciseSleep sleeps for d, or a little over. In a Linux VM a request for 64µs
// took about 150µs and one for 512µs about 700µs.
//
// time.Sleep is the wrong sleep for a loop that is waiting for a frame. When no
// other goroutine is runnable, Go's idle thread waits in epoll_wait, whose timeout
// is in whole milliseconds, and the runtime rounds any wait shorter than 1ms up to
// 1ms. A time.Sleep of 64µs takes about 1.2ms, so a frame that arrives while the
// loop is parked waits that long to be seen.
//
// PreciseSleep blocks the operating system thread for the length of the sleep,
// where time.Sleep parks only the goroutine, so many goroutines sleeping at once
// hold many threads. It is Java's LockSupport.parkNanos.
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

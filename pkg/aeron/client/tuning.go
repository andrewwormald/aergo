package client

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
)

// TuningProfile configures runtime parameters for low-latency operation.
type TuningProfile struct {
	// DisableGC disables the Go garbage collector entirely.
	// Call runtime.GC() manually during idle periods.
	DisableGC bool

	// GOMAXPROCS sets the maximum number of OS threads for Go scheduling.
	// Set to runtime.NumCPU() for maximum throughput, or a lower value
	// to leave cores free for the Aeron media driver.
	GOMAXPROCS int

	// MemoryLimit sets the soft memory limit for the runtime (Go 1.19+).
	// 0 = no limit.
	MemoryLimit int64
}

// LowLatencyProfile returns a tuning profile optimized for latency.
func LowLatencyProfile() TuningProfile {
	return TuningProfile{
		DisableGC:  true,
		GOMAXPROCS: runtime.NumCPU(),
	}
}

// Apply configures the Go runtime with this tuning profile.
// Returns a function that restores the previous settings.
func (t TuningProfile) Apply() func() {
	var oldGCPercent int
	var oldMaxProcs int

	if t.DisableGC {
		oldGCPercent = debug.SetGCPercent(-1)
	}

	if t.GOMAXPROCS > 0 {
		oldMaxProcs = runtime.GOMAXPROCS(t.GOMAXPROCS)
	}

	if t.MemoryLimit > 0 {
		debug.SetMemoryLimit(t.MemoryLimit)
	}

	return func() {
		if t.DisableGC {
			debug.SetGCPercent(oldGCPercent)
		}
		if t.GOMAXPROCS > 0 {
			runtime.GOMAXPROCS(oldMaxProcs)
		}
	}
}

// PrintAffinityHint prints a hint for CPU affinity configuration.
// CPU affinity must be set externally (taskset/cpuset) -- Go has no native support.
func PrintAffinityHint(pollCore int) {
	fmt.Fprintf(os.Stderr, `aergo: CPU affinity hint
  For lowest latency, pin the poll goroutine to an isolated core:

  Linux:
    # Isolate cores 2-3 from kernel scheduler (add to /etc/default/grub):
    GRUB_CMDLINE_LINUX="isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3"

    # Run aergo pinned to core %d:
    taskset -c %d ./aergo

    # Run aeronmd pinned to core %d:
    taskset -c %d ./aeronmd

  macOS:
    # macOS does not support CPU pinning. Use thread priority instead.
    # runtime.LockOSThread() is already applied by the poll loop.

`, pollCore, pollCore, pollCore+1, pollCore+1)
}

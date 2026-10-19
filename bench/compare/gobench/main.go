//go:build linux

// Go side of the Java vs Go Aeron client comparison. Same methodology as
// bench/compare/java/Bench.java; keep the two in step.
//
// Both attach to an already running media driver, use aeron:ipc, and put the
// subscriber behind a FragmentAssembler so a message is counted once however
// many frames it took.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/andrewwormald/aergo"
)

const (
	channel       = "aeron:ipc"
	pingStream    = 1001
	pongStream    = 1002
	fragmentLimit = 10
)

func main() {
	dir := flag.String("dir", "/dev/shm/aeron-bench", "aeron directory")
	mode := flag.String("mode", "all", "all|throughput|latency")
	sizesArg := flag.String("sizes", "32,256,1024,4000", "message sizes")
	count := flag.Int("count", 2000000, "messages per throughput run")
	repeats := flag.Int("repeats", 5, "measured throughput runs per size")
	latCount := flag.Int("latcount", 200000, "latency samples")
	latWarmup := flag.Int("latwarmup", 50000, "latency warm-up round trips")
	exclusive := flag.Bool("exclusive", false, "use exclusive publications (single writer), as the cluster client does")
	idle := flag.String("idle", "spin", "throughput consumer idle strategy: spin or backoff (Agrona defaults)")
	pin := flag.String("pin", "", "CPUs for the two busy threads: producer,consumer (pinger,ponger)")
	conductorMs := flag.Int("conductor-ms", 16, "DoWork interval; Java's client conductor sleeps 16 ms between duty cycles")
	conductor := flag.Bool("conductor", true, "run DoWork on a background goroutine")
	cpuprofile := flag.String("cpuprofile", "", "write a CPU profile to this file")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		check(err)
		check(pprof.StartCPUProfile(f))
		defer pprof.StopCPUProfile()
	}

	parsePin(*pin)
	useExclusive = *exclusive
	conductorInterval = time.Duration(*conductorMs) * time.Millisecond
	consumerIdle = *idle

	var sizes []int
	for _, s := range strings.Split(*sizesArg, ",") {
		n, err := strconv.Atoi(s)
		check(err)
		sizes = append(sizes, n)
	}

	client, err := aergo.Connect(aergo.WithDir(*dir))
	check(err)
	defer client.Close()
	if *conductor {
		defer startConductor(client)()
	}

	if *mode == "all" || *mode == "throughput" {
		for _, size := range sizes {
			throughput(client, size, *count, *repeats)
		}
	}
	if *mode == "burst" {
		for _, size := range sizes {
			burst(client, size, *repeats)
		}
	}
	if *mode == "all" || *mode == "latency" {
		for _, size := range sizes {
			latency(client, size, *latCount, *latWarmup)
		}
	}
}

// startConductor runs the client's DoWork in the background, standing in for
// the conductor thread Java's client runs on its own, which sleeps 16 ms
// between duty cycles by default. The Go client has none: DoWork must be called
// to answer the driver and send keepalives.
func startConductor(client *aergo.Aeron) (stop func()) {
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(conductorInterval)
		defer t.Stop()
		for {
			select {
			case <-quit:
				return
			case <-t.C:
				client.DoWork()
			}
		}
	}()
	return func() { close(quit); <-done }
}

var (
	useExclusive             bool
	conductorInterval        = 16 * time.Millisecond
	pinProducer, pinConsumer = -1, -1
	consumerIdle             = "spin"
)

func parsePin(arg string) {
	if arg == "" {
		return
	}
	parts := strings.Split(arg, ",")
	if len(parts) != 2 {
		check(fmt.Errorf("-pin wants producer,consumer"))
	}
	var err error
	pinProducer, err = strconv.Atoi(parts[0])
	check(err)
	pinConsumer, err = strconv.Atoi(parts[1])
	check(err)
}

// pinThread restricts the calling OS thread to one CPU. The goroutine must be
// locked to its thread and must not return it to the scheduler pinned, so the
// busy goroutines stay locked until they exit (which ends the thread).
func pinThread(cpu int) {
	if cpu < 0 {
		return
	}
	var mask [16]uint64 // up to 1024 CPUs
	mask[cpu/64] |= 1 << (cpu % 64)
	if _, _, errno := syscall.RawSyscall(syscall.SYS_SCHED_SETAFFINITY, 0, uintptr(len(mask)*8), uintptr(unsafe.Pointer(&mask[0]))); errno != 0 {
		check(errno)
	}
}

// addPublication adds a shared publication, or an exclusive one with -exclusive.
func addPublication(client *aergo.Aeron, stream int32) (*aergo.Publication, error) {
	if useExclusive {
		return client.AddExclusivePublication(channel, stream)
	}
	return client.AddPublication(channel, stream)
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func await(pub *aergo.Publication) {
	deadline := time.Now().Add(10 * time.Second)
	for !pub.IsConnected() {
		if time.Now().After(deadline) {
			check(fmt.Errorf("publication not connected"))
		}
		time.Sleep(time.Millisecond)
	}
}

func offer(pub *aergo.Publication, buf []byte) {
	for {
		r, err := pub.Offer(buf)
		check(err)
		if r > 0 {
			return
		}
		if r == aergo.Closed || r == aergo.MaxPositionExceeded {
			check(fmt.Errorf("offer failed: %d", r))
		}
	}
}

// throughput: one publisher, one polling goroutine; time from first offer
// until every message is received.
func throughput(client *aergo.Aeron, size, count, repeats int) {
	pub, err := addPublication(client, pingStream)
	check(err)
	defer pub.Close()
	sub, err := client.AddSubscription(channel, pingStream)
	check(err)
	defer sub.Close()
	await(pub)

	src := make([]byte, size)
	for run := 0; run <= repeats; run++ { // run 0 is a discarded warm-up
		var received atomic.Int64
		var local int64
		asm := aergo.NewFragmentAssembler(func([]byte, *aergo.Header) { local++ })
		done := make(chan struct{})
		go func() {
			runtime.LockOSThread()
			pinThread(pinConsumer)
			var idler aergo.IdleStrategy = aergo.NoOpIdleStrategy{}
			if consumerIdle == "backoff" {
				idler = aergo.NewBackoffIdleStrategy()
			}
			for local < int64(count) {
				idler.Idle(sub.Poll(asm.OnFragment, fragmentLimit))
			}
			received.Store(local)
			close(done)
		}()

		runtime.LockOSThread()
		pinThread(pinProducer)
		t0 := time.Now()
		for i := 0; i < count; i++ {
			offer(pub, src)
		}
		<-done
		secs := time.Since(t0).Seconds()

		if run > 0 {
			fmt.Printf("RESULT lang=go mode=throughput size=%d run=%d msgs=%d msgs_per_sec=%.0f mb_per_sec=%.1f\n",
				size, run, count, float64(count)/secs, float64(count)*float64(size)/secs/1e6)
		}
	}
}

// latency: closed-loop ping-pong, one message outstanding; round-trip time in ns.
func latency(client *aergo.Aeron, size, count, warmup int) {
	ping, err := addPublication(client, pingStream)
	check(err)
	defer ping.Close()
	pingSub, err := client.AddSubscription(channel, pingStream)
	check(err)
	defer pingSub.Close()
	pong, err := addPublication(client, pongStream)
	check(err)
	defer pong.Close()
	pongSub, err := client.AddSubscription(channel, pongStream)
	check(err)
	defer pongSub.Close()
	await(ping)
	await(pong)

	src := make([]byte, size)
	var stop atomic.Bool
	echo := aergo.NewFragmentAssembler(func(b []byte, _ *aergo.Header) { offer(pong, b) })
	ponged := make(chan struct{})
	var pongerSwitches int64
	go func() {
		runtime.LockOSThread()
		pinThread(pinConsumer)
		var r0, r1 syscall.Rusage
		_ = syscall.Getrusage(1, &r0) // RUSAGE_THREAD
		for !stop.Load() {
			pingSub.Poll(echo.OnFragment, fragmentLimit)
		}
		_ = syscall.Getrusage(1, &r1)
		pongerSwitches = r1.Nivcsw - r0.Nivcsw
		close(ponged)
	}()

	runtime.LockOSThread()
	pinThread(pinProducer)
	var ru0 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru0)
	var th0, th1 syscall.Rusage
	_ = syscall.Getrusage(1, &th0) // RUSAGE_THREAD: the pinger
	got := 0
	reply := aergo.NewFragmentAssembler(func([]byte, *aergo.Header) { got++ })
	samples := make([]int64, count)
	starts := make([]int64, count)
	base := time.Now()
	for i := 0; i < warmup+count; i++ {
		got = 0
		// time.Since(base) is a single monotonic clock read, as System.nanoTime
		// is; time.Now() reads the wall clock as well and costs about twice as much.
		t0 := int64(time.Since(base))
		offer(ping, src)
		for got == 0 {
			pongSub.Poll(reply.OnFragment, fragmentLimit)
		}
		rtt := int64(time.Since(base)) - t0
		if i >= warmup {
			samples[i-warmup] = rtt
			starts[i-warmup] = t0
		}
	}
	stop.Store(true)
	<-ponged

	var ru1 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru1)
	_ = syscall.Getrusage(1, &th1)
	if os.Getenv("TAIL") != "" {
		<-ponged
		fmt.Fprintf(os.Stderr, "SWITCHES involuntary: pinger %d, ponger %d\n", th1.Nivcsw-th0.Nivcsw, pongerSwitches)
		reportSlow(samples, starts)
	}
	sort.Slice(samples, func(a, b int) bool { return samples[a] < samples[b] })
	pct := func(p float64) int64 {
		idx := int(p*float64(len(samples))+0.999999999) - 1
		if idx >= len(samples) {
			idx = len(samples) - 1
		}
		return samples[idx]
	}
	fmt.Printf("RESULT lang=go mode=latency size=%d samples=%d p50_ns=%d p90_ns=%d p99_ns=%d p999_ns=%d max_ns=%d minflt=%d\n",
		size, count, pct(0.50), pct(0.90), pct(0.99), pct(0.999), samples[len(samples)-1], ru1.Minflt-ru0.Minflt)
}

// burst isolates the two halves of the pipeline: it times a publisher filling
// the log with no consumer running (a burst small enough to stay inside the
// flow-control window), then a consumer on another CPU draining it with no
// publisher running.
func burst(client *aergo.Aeron, size, repeats int) {
	pub, err := addPublication(client, pingStream)
	check(err)
	defer pub.Close()
	sub, err := client.AddSubscription(channel, pingStream)
	check(err)
	defer sub.Close()
	await(pub)

	n := 16 * 1024 * 1024 / (size + 32)
	src := make([]byte, size)
	runtime.LockOSThread()
	pinThread(pinProducer)
	for run := 0; run <= repeats; run++ { // run 0 is a discarded warm-up
		var ru0, ru1 syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru0)
		t0 := time.Now()
		for i := 0; i < n; i++ {
			offer(pub, src)
		}
		pubSecs := time.Since(t0).Seconds()
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru1)

		var drainSecs float64
		done := make(chan struct{})
		go func() {
			runtime.LockOSThread()
			pinThread(pinConsumer)
			got := 0
			asm := aergo.NewFragmentAssembler(func([]byte, *aergo.Header) { got++ })
			t1 := time.Now()
			for got < n {
				sub.Poll(asm.OnFragment, fragmentLimit)
			}
			drainSecs = time.Since(t1).Seconds()
			close(done)
		}()
		<-done

		if run > 0 {
			fmt.Printf("RESULT lang=go mode=burst size=%d run=%d msgs=%d publish_msgs_per_sec=%.0f drain_msgs_per_sec=%.0f publish_minflt=%d\n",
				size, run, n, float64(n)/pubSecs, float64(n)/drainSecs, ru1.Minflt-ru0.Minflt)
		}
	}
}

// reportSlow groups round trips slower than 20us that start within 5ms of each
// other, to tell a few long stalls from steady background noise.
func reportSlow(samples, starts []int64) {
	type burst struct {
		startMs float64
		n       int
		worstUs float64
		totalUs float64
	}
	var bursts []burst
	var slow int
	last := int64(-1 << 62)
	for i, v := range samples {
		if v < 20_000 {
			continue
		}
		slow++
		if starts[i]-last > 5_000_000 {
			bursts = append(bursts, burst{startMs: float64(starts[i]) / 1e6})
		}
		b := &bursts[len(bursts)-1]
		b.n++
		b.totalUs += float64(v) / 1e3
		if float64(v)/1e3 > b.worstUs {
			b.worstUs = float64(v) / 1e3
		}
		last = starts[i]
	}
	fmt.Fprintf(os.Stderr, "SLOW size samples>20us: %d of %d in %d clusters\n", slow, len(samples), len(bursts))
	for i, b := range bursts {
		if i >= 12 {
			fmt.Fprintf(os.Stderr, "  ... %d more\n", len(bursts)-i)
			break
		}
		fmt.Fprintf(os.Stderr, "  at %8.1f ms: %5d slow round trips, worst %8.0f us, together %7.1f ms\n", b.startMs, b.n, b.worstUs, b.totalUs/1000)
	}
}

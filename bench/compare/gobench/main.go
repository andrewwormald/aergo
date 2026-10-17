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
	"time"

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
	cpuprofile := flag.String("cpuprofile", "", "write a CPU profile to this file")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		check(err)
		check(pprof.StartCPUProfile(f))
		defer pprof.StopCPUProfile()
	}

	var sizes []int
	for _, s := range strings.Split(*sizesArg, ",") {
		n, err := strconv.Atoi(s)
		check(err)
		sizes = append(sizes, n)
	}

	client, err := aergo.Connect(aergo.WithDir(*dir))
	check(err)
	defer client.Close()
	defer startConductor(client)()

	if *mode == "all" || *mode == "throughput" {
		for _, size := range sizes {
			throughput(client, size, *count, *repeats)
		}
	}
	if *mode == "all" || *mode == "latency" {
		for _, size := range sizes {
			latency(client, size, *latCount, *latWarmup)
		}
	}
}

// startConductor runs the client's DoWork in the background, standing in for
// the conductor thread Java's client runs on its own. The Go client has none:
// DoWork must be called to answer the driver and send keepalives.
func startConductor(client *aergo.Aeron) (stop func()) {
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(time.Millisecond)
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
	pub, err := client.AddPublication(channel, pingStream)
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
			for local < int64(count) {
				sub.Poll(asm.OnFragment, fragmentLimit)
			}
			received.Store(local)
			close(done)
		}()

		runtime.LockOSThread()
		t0 := time.Now()
		for i := 0; i < count; i++ {
			offer(pub, src)
		}
		<-done
		secs := time.Since(t0).Seconds()
		runtime.UnlockOSThread()

		if run > 0 {
			fmt.Printf("RESULT lang=go mode=throughput size=%d run=%d msgs=%d msgs_per_sec=%.0f mb_per_sec=%.1f\n",
				size, run, count, float64(count)/secs, float64(count)*float64(size)/secs/1e6)
		}
	}
}

// latency: closed-loop ping-pong, one message outstanding; round-trip time in ns.
func latency(client *aergo.Aeron, size, count, warmup int) {
	ping, err := client.AddPublication(channel, pingStream)
	check(err)
	defer ping.Close()
	pingSub, err := client.AddSubscription(channel, pingStream)
	check(err)
	defer pingSub.Close()
	pong, err := client.AddPublication(channel, pongStream)
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
	go func() {
		runtime.LockOSThread()
		for !stop.Load() {
			pingSub.Poll(echo.OnFragment, fragmentLimit)
		}
		close(ponged)
	}()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	got := 0
	reply := aergo.NewFragmentAssembler(func([]byte, *aergo.Header) { got++ })
	samples := make([]int64, count)
	for i := 0; i < warmup+count; i++ {
		got = 0
		t0 := time.Now()
		offer(ping, src)
		for got == 0 {
			pongSub.Poll(reply.OnFragment, fragmentLimit)
		}
		rtt := time.Since(t0).Nanoseconds()
		if i >= warmup {
			samples[i-warmup] = rtt
		}
	}
	stop.Store(true)
	<-ponged

	sort.Slice(samples, func(a, b int) bool { return samples[a] < samples[b] })
	pct := func(p float64) int64 {
		idx := int(p*float64(len(samples))+0.999999999) - 1
		if idx >= len(samples) {
			idx = len(samples) - 1
		}
		return samples[idx]
	}
	fmt.Printf("RESULT lang=go mode=latency size=%d samples=%d p50_ns=%d p90_ns=%d p99_ns=%d p999_ns=%d max_ns=%d\n",
		size, count, pct(0.50), pct(0.90), pct(0.99), pct(0.999), samples[len(samples)-1])
}

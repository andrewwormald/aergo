package aergo

import (
	"bytes"
	"testing"
)

// scriptedOps drives a publication through small and fragmented messages, a
// claim, and enough data to rotate through several terms, with a subscriber
// draining as it goes (and, standing in for the media driver, cleaning the term
// that is about to be reused). It returns every Offer result and every message
// delivered, in order.
func scriptedOps(t *testing.T, exclusive bool) (results []int64, delivered [][]byte, tail int64) {
	t.Helper()
	const termLen = 64 * 1024
	pub, sub, lb := newFragPair(termLen)
	pub.exclusive = exclusive

	asm := NewFragmentAssembler(func(m []byte, _ *Header) { delivered = append(delivered, append([]byte(nil), m...)) })
	drainNow := func() {
		for sub.Poll(asm.OnFragment, 64) > 0 {
		}
	}
	offer := func(msg []byte) {
		for {
			r, err := pub.Offer(msg)
			if err != nil {
				t.Fatal(err)
			}
			results = append(results, r)
			if r == AdminAction {
				zeroTerm(lb, int(lb.ActiveTermCount()+1)%PartitionCount, termLen)
				continue
			}
			if r < 0 {
				t.Fatalf("offer = %d", r)
			}
			drainNow()
			return
		}
	}

	sizes := []int{0, 1, 31, 32, 100, 1375, 1376, 1377, 2752, 3000, 7000, 8000, 50, 5000}
	for round := 0; round < 14; round++ { // ~6 terms of data
		for i, n := range sizes {
			offer(patterned(n, byte(round*len(sizes)+i)))
		}
		claim, pos, err := pub.TryClaim(50)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, pos)
		if pos > 0 {
			copy(claim.Buffer(), patterned(50, 0xEE))
			claim.Commit()
			drainNow()
		}
	}
	rawTail := lb.Meta().GetInt64Volatile(MetaTermTailCounterOff + int32(lb.ActiveTermCount()%PartitionCount)*8)
	return results, delivered, rawTail
}

// The single-writer claim must produce exactly the log the shared one does:
// same positions, same messages, same rotations.
func TestExclusivePublicationWritesTheSameLogAsShared(t *testing.T) {
	sr, sd, st := scriptedOps(t, false)
	er, ed, et := scriptedOps(t, true)

	if len(sr) != len(er) {
		t.Fatalf("shared made %d offer results, exclusive %d", len(sr), len(er))
	}
	for i := range sr {
		if sr[i] != er[i] {
			t.Fatalf("result %d: shared %d, exclusive %d", i, sr[i], er[i])
		}
	}
	if len(sd) != len(ed) {
		t.Fatalf("shared delivered %d messages, exclusive %d", len(sd), len(ed))
	}
	for i := range sd {
		if !bytes.Equal(sd[i], ed[i]) {
			t.Fatalf("message %d differs between shared and exclusive", i)
		}
	}
	if st != et {
		t.Errorf("final tail: shared %#x, exclusive %#x", st, et)
	}
	rotations := 0
	for _, r := range sr {
		if r == AdminAction {
			rotations++
		}
	}
	if rotations < 3 {
		t.Errorf("only %d term rotations; the script should cross several", rotations)
	}
}

// One goroutine offering while another polls the same log, with rotation:
// the single-writer path publishes with the release store, so the subscriber
// must see whole messages only. Under -race this also checks the handoff.
func TestExclusivePublicationWithAConcurrentSubscriber(t *testing.T) {
	publishAndPollConcurrently(t, 4*1024*1024, 1, 200, 3000, true)
	admin := publishAndPollConcurrently(t, 512*1024, 1, 150, 7000, true)
	if admin == 0 {
		t.Error("no offer hit the end of a term; the test is not exercising rotation")
	}
}

// AddExclusivePublication, and only it, makes the publication single-writer.
func TestOnlyExclusivePublicationsAreSingleWriter(t *testing.T) {
	c := newBareConductor()
	c.proxy = NewDriverProxy(newTestRingBuffer(4096), 1)

	shared := c.AddPublication("aeron:ipc", 1)
	excl := c.AddExclusivePublication("aeron:ipc", 2)
	if st := c.publications[shared]; st.exclusive || newPublication(c, st).exclusive {
		t.Error("a shared publication is marked exclusive")
	}
	if st := c.publications[excl]; !st.exclusive || !newPublication(c, st).exclusive {
		t.Error("an exclusive publication is not marked exclusive")
	}
}

func BenchmarkPublicationOfferExclusive(b *testing.B) {
	for _, exclusive := range []bool{false, true} {
		name := "shared"
		if exclusive {
			name = "exclusive"
		}
		b.Run(name, func(b *testing.B) {
			lb := newInMemLogBuffers(1024 * 1024)
			pub := newInMemPublication(lb, 1, 1001)
			pub.exclusive = exclusive
			payload := make([]byte, 32)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if r, _ := pub.Offer(payload); r < 0 {
					b.StopTimer()
					resetLogBufferToGenesis(lb, 1024*1024)
					b.StartTimer()
				}
			}
		})
	}
}

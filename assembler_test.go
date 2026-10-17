package aergo

import "testing"

func TestFragmentAssemblerUnfragmented(t *testing.T) {
	var received []byte
	assembler := NewFragmentAssembler(func(buffer []byte, header *Header) {
		received = append([]byte(nil), buffer...)
	})

	assembler.OnFragment([]byte("hello"), &Header{Flags: FlagUnfrag, SessionID: 1})
	if string(received) != "hello" {
		t.Errorf("got %q, want %q", received, "hello")
	}
}

func TestFragmentAssemblerMultiFragment(t *testing.T) {
	var received []byte
	assembler := NewFragmentAssembler(func(buffer []byte, header *Header) {
		received = append([]byte(nil), buffer...)
	})

	assembler.OnFragment([]byte("hel"), &Header{Flags: FlagBeginFrag, SessionID: 1})
	if received != nil {
		t.Fatal("should not deliver on begin fragment")
	}

	assembler.OnFragment([]byte("lo "), &Header{Flags: 0, SessionID: 1})
	if received != nil {
		t.Fatal("should not deliver on middle fragment")
	}

	assembler.OnFragment([]byte("world"), &Header{Flags: FlagEndFrag, SessionID: 1})
	if string(received) != "hello world" {
		t.Errorf("got %q, want %q", received, "hello world")
	}
}

func TestFragmentAssemblerMultipleSessions(t *testing.T) {
	results := make(map[int32]string)
	assembler := NewFragmentAssembler(func(buffer []byte, header *Header) {
		results[header.SessionID] = string(buffer)
	})

	assembler.OnFragment([]byte("A1"), &Header{Flags: FlagBeginFrag, SessionID: 1})
	assembler.OnFragment([]byte("B1"), &Header{Flags: FlagBeginFrag, SessionID: 2})
	assembler.OnFragment([]byte("A2"), &Header{Flags: FlagEndFrag, SessionID: 1})
	assembler.OnFragment([]byte("B2"), &Header{Flags: FlagEndFrag, SessionID: 2})

	if results[1] != "A1A2" {
		t.Errorf("session 1: got %q", results[1])
	}
	if results[2] != "B1B2" {
		t.Errorf("session 2: got %q", results[2])
	}
}

func TestFragmentAssemblerOrphanFragment(t *testing.T) {
	var called bool
	assembler := NewFragmentAssembler(func(buffer []byte, header *Header) {
		called = true
	})

	// Middle fragment without a begin -- should be discarded
	assembler.OnFragment([]byte("orphan"), &Header{Flags: 0, SessionID: 1})
	assembler.OnFragment([]byte("end"), &Header{Flags: FlagEndFrag, SessionID: 1})

	if called {
		t.Error("orphan fragments should not be delivered")
	}
}

// The buffer handed to the delegate is reused for the next message of the
// session, so reassembly settles to no allocation once it has grown.
func TestFragmentAssemblerSteadyStateDoesNotAllocate(t *testing.T) {
	var delivered int
	a := NewFragmentAssembler(func([]byte, *Header) { delivered++ })
	chunk := make([]byte, 1376)
	begin := &Header{SessionID: 1, Flags: FlagBeginFrag}
	mid := &Header{SessionID: 1, Flags: 0}
	end := &Header{SessionID: 1, Flags: FlagEndFrag}
	send := func() {
		a.OnFragment(chunk, begin)
		a.OnFragment(chunk, mid)
		a.OnFragment(chunk, end)
	}
	send() // grow the buffer

	if allocs := testing.AllocsPerRun(100, send); allocs != 0 {
		t.Errorf("steady-state reassembly allocates %v times per message", allocs)
	}
	if delivered == 0 {
		t.Fatal("nothing delivered")
	}
}

// A new begin fragment discards a message whose end never arrived, and does not
// leak its bytes into the next one.
func TestFragmentAssemblerBeginDropsUnfinishedMessage(t *testing.T) {
	var got []string
	a := NewFragmentAssembler(func(b []byte, _ *Header) { got = append(got, string(b)) })
	a.OnFragment([]byte("lost-"), &Header{SessionID: 1, Flags: FlagBeginFrag})
	a.OnFragment([]byte("ab"), &Header{SessionID: 1, Flags: FlagBeginFrag})
	a.OnFragment([]byte("cd"), &Header{SessionID: 1, Flags: FlagEndFrag})
	if len(got) != 1 || got[0] != "abcd" {
		t.Errorf("got %q, want [abcd]", got)
	}
}

// An end fragment arriving after a message completed is an orphan, not the
// start of another delivery.
func TestFragmentAssemblerOrphanEndAfterCompletion(t *testing.T) {
	n := 0
	a := NewFragmentAssembler(func([]byte, *Header) { n++ })
	a.OnFragment([]byte("a"), &Header{SessionID: 1, Flags: FlagBeginFrag})
	a.OnFragment([]byte("b"), &Header{SessionID: 1, Flags: FlagEndFrag})
	a.OnFragment([]byte("c"), &Header{SessionID: 1, Flags: FlagEndFrag})
	if n != 1 {
		t.Errorf("delivered %d messages, want 1", n)
	}
}

func TestFragmentAssemblerFreeSessionBuffer(t *testing.T) {
	n := 0
	a := NewFragmentAssembler(func([]byte, *Header) { n++ })
	a.OnFragment([]byte("a"), &Header{SessionID: 1, Flags: FlagBeginFrag})
	a.FreeSessionBuffer(1)
	a.OnFragment([]byte("b"), &Header{SessionID: 1, Flags: FlagEndFrag})
	if n != 0 {
		t.Errorf("delivered %d messages after the session buffer was freed, want 0", n)
	}
	// And the session is usable again afterwards.
	a.OnFragment([]byte("x"), &Header{SessionID: 1, Flags: FlagBeginFrag})
	a.OnFragment([]byte("y"), &Header{SessionID: 1, Flags: FlagEndFrag})
	if n != 1 {
		t.Errorf("session not usable after FreeSessionBuffer: delivered %d", n)
	}
}

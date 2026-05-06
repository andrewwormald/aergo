package aeron

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

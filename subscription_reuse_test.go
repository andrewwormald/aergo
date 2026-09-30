package aergo

import (
	"bytes"
	"fmt"
	"testing"
)

// TestPoll_HandlerSeesItsOwnFragment is the guarantee the reuse rests
// on: the handler is called synchronously, so it runs to completion
// before the loop overwrites the shared buffer for the next fragment.
// Each call must therefore see its own payload, not a later one.
func TestPoll_HandlerSeesItsOwnFragment(t *testing.T) {
	termLen := int32(64 * 1024)
	lb := newInMemLogBuffers(termLen)
	app := NewTermAppender(lb, 0)

	const frames = 64
	for i := 0; i < frames; i++ {
		// Distinct payload per fragment, so a stale buffer is visible.
		app.Append(0, 1, 1001, []byte(fmt.Sprintf("fragment-%03d-payload", i)))
	}

	sub := newInMemSubscription(lb, 1001)
	var seen [][]byte
	var headers []Header
	n := sub.Poll(func(buf []byte, h *Header) {
		// Copy inside the call, which is what the contract requires.
		seen = append(seen, append([]byte(nil), buf...))
		headers = append(headers, *h)
	}, frames)

	if n != frames {
		t.Fatalf("polled %d fragments, want %d", n, frames)
	}
	for i, got := range seen {
		want := []byte(fmt.Sprintf("fragment-%03d-payload", i))
		if !bytes.Equal(got, want) {
			t.Errorf("fragment %d = %q, want %q — the handler saw another fragment's data", i, got, want)
		}
	}
	// Headers must also differ per fragment, not all be the last one.
	for i := 1; i < len(headers); i++ {
		if headers[i].TermOffset == headers[i-1].TermOffset {
			t.Errorf("header %d has the same TermOffset as %d; header state did not advance", i, i-1)
		}
	}
}

// TestPoll_RetainedBufferIsOverwritten documents the hazard the
// contract exists to prevent. A handler that keeps the slice instead of
// copying sees it change under it. This is the behaviour Aeron has, and
// the test is here so the consequence is written down rather than
// discovered.
func TestPoll_RetainedBufferIsOverwritten(t *testing.T) {
	termLen := int32(64 * 1024)
	lb := newInMemLogBuffers(termLen)
	app := NewTermAppender(lb, 0)
	app.Append(0, 1, 1001, []byte("first-fragment"))
	app.Append(0, 1, 1001, []byte("second-fragment"))

	sub := newInMemSubscription(lb, 1001)
	var retained [][]byte
	var retainedHeaders []*Header
	sub.Poll(func(buf []byte, h *Header) {
		retained = append(retained, buf) // deliberately not copied
		retainedHeaders = append(retainedHeaders, h)
	}, 2)

	if len(retained) != 2 {
		t.Fatalf("got %d fragments, want 2", len(retained))
	}
	if !bytes.HasPrefix(retained[1], []byte("second-fragment")) {
		t.Fatalf("second fragment = %q, want it to start with second-fragment", retained[1])
	}
	// The retained slices alias one buffer, so the first now reads as
	// the second. If this ever stops being true the contract has
	// loosened and the doc should say so.
	if bytes.HasPrefix(retained[0], []byte("first-fragment")) {
		t.Log("retained buffers did not alias; reuse is not observable here")
	} else {
		t.Logf("confirmed: retained buffer was overwritten (now %q) — handlers must copy", retained[0])
	}
	if retainedHeaders[0] != retainedHeaders[1] {
		t.Log("headers did not alias")
	} else {
		t.Log("confirmed: retained headers alias one struct — handlers must copy")
	}
}

// TestPoll_ScratchGrowthSettles covers a stream of increasing payload
// sizes. Growing to the exact size would reallocate at every new
// high-water mark; growing with slack has to settle instead.
func TestPoll_ScratchGrowthSettles(t *testing.T) {
	termLen := int32(1024 * 1024)
	lb := newInMemLogBuffers(termLen)
	app := NewTermAppender(lb, 0)

	const frames = 40
	for i := 1; i <= frames; i++ {
		app.Append(0, 1, 1001, make([]byte, i*16)) // steadily larger
	}
	sub := newInMemSubscription(lb, 1001)

	grows := 0
	lastCap := 0
	sub.Poll(func(buf []byte, h *Header) {
		if c := cap(sub.payloadScratch); c != lastCap {
			grows++
			lastCap = c
		}
	}, frames)

	// Doubling from nothing to 640 bytes is a handful of steps, not one
	// per fragment.
	if grows > 12 {
		t.Errorf("scratch grew %d times over %d increasing payloads; growth is not settling", grows, frames)
	}
	t.Logf("grew %d times over %d fragments, final cap %d", grows, frames, lastCap)
}

// TestPoll_HandlesPayloadLargerThanScratch is the boundary the growth
// path exists for.
func TestPoll_HandlesPayloadLargerThanScratch(t *testing.T) {
	termLen := int32(1024 * 1024)
	lb := newInMemLogBuffers(termLen)
	app := NewTermAppender(lb, 0)

	small := []byte("tiny")
	large := make([]byte, 8192)
	for i := range large {
		large[i] = byte('a' + i%26)
	}
	app.Append(0, 1, 1001, small)
	app.Append(0, 1, 1001, large)

	sub := newInMemSubscription(lb, 1001)
	var got [][]byte
	sub.Poll(func(buf []byte, h *Header) {
		got = append(got, append([]byte(nil), buf...))
	}, 2)

	if len(got) != 2 {
		t.Fatalf("got %d fragments, want 2", len(got))
	}
	if string(got[0]) != "tiny" {
		t.Errorf("first = %q, want tiny", got[0])
	}
	if len(got[1]) != len(large) {
		t.Fatalf("second len = %d, want %d", len(got[1]), len(large))
	}
	for i := range large {
		if got[1][i] != large[i] {
			t.Fatalf("second fragment differs at byte %d after the scratch grew", i)
		}
	}
}

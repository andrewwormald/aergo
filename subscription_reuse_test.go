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

// TestPoll_PayloadIsAViewIntoTheLog pins the contract the handler is given,
// which is the Java client's: the payload is not a copy but the log's own
// memory, valid only for the call, so the handler must copy what it keeps. The
// header, by contrast, is one struct reused for every fragment.
func TestPoll_PayloadIsAViewIntoTheLog(t *testing.T) {
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
	if !bytes.Equal(retained[0], []byte("first-fragment")) || !bytes.Equal(retained[1], []byte("second-fragment")) {
		t.Fatalf("payloads = %q, %q", retained[0], retained[1])
	}

	// Changing the log changes what was handed out: it is a view, not a copy.
	lb.Term(0).PutBytes(DataFrameHeaderLen, []byte("FIRST"))
	if !bytes.HasPrefix(retained[0], []byte("FIRST")) {
		t.Errorf("payload did not follow the log (%q): it was copied", retained[0])
	}

	if retainedHeaders[0] != retainedHeaders[1] {
		t.Error("headers are distinct structs; they are meant to be reused across fragments")
	}
}

// TestPoll_DoesNotCopyPayloads: polling large payloads allocates nothing and
// the cost does not grow with the payload.
func TestPoll_DoesNotCopyPayloads(t *testing.T) {
	lb := newInMemLogBuffers(1024 * 1024)
	app := NewTermAppender(lb, 0)
	app.Append(0, 1, 1001, make([]byte, 8000))
	sub := newInMemSubscription(lb, 1001)
	img := sub.state.images[0]

	n := 0
	allocs := testing.AllocsPerRun(50, func() {
		img.subscriberPosition = 0
		n += sub.Poll(func(buf []byte, h *Header) {}, 1)
	})
	if allocs != 0 {
		t.Errorf("Poll of an 8000 byte payload allocates %v times", allocs)
	}
	if n == 0 {
		t.Fatal("nothing polled")
	}
}

// TestPoll_HandlesLargePayload checks a payload much larger than the one
// before it arrives intact.
func TestPoll_HandlesLargePayload(t *testing.T) {
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
			t.Fatalf("second fragment differs at byte %d ", i)
		}
	}
}

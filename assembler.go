package aergo

// FragmentAssembler reassembles fragmented messages into complete messages
// before delivering to the delegate handler.
//
// As with Java's FragmentAssembler, each session has one buffer that is
// reused for every message, so reassembly allocates only while the buffer
// grows to the largest message seen. The buffer handed to the delegate for a
// reassembled message is therefore valid only for the duration of the call,
// the same contract an unfragmented message already has; copy it to keep it.
type FragmentAssembler struct {
	delegate FragmentHandler
	builders map[int32]*messageBuilder

	// The last builder used, so a stream from one session (the common case)
	// skips the map lookup on every fragment.
	lastSession int32
	last        *messageBuilder

	// assembled is the header handed to the delegate for a reassembled message.
	assembled Header
}

// messageBuilder accumulates the fragments of one in-flight message.
type messageBuilder struct {
	buf             []byte
	active          bool  // a begin fragment has been seen and no end fragment yet
	beginTermOffset int32 // where the message starts in the term
}

// NewFragmentAssembler wraps a handler with fragment reassembly.
func NewFragmentAssembler(delegate FragmentHandler) *FragmentAssembler {
	return &FragmentAssembler{
		delegate: delegate,
		builders: make(map[int32]*messageBuilder),
	}
}

func (a *FragmentAssembler) builder(sessionID int32, create bool) *messageBuilder {
	if a.last != nil && a.lastSession == sessionID {
		return a.last
	}
	b := a.builders[sessionID]
	if b == nil {
		if !create {
			return nil
		}
		b = &messageBuilder{}
		a.builders[sessionID] = b
	}
	a.lastSession, a.last = sessionID, b
	return b
}

// FreeSessionBuffer releases the buffer held for a session, for when its
// image has gone away (Java FragmentAssembler.freeSessionBuffer). A partly
// assembled message for the session is discarded.
func (a *FragmentAssembler) FreeSessionBuffer(sessionID int32) {
	delete(a.builders, sessionID)
	if a.last != nil && a.lastSession == sessionID {
		a.last = nil
	}
}

// OnFragment handles a single fragment, buffering partial messages
// and delivering complete ones to the delegate.
func (a *FragmentAssembler) OnFragment(buffer []byte, header *Header) {
	flags := header.Flags

	if flags&FlagUnfrag == FlagUnfrag {
		// Unfragmented message -- pass through directly
		a.delegate(buffer, header)
		return
	}

	if flags&FlagBeginFrag == FlagBeginFrag {
		// Start of a new fragmented message; a message left unfinished by a
		// lost end fragment is dropped.
		b := a.builder(header.SessionID, true)
		b.buf = append(b.buf[:0], buffer...)
		b.active = true
		b.beginTermOffset = header.TermOffset
		return
	}

	b := a.builder(header.SessionID, false)
	if b == nil || !b.active {
		return // no begin fragment seen, discard
	}

	b.buf = append(b.buf, buffer...)

	if flags&FlagEndFrag == FlagEndFrag {
		// Final fragment -- deliver complete message
		b.active = false
		// The header describes the message, not its last fragment, as in Java's
		// FragmentAssembler: the whole length, flagged unfragmented, starting where
		// the first fragment did. The position is that of the end of the message.
		a.assembled = *header
		a.assembled.FrameLength = int32(DataFrameHeaderLen + len(b.buf))
		a.assembled.Flags = FlagUnfrag
		a.assembled.TermOffset = b.beginTermOffset
		a.delegate(b.buf, &a.assembled)
	}
}

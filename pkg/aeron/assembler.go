package aeron

// FragmentAssembler reassembles fragmented messages into complete messages
// before delivering to the delegate handler.
type FragmentAssembler struct {
	delegate FragmentHandler
	buffers  map[int32][]byte // sessionID -> assembly buffer
}

// NewFragmentAssembler wraps a handler with fragment reassembly.
func NewFragmentAssembler(delegate FragmentHandler) *FragmentAssembler {
	return &FragmentAssembler{
		delegate: delegate,
		buffers:  make(map[int32][]byte),
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
		// Start of a new fragmented message
		a.buffers[header.SessionID] = append([]byte(nil), buffer...)
		return
	}

	assembled, ok := a.buffers[header.SessionID]
	if !ok {
		return // no begin fragment seen, discard
	}

	// Append this fragment
	assembled = append(assembled, buffer...)
	a.buffers[header.SessionID] = assembled

	if flags&FlagEndFrag == FlagEndFrag {
		// Final fragment -- deliver complete message
		delete(a.buffers, header.SessionID)
		a.delegate(assembled, header)
	}
}

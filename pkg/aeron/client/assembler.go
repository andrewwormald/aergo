package client

// FragmentAssembler reassembles fragmented messages in pure Go.
// Eliminates the C-side fragment assembler and its allocations.
// One assembler per subscription -- not thread-safe.
type FragmentAssembler struct {
	delegate FragmentHandler
	sessions map[int32]*assemblyBuffer
}

type assemblyBuffer struct {
	buf    []byte
	offset int
}

// NewFragmentAssembler creates a Go-native fragment assembler.
// The delegate receives complete reassembled messages.
func NewFragmentAssembler(delegate FragmentHandler) *FragmentAssembler {
	return &FragmentAssembler{
		delegate: delegate,
		sessions: make(map[int32]*assemblyBuffer),
	}
}

// OnFragment is the FragmentHandler to pass to Subscription.Poll.
// It handles fragment reassembly and delivers complete messages to the delegate.
func (fa *FragmentAssembler) OnFragment(buffer []byte, header *Header) {
	if header == nil || header.IsUnfragmented() {
		// No header or unfragmented -- pass through directly
		fa.delegate(buffer, header)
		return
	}

	isBegin := header.IsBeginFragment()
	isEnd := header.IsEndFragment()
	sessionId := header.SessionId

	if isBegin {
		// First fragment -- start assembly
		ab := fa.getOrCreate(sessionId, len(buffer)*4)
		ab.offset = 0
		ab.append(buffer)
		return
	}

	ab, ok := fa.sessions[sessionId]
	if !ok {
		// Middle/end fragment without a begin -- discard
		return
	}

	ab.append(buffer)

	if isEnd {
		// Final fragment -- deliver assembled message
		fa.delegate(ab.buf[:ab.offset], header)
		ab.offset = 0
	}
}

func (fa *FragmentAssembler) getOrCreate(sessionId int32, initialCap int) *assemblyBuffer {
	ab, ok := fa.sessions[sessionId]
	if !ok {
		ab = &assemblyBuffer{buf: make([]byte, initialCap)}
		fa.sessions[sessionId] = ab
	}
	return ab
}

func (ab *assemblyBuffer) append(data []byte) {
	needed := ab.offset + len(data)
	if needed > len(ab.buf) {
		newBuf := make([]byte, needed*2)
		copy(newBuf, ab.buf[:ab.offset])
		ab.buf = newBuf
	}
	copy(ab.buf[ab.offset:], data)
	ab.offset += len(data)
}

// Free releases all assembly buffers.
func (fa *FragmentAssembler) Free() {
	for k := range fa.sessions {
		delete(fa.sessions, k)
	}
}

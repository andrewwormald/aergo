package aergo

import "sync/atomic"

// Header contains parsed information from a received message frame.
type Header struct {
	FrameLength   int32
	Flags         uint8
	SessionID     int32
	StreamID      int32
	TermID        int32
	TermOffset    int32
	ReservedValue int64
	Position      int64
}

// FragmentHandler is called for each received message fragment.
//
// As in the Java client, buffer is a view into the log buffer, not a copy: it
// is valid only until the handler returns, and the handler must not modify or
// retain it. Copy what you need to keep. header is likewise only valid for the
// call.
type FragmentHandler func(buffer []byte, header *Header)

// Subscription receives messages from one or more publications via images.
//
// Poll is not safe to call concurrently for the same Subscription. This
// matches Aeron itself, where a subscription is drained by the one
// thread that owns it, and it is what lets Poll reuse scratch state
// between calls instead of allocating on every pass. Close may be
// called from any goroutine.
type Subscription struct {
	conductor      *Conductor
	state          *subscriptionState
	channel        string
	streamID       int32
	registrationID int64
	closed         atomic.Bool

	// Scratch reused across fragments, owned by the polling goroutine.
	// Both are handed to the handler, which is why the contract above
	// says they are only valid for the duration of the call.
	headerScratch Header
}

func newSubscription(conductor *Conductor, corrID int64, state *subscriptionState) *Subscription {
	return &Subscription{
		conductor:      conductor,
		state:          state,
		channel:        state.channel,
		streamID:       state.streamID,
		registrationID: corrID,
	}
}

// Poll reads available messages from the subscription's images.
// Returns the number of fragments received.
func (s *Subscription) Poll(handler FragmentHandler, fragmentLimit int) int {
	if s.closed.Load() {
		return 0
	}

	// Lock-free: the conductor publishes a fresh immutable slice whenever an
	// image comes or goes, so polling never contends with other pollers or
	// with the conductor.
	snap := s.state.snapshot.Load()
	if snap == nil {
		return 0
	}

	totalFragments := 0
	for _, img := range *snap {
		if img.LogBuffers == nil {
			continue
		}
		remaining := fragmentLimit - totalFragments
		if remaining <= 0 {
			break
		}
		totalFragments += s.pollImage(img, handler, remaining)
	}

	return totalFragments
}

// pollImage reads up to limit fragments from one image. It is the same walk of
// the term as ReadTerm, written out so the hot path is one loop with no
// closure and no intermediate header: a handler call per fragment, straight
// from the log. Java's Image.poll / TermReader.read do the same.
func (s *Subscription) pollImage(img *Image, handler FragmentHandler, limit int) int {
	lb := img.LogBuffers
	termLen := lb.TermLength()
	shift := numberOfTrailingZeros(uint32(termLen))
	initialTermID := lb.InitialTermID()

	position := img.subscriberPosition
	termID := initialTermID + int32(position>>shift)
	termOffset := int32(position) & (termLen - 1)
	term := lb.Term(int((termID - initialTermID) % PartitionCount))
	capacity := term.Capacity()
	termStart := position - int64(termOffset) // stream position of this term's first byte

	offset := termOffset
	fragments := 0
	for fragments < limit && offset < capacity {
		frameLen := term.GetInt32Volatile(offset + FrameLengthOffset)
		if frameLen <= 0 {
			break // no more committed frames
		}
		aligned := align(frameLen, DataFrameHeaderLen)

		if int16(term.GetInt32(offset+FrameTypeOffset)) != FrameTypePadding {
			// The handler gets a view straight into the log, as Java's
			// FragmentHandler does, so the payload is not copied unless the
			// handler copies it. The header is reused across fragments. Both
			// are valid only for the duration of the call.
			length := frameLen - DataFrameHeaderLen
			if length >= 0 && int64(offset)+int64(frameLen) <= int64(capacity) {
				s.headerScratch = Header{
					FrameLength:   frameLen,
					Flags:         term.GetUint8(offset + FrameFlagsOffset),
					SessionID:     term.GetInt32(offset + FrameSessionIDOff),
					StreamID:      term.GetInt32(offset + FrameStreamIDOff),
					TermID:        term.GetInt32(offset + FrameTermIDOff),
					TermOffset:    term.GetInt32(offset + FrameTermOffsetOff),
					ReservedValue: term.GetInt64(offset + FrameReservedValueOff),
					Position:      termStart + int64(offset) + int64(aligned),
				}
				handler(term.Slice(offset+DataFrameHeaderLen, length), &s.headerScratch)
			}
			fragments++
		}

		offset += aligned
	}

	// Advance even when only padding was consumed (fragments == 0 but the
	// offset moved) so the reader can cross a term boundary. Every advance is
	// also published to the driver's subscriber position counter so flow
	// control does not stall the publication.
	if offset > termOffset {
		img.updatePosition(position + int64(offset-termOffset))
	}

	return fragments
}

// StreamID returns the stream identifier.
func (s *Subscription) StreamID() int32 { return s.streamID }

// Close releases the subscription.
func (s *Subscription) Close() {
	if s.closed.CompareAndSwap(false, true) {
		// A loopback subscription has no driver to tell.
		if s.conductor != nil && s.conductor.proxy != nil {
			s.conductor.proxy.RemoveSubscription(s.registrationID)
		}
	}
}

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
	channel        string
	streamID       int32
	registrationID int64
	closed         atomic.Bool

	// Scratch reused across fragments, owned by the polling goroutine.
	// Both are handed to the handler, which is why the contract above
	// says they are only valid for the duration of the call.
	payloadScratch []byte
	headerScratch  Header
}

func newSubscription(conductor *Conductor, corrID int64, state *subscriptionState) *Subscription {
	return &Subscription{
		conductor:      conductor,
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

	state := s.conductor.FindSubscription(s.registrationID)
	if state == nil {
		return 0
	}

	// Snapshot under the lock, then iterate outside it. Appending into
	// the retained scratch reuses its capacity instead of allocating a
	// new slice on every poll.
	s.conductor.mu.Lock()
	images := make([]*Image, len(state.images))
	copy(images, state.images)
	s.conductor.mu.Unlock()

	totalFragments := 0

	for _, img := range images {
		if img.LogBuffers == nil {
			continue
		}
		remaining := fragmentLimit - totalFragments
		if remaining <= 0 {
			break
		}

		termLen := img.LogBuffers.TermLength()
		shift := numberOfTrailingZeros(uint32(termLen))
		initialTermID := img.LogBuffers.InitialTermID()

		position := img.subscriberPosition
		termID := initialTermID + int32(position>>shift)
		termOffset := int32(position) & (termLen - 1)
		partIndex := int((termID - initialTermID) % PartitionCount)
		term := img.LogBuffers.Term(partIndex)

		fragments, newOffset := ReadTerm(term, termOffset, func(buf *AtomicBuffer, offset, length int32, hdr *DataFrameHeader) {
			// Reuse the buffer and header across fragments rather than
			// allocating a pair per message. Both are only valid for the
			// duration of the handler call, which is the contract Aeron
			// itself has and what every consumer here already assumes -
			// they decode or copy before returning.
			if cap(s.payloadScratch) < int(length) {
				// Grow with slack rather than to the exact size, so a
				// stream of increasing payloads settles instead of
				// reallocating on each new high-water mark.
				grown := 2 * cap(s.payloadScratch)
				if grown < int(length) {
					grown = int(length)
				}
				s.payloadScratch = make([]byte, grown)
			}
			payload := s.payloadScratch[:length]
			buf.GetBytes(offset, payload)

			alignedFrame := align(hdr.FrameLength, DataFrameHeaderLen)
			pos := computePosition(hdr.TermID, hdr.TermOffset+alignedFrame, termLen, initialTermID)

			s.headerScratch = Header{
				FrameLength:   hdr.FrameLength,
				Flags:         hdr.Flags,
				SessionID:     hdr.SessionID,
				StreamID:      hdr.StreamID,
				TermID:        hdr.TermID,
				TermOffset:    hdr.TermOffset,
				ReservedValue: hdr.ReservedValue,
				Position:      pos,
			}
			handler(payload, &s.headerScratch)
		}, remaining)

		// Advance even when only padding was consumed (fragments == 0 but
		// the offset moved) so the reader can cross a term boundary. Every
		// advance is also published to the driver's subscriber position
		// counter so flow control does not stall the publication.
		if newOffset > termOffset {
			img.updatePosition(computePosition(termID, newOffset, termLen, initialTermID))
		}

		totalFragments += fragments
	}

	return totalFragments
}

// StreamID returns the stream identifier.
func (s *Subscription) StreamID() int32 { return s.streamID }

// Close releases the subscription.
func (s *Subscription) Close() {
	if s.closed.CompareAndSwap(false, true) {
		s.conductor.proxy.RemoveSubscription(s.registrationID)
	}
}

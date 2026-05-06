package aeron

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
}

// FragmentHandler is the callback for received message fragments.
type FragmentHandler func(buffer []byte, header *Header)

// Subscription wraps images for receiving messages from a stream.
type Subscription struct {
	conductor      *Conductor
	channel        string
	streamID       int32
	registrationID int64
	closed         atomic.Bool
}

// newSubscription creates a subscription from a ready subscriptionState.
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

	totalFragments := 0

	s.conductor.mu.Lock()
	images := make([]*Image, len(state.images))
	copy(images, state.images)
	s.conductor.mu.Unlock()

	for _, img := range images {
		if img.LogBuffers == nil {
			continue
		}

		remaining := fragmentLimit - totalFragments
		if remaining <= 0 {
			break
		}

		// Determine which term and offset to read from
		activeCount := img.LogBuffers.ActiveTermCount()
		partIndex := int(activeCount % PartitionCount)
		term := img.LogBuffers.Term(partIndex)

		// Read subscriber position from counter
		_, termOffset := img.LogBuffers.TermTailCounter(partIndex)
		// Start reading from position 0 or tracked position
		// (simplified -- production would track per-image position)

		fragments, _ := ReadTerm(term, 0, func(buf *AtomicBuffer, offset, length int32, hdr *DataFrameHeader) {
			payload := make([]byte, length)
			buf.GetBytes(offset, payload)
			h := &Header{
				FrameLength:   hdr.FrameLength,
				Flags:         hdr.Flags,
				SessionID:     hdr.SessionID,
				StreamID:      hdr.StreamID,
				TermID:        hdr.TermID,
				TermOffset:    hdr.TermOffset,
				ReservedValue: hdr.ReservedValue,
			}
			handler(payload, h)
		}, remaining)

		totalFragments += fragments
		_ = termOffset
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

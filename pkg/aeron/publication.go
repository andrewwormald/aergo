package aeron

import "sync/atomic"

// Publication wraps a log buffer for sending messages to a stream.
type Publication struct {
	conductor      *Conductor
	channel        string
	streamID       int32
	sessionID      int32
	registrationID int64
	logBuffers     *LogBuffers
	initialTermID  int32
	posLimit       int32
	closed         atomic.Bool
}

func newPublication(conductor *Conductor, state *publicationState) *Publication {
	p := &Publication{
		conductor:      conductor,
		channel:        state.channel,
		streamID:       state.streamID,
		sessionID:      state.sessionID,
		registrationID: state.registrationID,
		logBuffers:     state.logBuffers,
	}
	if state.logBuffers != nil {
		p.initialTermID = state.logBuffers.InitialTermID()
	}
	return p
}

// Offer sends a message to the publication's stream.
func (p *Publication) Offer(buf []byte) int64 {
	if p.closed.Load() || p.logBuffers == nil {
		return -4
	}
	if !p.logBuffers.IsConnected() {
		return -1
	}

	activeCount := p.logBuffers.ActiveTermCount()
	partIndex := int(activeCount % PartitionCount)
	term := p.logBuffers.Term(partIndex)
	termLen := term.Capacity()
	termID := p.initialTermID + activeCount

	frameLen := int32(DataFrameHeaderLen + len(buf))
	alignedLen := align(frameLen, DataFrameHeaderLen)

	tailOff := int32(MetaTermTailCounterOff + partIndex*8)
	rawTail := p.logBuffers.Meta().GetAndAddInt64(tailOff, int64(alignedLen))
	termOffset := int32(rawTail)

	if termOffset+alignedLen > termLen {
		return -2
	}

	term.PutInt32(termOffset+FrameLengthOffset, 0)
	term.PutUint8(termOffset+FrameVersionOffset, 0)
	term.PutUint8(termOffset+FrameFlagsOffset, FlagUnfrag)
	term.PutInt32(termOffset+FrameTypeOffset, FrameTypeData)
	term.PutInt32(termOffset+FrameTermOffsetOff, termOffset)
	term.PutInt32(termOffset+FrameSessionIDOff, p.sessionID)
	term.PutInt32(termOffset+FrameStreamIDOff, p.streamID)
	term.PutInt32(termOffset+FrameTermIDOff, termID)
	term.PutBytes(termOffset+DataFrameHeaderLen, buf)
	term.PutInt32Ordered(termOffset+FrameLengthOffset, frameLen)

	return computePosition(termID, termOffset+alignedLen, termLen, p.initialTermID)
}

// OfferWithRetry retries Offer on back-pressure up to maxRetries times.
func (p *Publication) OfferWithRetry(buf []byte, maxRetries int) int64 {
	for i := 0; i <= maxRetries; i++ {
		result := p.Offer(buf)
		if result > 0 || result == -1 || result == -4 {
			return result
		}
	}
	return -2
}

// IsConnected returns whether the publication has active subscribers.
func (p *Publication) IsConnected() bool {
	return p.logBuffers != nil && p.logBuffers.IsConnected()
}

// StreamID returns the stream identifier.
func (p *Publication) StreamID() int32 { return p.streamID }

// SessionID returns the session identifier.
func (p *Publication) SessionID() int32 { return p.sessionID }

// Close releases the publication.
func (p *Publication) Close() {
	if p.closed.CompareAndSwap(false, true) {
		p.conductor.proxy.RemovePublication(p.registrationID)
	}
}

func computePosition(termID, termOffset, termLen, initialTermID int32) int64 {
	shift := numberOfTrailingZeros(uint32(termLen))
	return int64(termID-initialTermID)<<shift + int64(termOffset)
}

func numberOfTrailingZeros(v uint32) int {
	if v == 0 {
		return 32
	}
	n := 0
	for v&1 == 0 {
		n++
		v >>= 1
	}
	return n
}

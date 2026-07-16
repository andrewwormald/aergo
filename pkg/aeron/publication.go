package aeron

import (
	"math"
	"sync/atomic"
)

// Publication wraps a log buffer for sending messages to a stream.
type Publication struct {
	conductor      *Conductor
	channel        string
	streamID       int32
	sessionID      int32
	registrationID int64
	logBuffers     *LogBuffers
	initialTermID  int32
	closed         atomic.Bool

	// posLimitCounterID identifies the publication-limit counter allocated
	// by the driver (from RespOnPublication). Its volatile int64 value in
	// counterValues is the max stream position we may claim up to.
	posLimitCounterID int32
	counterValues     *AtomicBuffer
}

func newPublication(conductor *Conductor, state *publicationState) *Publication {
	p := &Publication{
		conductor:         conductor,
		channel:           state.channel,
		streamID:          state.streamID,
		sessionID:         state.sessionID,
		registrationID:    state.registrationID,
		logBuffers:        state.logBuffers,
		posLimitCounterID: state.posLimitCounterID,
	}
	if state.logBuffers != nil {
		p.initialTermID = state.logBuffers.InitialTermID()
	}
	if conductor != nil && conductor.cnc != nil {
		p.counterValues = conductor.cnc.CounterValues
	}
	return p
}

// positionLimit reads the publication-limit counter (flow control window).
// If the counter is not available the limit is treated as unbounded.
func (p *Publication) positionLimit() int64 {
	if p.counterValues == nil || p.posLimitCounterID < 0 {
		return math.MaxInt64
	}
	return p.counterValues.GetInt64Volatile(p.posLimitCounterID * CounterValueLength)
}

// Offer sends a message to the publication's stream.
//
// Returns the new stream position on success, otherwise a negative error
// value:
//
//	-1 NOT_CONNECTED:  no active subscribers.
//	-2 BACK_PRESSURED: the position limit (flow control window) is reached;
//	   retry once subscribers have consumed.
//	-3 ADMIN_ACTION:   the log rotated to the next term (or a rotation by
//	   another publisher is in progress); retry the offer.
//	-4 CLOSED:         the publication is closed.
func (p *Publication) Offer(buf []byte) int64 {
	if p.closed.Load() || p.logBuffers == nil {
		return -4
	}
	if !p.logBuffers.IsConnected() {
		return -1
	}

	limit := p.positionLimit()
	meta := p.logBuffers.Meta()
	termCount := p.logBuffers.ActiveTermCount()
	index := int(termCount % PartitionCount)
	term := p.logBuffers.Term(index)
	termLen := term.Capacity()
	tailOff := int32(MetaTermTailCounterOff + index*8)

	rawTail := meta.GetInt64Volatile(tailOff)
	termID := rawTailTermID(rawTail)
	termOffset := rawTailTermOffset(rawTail, termLen)

	if termCount != termID-p.initialTermID {
		return -3 // Rotation in progress by another thread; retry.
	}

	position := computePosition(termID, termOffset, termLen, p.initialTermID)
	if position >= limit {
		return -2
	}

	frameLen := int32(DataFrameHeaderLen + len(buf))
	alignedLen := align(frameLen, DataFrameHeaderLen)

	// Claim space: atomically add to the tail, then work with the claimed
	// termID/termOffset from the returned raw tail.
	rawTail = meta.GetAndAddInt64(tailOff, int64(alignedLen))
	termID = rawTailTermID(rawTail)
	termOffset = rawTailTermOffset(rawTail, termLen)

	resultingOffset := termOffset + alignedLen
	if resultingOffset > termLen {
		return p.handleEndOfLog(term, termLen, termID, termOffset)
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

	return computePosition(termID, resultingOffset, termLen, p.initialTermID)
}

// handleEndOfLog deals with a claim that tripped the end of the term: the
// first thread to trip (termOffset still inside the term) writes a padding
// frame over the remainder, then the log is rotated to the next term.
// Always returns -3 (ADMIN_ACTION) so the caller retries the offer.
func (p *Publication) handleEndOfLog(term *AtomicBuffer, termLen, termID, termOffset int32) int64 {
	if termOffset < termLen {
		paddingLen := termLen - termOffset
		term.PutInt32(termOffset+FrameLengthOffset, 0)
		term.PutUint8(termOffset+FrameVersionOffset, 0)
		term.PutUint8(termOffset+FrameFlagsOffset, FlagUnfrag)
		term.PutInt32(termOffset+FrameTypeOffset, FrameTypePadding)
		term.PutInt32(termOffset+FrameTermOffsetOff, termOffset)
		term.PutInt32(termOffset+FrameSessionIDOff, p.sessionID)
		term.PutInt32(termOffset+FrameStreamIDOff, p.streamID)
		term.PutInt32(termOffset+FrameTermIDOff, termID)
		term.PutInt32Ordered(termOffset+FrameLengthOffset, paddingLen)
	}

	rotateLog(p.logBuffers.Meta(), termID-p.initialTermID, termID)

	return -3
}

// OfferWithRetry retries Offer up to maxRetries times while it returns a
// retryable status: -2 (back-pressured) or -3 (log rotated / admin action).
// Terminal statuses (success, -1 not connected, -4 closed) return immediately.
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

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
// value of NotConnected, BackPressured, AdminAction, Closed, or
// MaxPositionExceeded.
func (p *Publication) Offer(buf []byte) int64 {
	if p.closed.Load() || p.logBuffers == nil {
		return Closed
	}
	if p.conductor != nil && p.conductor.isTerminated() {
		return Closed
	}
	if !p.logBuffers.IsConnected() {
		return NotConnected
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
		return AdminAction // Rotation in progress by another thread; retry.
	}

	frameLen := int32(DataFrameHeaderLen + len(buf))
	alignedLen := align(frameLen, DataFrameHeaderLen)

	position := computePosition(termID, termOffset, termLen, p.initialTermID)
	if position >= limit {
		return p.backPressureStatus(position, alignedLen, termLen)
	}

	// Claim space: atomically add to the tail, then work with the claimed
	// termID/termOffset from the returned raw tail.
	rawTail = meta.GetAndAddInt64(tailOff, int64(alignedLen))
	termID = rawTailTermID(rawTail)
	termOffset = rawTailTermOffset(rawTail, termLen)

	resultingOffset := termOffset + alignedLen
	resultingPosition := computePosition(termID, resultingOffset, termLen, p.initialTermID)
	if resultingOffset > termLen {
		return p.handleEndOfLog(term, termLen, termID, termOffset, resultingPosition)
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

	return resultingPosition
}

// maxPossiblePosition is the maximum position a stream can reach given its
// term length: termLen << 31 (Java Publication: termBufferLength * (1L << 31),
// the term length times the total possible number of terms).
func maxPossiblePosition(termLen int32) int64 {
	return int64(termLen) << 31
}

// backPressureStatus resolves the status of an offer that reached the
// flow-control position limit, mirroring Java Publication.backPressureStatus:
// if appending the aligned frame would meet or exceed the stream's maximum
// possible position the publication can make no further progress
// (MaxPositionExceeded); otherwise it is BackPressured while subscribers are
// connected and NotConnected when they are not.
//
// alignedFrameLen is align(messageLength+DataFrameHeaderLen, FrameAlignment),
// which the caller has already computed.
func (p *Publication) backPressureStatus(currentPosition int64, alignedFrameLen, termLen int32) int64 {
	if currentPosition+int64(alignedFrameLen) >= maxPossiblePosition(termLen) {
		return MaxPositionExceeded
	}
	if p.logBuffers.IsConnected() {
		return BackPressured
	}
	return NotConnected
}

// handleEndOfLog deals with a claim that tripped the end of the term: the
// first thread to trip (termOffset still inside the term) writes a padding
// frame over the remainder. If the claimed position reached the stream's
// maximum possible position the publication is unrecoverable and
// MaxPositionExceeded is returned without rotating (mirrors Java
// ConcurrentPublication.handleEndOfLog); otherwise the log is rotated to the
// next term and AdminAction is returned so the caller retries the offer.
func (p *Publication) handleEndOfLog(term *AtomicBuffer, termLen, termID, termOffset int32, position int64) int64 {
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

	if position >= maxPossiblePosition(termLen) {
		return MaxPositionExceeded
	}

	rotateLog(p.logBuffers.Meta(), termID-p.initialTermID, termID)

	return AdminAction
}

// OfferWithRetry retries Offer up to maxRetries times while it returns a
// retryable status: BackPressured or AdminAction (log rotated). Terminal
// statuses (success, NotConnected, Closed, MaxPositionExceeded) return
// immediately.
func (p *Publication) OfferWithRetry(buf []byte, maxRetries int) int64 {
	for i := 0; i <= maxRetries; i++ {
		result := p.Offer(buf)
		if result > 0 || result == NotConnected || result == Closed || result == MaxPositionExceeded {
			return result
		}
	}
	return BackPressured
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

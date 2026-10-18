package aergo

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// Publication wraps a log buffer for sending messages to a stream.
//
// A publication from AddPublication may be offered to from any number of
// goroutines at once. One from AddExclusivePublication is faster, because it
// reserves space in the log without an atomic read-modify-write, but only one
// goroutine may call Offer, OfferParts or TryClaim on it at a time (Java's
// ExclusivePublication has the same rule); two at once would claim the same
// space and corrupt the log. Close, IsConnected and the accessors are safe from
// any goroutine.
type Publication struct {
	conductor      *Conductor
	state          *publicationState
	channel        string
	streamID       int32
	sessionID      int32
	registrationID int64
	logBuffers     *LogBuffers
	initialTermID  int32
	closed         atomic.Bool

	// exclusive: a single goroutine offers, so the tail needs no atomic add.
	exclusive bool

	// maxPayloadLength is the most payload one frame carries (MTU minus the
	// frame header); a longer message is fragmented. maxMessageLength is the
	// most a single offer accepts. Both are fixed for the life of the log.
	maxPayloadLength int32
	maxMessageLength int32

	// posLimitCounterID identifies the publication-limit counter allocated
	// by the driver (from RespOnPublication). Its volatile int64 value in
	// counterValues is the max stream position we may claim up to.
	posLimitCounterID int32
	counterValues     *AtomicBuffer
}

func newPublication(conductor *Conductor, state *publicationState) *Publication {
	p := &Publication{
		conductor:         conductor,
		state:             state,
		exclusive:         state.exclusive,
		channel:           state.channel,
		streamID:          state.streamID,
		sessionID:         state.sessionID,
		registrationID:    state.registrationID,
		logBuffers:        state.logBuffers,
		posLimitCounterID: state.posLimitCounterID,
	}
	if state.logBuffers != nil {
		p.initialTermID = state.logBuffers.InitialTermID()
		p.setLimits()
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

// setLimits derives the message limits from the log metadata, the same way
// Java's Publication constructor does.
func (p *Publication) setLimits() {
	mtu := p.logBuffers.Meta().GetInt32(MetaMtuLenOff)
	if mtu <= DataFrameHeaderLen || mtu%FrameAlignment != 0 {
		mtu = DefaultMtuLength
	}
	p.maxPayloadLength = mtu - DataFrameHeaderLen
	p.maxMessageLength = computeMaxMessageLength(p.logBuffers.TermLength())
}

// MaxPayloadLength is the longest message that fits in one frame: the MTU
// minus the frame header. It is also the longest TryClaim accepts. Offer
// fragments anything longer.
func (p *Publication) MaxPayloadLength() int { return int(p.maxPayloadLength) }

// MaxMessageLength is the longest message Offer accepts: an eighth of the
// term length, at most 16 MiB. Messages longer than MaxPayloadLength are sent
// as several fragments and must be reassembled by the subscriber (see
// FragmentAssembler).
func (p *Publication) MaxMessageLength() int { return int(p.maxMessageLength) }

// Offer sends a message to the publication's stream.
//
// A message longer than MaxPayloadLength is split into fragments, each in its
// own frame, which the subscriber reassembles; one longer than
// MaxMessageLength is rejected with a *MessageTooLongError (matching
// ErrMessageTooLong) and nothing is written.
//
// Otherwise the returned int64 is the new stream position on success, or a
// negative value of NotConnected, BackPressured, AdminAction, Closed, or
// MaxPositionExceeded, with a nil error. Those are conditions the caller
// retries or reacts to, so they are not errors; the error is for a message
// that can never be sent.
func (p *Publication) Offer(buf []byte) (int64, error) {
	if p.maxPayloadLength > 0 && len(buf) > int(p.maxPayloadLength) {
		return p.offerFragmented(buf, nil)
	}

	term, termOffset, status := p.claim(int32(len(buf)))
	if status <= 0 {
		return status, nil
	}

	term.PutBytes(termOffset+DataFrameHeaderLen, buf)
	term.PutInt32Ordered(termOffset+FrameLengthOffset, DataFrameHeaderLen+int32(len(buf)))

	return status, nil
}

// OfferParts sends first followed by second as one message, without the
// caller concatenating them. It is Java's offer(bufferOne, bufferTwo) and is
// how AeronCluster prefixes its session header to a payload with no extra
// copy. Results and errors are as for Offer.
func (p *Publication) OfferParts(first, second []byte) (int64, error) {
	length := len(first) + len(second)
	if p.maxPayloadLength > 0 && length > int(p.maxPayloadLength) {
		return p.offerFragmented(first, second)
	}

	term, termOffset, status := p.claim(int32(length))
	if status <= 0 {
		return status, nil
	}

	term.PutBytes(termOffset+DataFrameHeaderLen, first)
	term.PutBytes(termOffset+DataFrameHeaderLen+int32(len(first)), second)
	term.PutInt32Ordered(termOffset+FrameLengthOffset, DataFrameHeaderLen+int32(length))

	return status, nil
}

// offerFragmented sends first+second, which is longer than one frame can
// carry, as a run of frames. As in Java's TermAppender.appendFragmentedMessage
// the space for every fragment is claimed in one step, so the fragments are
// contiguous (a message that does not fit in the rest of the term pads it out
// and rotates, exactly as a single frame would), and each fragment is
// published as soon as it is written: the first carries FlagBeginFrag, the
// last FlagEndFrag, those between neither.
func (p *Publication) offerFragmented(first, second []byte) (int64, error) {
	length := len(first) + len(second)
	if length > int(p.maxMessageLength) {
		return 0, &MessageTooLongError{Length: length, Max: int(p.maxMessageLength)}
	}

	required := computeFragmentedFrameLength(int32(length), p.maxPayloadLength)
	term, termOffset, termID, status := p.claimSpace(required)
	if status <= 0 {
		return status, nil
	}

	var (
		flags     = byte(FlagBeginFrag)
		remaining = int32(length)
		consumed  = int32(0) // bytes of first+second already written
		offset    = termOffset
	)
	for remaining > 0 {
		chunk := min(remaining, p.maxPayloadLength)
		if chunk == remaining {
			flags |= FlagEndFrag
		}

		p.putFrameHeader(term, offset, termID, flags)
		copyParts(term.Slice(offset+DataFrameHeaderLen, chunk), first, second, int(consumed))
		term.PutInt32Ordered(offset+FrameLengthOffset, DataFrameHeaderLen+chunk)

		offset += align(DataFrameHeaderLen+chunk, FrameAlignment)
		consumed += chunk
		remaining -= chunk
		flags = 0
	}

	return status, nil
}

// copyParts fills dst with the bytes of first+second starting at from.
func copyParts(dst, first, second []byte, from int) {
	n := 0
	if from < len(first) {
		n = copy(dst, first[from:])
		from = len(first)
	}
	if n < len(dst) {
		copy(dst[n:], second[from-len(first):])
	}
}

// TryClaim reserves length bytes of frame payload space in the log buffer
// and returns a BufferClaim wrapping that region directly (no allocation, no
// intermediate copy) for the caller to write message content into. The
// caller must call Commit or Abort on the claim exactly once - an
// uncommitted, unaborted claim is never visible to subscribers and stalls
// the stream, since consumers can't skip past it.
//
// A claim is one frame, so length may not exceed MaxPayloadLength: a longer
// one is rejected with a *MessageTooLongError (Claim set), as is a negative
// one with a plain error; Java throws IllegalArgumentException for both.
//
// Returns the claim and the resulting stream position on success (mirroring
// Offer's positive/negative convention), or the zero BufferClaim and a
// negative status (NotConnected, BackPressured, AdminAction, Closed,
// MaxPositionExceeded) on failure.
func (p *Publication) TryClaim(length int) (BufferClaim, int64, error) {
	if length < 0 {
		return BufferClaim{}, 0, fmt.Errorf("invalid length: %d", length)
	}
	if p.maxPayloadLength > 0 && length > int(p.maxPayloadLength) {
		return BufferClaim{}, 0, &MessageTooLongError{Length: length, Max: int(p.maxPayloadLength), Claim: true}
	}

	term, termOffset, status := p.claim(int32(length))
	if status <= 0 {
		return BufferClaim{}, status, nil
	}
	return BufferClaim{term: term, offset: termOffset, length: int32(length)}, status, nil
}

// claim reserves one unfragmented frame of length payload bytes and writes
// every header field except the length field itself (the ordered store that
// publishes the frame to consumers - left to the caller, since it must
// happen only after the payload is written). Shared by Offer and TryClaim so
// both use identical position-limit and end-of-log-rotation handling.
//
// On success returns the claimed term buffer, the claimed offset within it,
// and the resulting stream position (> 0). On failure returns a negative
// status matching Offer's error convention.
func (p *Publication) claim(length int32) (*AtomicBuffer, int32, int64) {
	term, termOffset, termID, status := p.claimSpace(align(DataFrameHeaderLen+length, FrameAlignment))
	if status <= 0 {
		return nil, 0, status
	}
	p.putFrameHeader(term, termOffset, termID, FlagUnfrag)
	return term, termOffset, status
}

// claimSpace reserves alignedLen bytes in the current term, which the caller
// fills with one or more frames. It handles the position limit and the end
// of the term (padding and rotation) for everything that appends.
//
// On success returns the term, the offset claimed in it, the term ID, and
// the resulting stream position (> 0). On failure returns a negative status.
func (p *Publication) claimSpace(alignedLen int32) (*AtomicBuffer, int32, int32, int64) {
	if p.closed.Load() || p.logBuffers == nil {
		return nil, 0, 0, Closed
	}
	if p.conductor != nil && p.conductor.isTerminated() {
		return nil, 0, 0, Closed
	}
	if !p.logBuffers.IsConnected() {
		return nil, 0, 0, NotConnected
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
		return nil, 0, 0, AdminAction // Rotation in progress by another thread; retry.
	}

	position := computePosition(termID, termOffset, termLen, p.initialTermID)
	if position >= limit {
		return nil, 0, 0, p.backPressureStatus(position, alignedLen, termLen)
	}

	// Claim space, then work with the claimed termID/termOffset from the raw
	// tail as it was before the claim.
	if p.exclusive {
		// Only this goroutine moves the tail, so the value read above is
		// current: store the new one with release semantics. No atomic
		// read-modify-write (a LOCK XADD on amd64), which would wait for the
		// previous message's stores to drain. This is what Java's
		// ExclusivePublication does.
		meta.PutInt64Ordered(tailOff, rawTail+int64(alignedLen))
	} else {
		// Shared: other goroutines may claim at the same moment, so add atomically.
		rawTail = meta.GetAndAddInt64(tailOff, int64(alignedLen))
	}
	termID = rawTailTermID(rawTail)
	termOffset = rawTailTermOffset(rawTail, termLen)

	resultingOffset := termOffset + alignedLen
	resultingPosition := computePosition(termID, resultingOffset, termLen, p.initialTermID)
	if resultingOffset > termLen {
		return nil, 0, 0, p.handleEndOfLog(term, termLen, termID, termOffset, resultingPosition)
	}

	return term, termOffset, termID, resultingPosition
}

// putFrameHeader writes every data frame header field except the length,
// which the caller publishes last with an ordered store. The length is left
// zero (uncommitted).
//
// The 32-byte header is written as three aligned 64-bit stores, as Java's
// HeaderWriter does, not field by field. A subscriber polling for the next
// frame reads the cache line the header is being written into, and every
// store that lands after it has taken the line is a miss for the publisher:
// eight small stores instead of three cost about 40% of 4000 byte throughput
// when the subscriber keeps up. The log is little-endian, so the fields are
// packed as they lie in memory. The reserved value is left as the driver
// zeroed it.
func (p *Publication) putFrameHeader(term *AtomicBuffer, offset, termID int32, flags byte) {
	// bytes 0..7: frame length (0), version (0), flags, type. This word holds
	// the frame length a subscriber polls with an atomic load, so it is stored
	// with release semantics, as Java's HeaderWriter does; a plain store would
	// be a data race with that load.
	term.PutInt64Ordered(offset+FrameLengthOffset, int64(flags)<<(8*FrameFlagsOffset)|int64(FrameTypeData)<<(8*FrameTypeOffset))
	// bytes 8..15: term offset, session ID
	term.PutInt64(offset+FrameTermOffsetOff, int64(uint32(offset))|int64(uint32(p.sessionID))<<32)
	// bytes 16..23: stream ID, term ID
	term.PutInt64(offset+FrameStreamIDOff, int64(uint32(p.streamID))|int64(uint32(termID))<<32)
}

// BufferClaim is a claimed, not-yet-published region of a log buffer term,
// obtained via Publication.TryClaim. Write message content into Buffer()
// directly - it aliases the term buffer's own memory, so there is no
// allocation or copy - then call Commit or Abort exactly once.
type BufferClaim struct {
	term   *AtomicBuffer
	offset int32 // start of the frame header; data starts at offset+DataFrameHeaderLen
	length int32 // length of the claimed data region, excluding the frame header
}

// Buffer returns the claimed data region for the caller to write message
// content into directly. The returned slice aliases the term buffer's
// memory and is exactly the length passed to TryClaim; it must not be used
// after Commit or Abort is called.
func (c BufferClaim) Buffer() []byte {
	return c.term.Slice(c.offset+DataFrameHeaderLen, c.length)
}

// Commit publishes the claimed frame, making it visible to subscribers.
// Must be called exactly once per claim, after the caller has finished
// writing into Buffer().
func (c BufferClaim) Commit() {
	c.term.PutInt32Ordered(c.offset+FrameLengthOffset, DataFrameHeaderLen+c.length)
}

// Abort marks the claimed region as padding instead of publishing it, so
// subscribers skip over it. Use when the caller decides not to publish
// after already claiming space (e.g. an error building the message).
func (c BufferClaim) Abort() {
	c.term.PutInt16(c.offset+FrameTypeOffset, FrameTypePadding)
	c.term.PutInt32Ordered(c.offset+FrameLengthOffset, DataFrameHeaderLen+c.length)
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
		term.PutInt32Ordered(termOffset+FrameLengthOffset, 0) // read atomically by Poll
		term.PutUint8(termOffset+FrameVersionOffset, 0)
		term.PutUint8(termOffset+FrameFlagsOffset, FlagUnfrag)
		term.PutInt16(termOffset+FrameTypeOffset, FrameTypePadding)
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
func (p *Publication) OfferWithRetry(buf []byte, maxRetries int) (int64, error) {
	for i := 0; i <= maxRetries; i++ {
		result, err := p.Offer(buf)
		if err != nil || result > 0 || result == NotConnected || result == Closed || result == MaxPositionExceeded {
			return result, err
		}
	}
	return BackPressured, nil
}

// OfferWithBackoff offers buf, retrying while the result is OfferRetryable
// until timeout elapses or ctx ends, and returns the last result.
//
// OfferWithRetry spins without pausing, which is too short to ride out the
// gap after a publication first connects: the log reports connected when the
// driver's sender gets the first status message, but the driver raises the
// publication limit on a later duty cycle. An offer in that gap sees position
// 0 against limit 0 and returns BackPressured even though nothing is wrong.
// This sleeps a millisecond between attempts instead.
func (p *Publication) OfferWithBackoff(ctx context.Context, buf []byte, timeout time.Duration) (int64, error) {
	deadline := time.Now().Add(timeout)
	for {
		result, err := p.Offer(buf)
		if err != nil || !OfferRetryable(result) || !time.Now().Before(deadline) || ctx.Err() != nil {
			return result, err
		}
		time.Sleep(time.Millisecond)
	}
}

// IsConnected returns whether the publication has active subscribers.
func (p *Publication) IsConnected() bool {
	// A closed publication's log is unmapped after the linger, so it must not
	// be read.
	return !p.closed.Load() && p.logBuffers != nil && p.logBuffers.IsConnected()
}

// StreamID returns the stream identifier.
func (p *Publication) StreamID() int32 { return p.streamID }

// SessionID returns the session identifier.
func (p *Publication) SessionID() int32 { return p.sessionID }

// Close releases the publication.
func (p *Publication) Close() {
	if p.closed.CompareAndSwap(false, true) {
		// A loopback publication has no driver to tell.
		if p.conductor != nil && p.conductor.proxy != nil {
			p.conductor.proxy.RemovePublication(p.registrationID)
			p.conductor.lingerPublicationLog(p.state)
			p.conductor.forgetPublication(p.state)
		}
	}
}

func computePosition(termID, termOffset, termLen, initialTermID int32) int64 {
	shift := numberOfTrailingZeros(uint32(termLen))
	return int64(termID-initialTermID)<<shift + int64(termOffset)
}

func numberOfTrailingZeros(v uint32) int {
	return bits.TrailingZeros32(v)
}

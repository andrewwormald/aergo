package aeron

import (
	"log"
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
	posLimit       int32
	closed         atomic.Bool
	loggedMetaDiag bool
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
// Returns the new stream position (>0), or a negative error code:
//
//	-1 = NOT_CONNECTED
//	-2 = BACK_PRESSURED (term full, caller should retry)
//	-4 = CLOSED
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

	// Claim space atomically
	tailOff := int32(MetaTermTailCounterOff + partIndex*8)
	rawTail := p.logBuffers.Meta().GetAndAddInt64(tailOff, int64(alignedLen))
	termOffset := int32(rawTail) // low 32 bits

	if termOffset+alignedLen > termLen {
		return -2 // term full -- media driver handles rotation
	}

	// Write data frame header
	term.PutInt32(termOffset+FrameLengthOffset, 0) // uncommitted
	term.PutUint8(termOffset+FrameVersionOffset, 0)
	term.PutUint8(termOffset+FrameFlagsOffset, FlagUnfrag)
	term.PutInt32(termOffset+FrameTypeOffset, FrameTypeData)
	term.PutInt32(termOffset+FrameTermOffsetOff, termOffset)
	term.PutInt32(termOffset+FrameSessionIDOff, p.sessionID)
	term.PutInt32(termOffset+FrameStreamIDOff, p.streamID)
	term.PutInt32(termOffset+FrameTermIDOff, termID)

	// Write payload
	term.PutBytes(termOffset+DataFrameHeaderLen, buf)

	// Commit: ordered store makes payload visible to readers
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
		// -2 = back pressure, retry
	}
	return -2
}

// IsConnected returns whether the publication has active subscribers.
func (p *Publication) IsConnected() bool {
	if p.logBuffers == nil {
		return false
	}
	meta := p.logBuffers.Meta()
	raw := meta.GetInt32Volatile(MetaIsConnectedOff)
	if raw != 0 && raw != 1 {
		// Log once if we see an unexpected value (metadata offset likely wrong)
		if !p.loggedMetaDiag {
			p.loggedMetaDiag = true
			log.Printf("pub: IsConnected raw=%d at offset=%d, meta capacity=%d, termLen=%d, fileSize=%d+%d",
				raw, MetaIsConnectedOff, meta.Capacity(), p.logBuffers.TermLength(),
				int64(p.logBuffers.TermLength())*PartitionCount, LogMetaDataLength)
		}
	}
	return raw == 1
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

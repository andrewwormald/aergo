package aeron

import "sync/atomic"

// Publication wraps a log buffer for sending messages to a stream.
type Publication struct {
	conductor     *Conductor
	channel       string
	streamID      int32
	sessionID     int32
	registrationID int64
	logBuffers    *LogBuffers
	appender      *TermAppender
	initialTermID int32
	posLimit      int32 // counter ID
	closed        atomic.Bool
}

// newPublication creates a publication from a ready publicationState.
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
		activeIndex := state.logBuffers.ActiveTermCount() % PartitionCount
		p.appender = NewTermAppender(state.logBuffers, int(activeIndex))
	}
	return p
}

// Offer sends a message to the publication's stream.
// Returns the new stream position (>0), or a negative error code.
func (p *Publication) Offer(buf []byte) int64 {
	if p.closed.Load() || p.logBuffers == nil {
		return -4 // CLOSED
	}
	if !p.logBuffers.IsConnected() {
		return -1 // NOT_CONNECTED
	}

	activeCount := p.logBuffers.ActiveTermCount()
	partIndex := int(activeCount % PartitionCount)
	termID := p.initialTermID + activeCount

	// Re-create appender for current active term if needed
	if p.appender == nil || p.appender.partIndex != partIndex {
		p.appender = NewTermAppender(p.logBuffers, partIndex)
	}

	return p.appender.Append(termID, p.sessionID, p.streamID, buf)
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

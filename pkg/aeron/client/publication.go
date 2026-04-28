package client

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/andrewwormald/aergo/internal/unsafeutil"
	"github.com/andrewwormald/aergo/pkg/aeron/driver"
)

// ---------------------------------------------------------------------------
// BufferClaim -- zero-copy write directly into the Aeron log buffer.
//
// C struct (24 bytes, no padding on 64-bit):
//
//	typedef struct aeron_buffer_claim_stct {
//	    uint8_t *frame_header;  // offset 0, 8 bytes
//	    uint8_t *data;          // offset 8, 8 bytes
//	    size_t   length;        // offset 16, 8 bytes
//	} aeron_buffer_claim_t;
// ---------------------------------------------------------------------------

// BufferClaim provides zero-copy access to a region of the Aeron log buffer.
// Write your message into Buffer(), then call Commit() or Abort().
type BufferClaim struct {
	claim [24]byte // inline storage matching aeron_buffer_claim_t
}

// Buffer returns the writable region of the claimed log buffer.
// Write your message here, then call Commit(). Do not retain past Commit/Abort.
func (bc *BufferClaim) Buffer() []byte {
	dataPtr := *(*unsafe.Pointer)(unsafe.Pointer(&bc.claim[8]))
	length := *(*uintptr)(unsafe.Pointer(&bc.claim[16]))
	return unsafe.Slice((*byte)(dataPtr), int(length))
}

// Length returns the claimed buffer length.
func (bc *BufferClaim) Length() int {
	return int(*(*uintptr)(unsafe.Pointer(&bc.claim[16])))
}

// Commit publishes the claimed buffer to subscribers.
func (bc *BufferClaim) Commit() error {
	return driver.BufferClaimCommit(unsafe.Pointer(&bc.claim[0]))
}

// Abort discards the claimed buffer without publishing.
func (bc *BufferClaim) Abort() error {
	return driver.BufferClaimAbort(unsafe.Pointer(&bc.claim[0]))
}

// ---------------------------------------------------------------------------
// Backpressure strategies
// ---------------------------------------------------------------------------

// BackpressureStrategy defines how to handle back pressure from Offer/TryClaim.
type BackpressureStrategy int

const (
	// BackpressureSpin retries immediately in a tight loop (lowest latency, highest CPU).
	BackpressureSpin BackpressureStrategy = iota
	// BackpressureYield calls runtime.Gosched() between retries.
	BackpressureYield
	// BackpressureNone returns immediately on back pressure (caller handles).
	BackpressureNone
)

// ---------------------------------------------------------------------------
// Publication
// ---------------------------------------------------------------------------

// Publication wraps an Aeron publication for sending messages.
type Publication struct {
	ptr      unsafe.Pointer
	streamId int32
	uri      string
	closed   bool
}

// Offer sends a message buffer to the publication.
// Returns the new stream position on success, or a negative value:
//   - NotConnected (-1): no subscribers connected
//   - BackPressured (-2): offer failed due to back pressure
//   - AdminAction (-3): admin action (e.g., log rotation) interrupted
//   - PublicationClosed (-4): publication has been closed
//   - MaxPositionExceeded (-5): max position exceeded
func (p *Publication) Offer(buf []byte) int64 {
	if p.closed {
		return driver.PublicationClosed
	}
	ptr := unsafeutil.BytesToPointer(buf)
	return driver.PublicationOffer(p.ptr, ptr, uintptr(len(buf)))
}

// OfferWithBackpressure sends a message with the specified backpressure strategy.
// Retries on BackPressured and AdminAction. Returns position or fatal error code.
func (p *Publication) OfferWithBackpressure(buf []byte, strategy BackpressureStrategy, maxRetries int) int64 {
	for i := 0; ; i++ {
		result := p.Offer(buf)
		if result > 0 || result == driver.NotConnected || result == driver.PublicationClosed || result == driver.MaxPositionExceeded {
			return result
		}
		// BackPressured or AdminAction -- retry
		if strategy == BackpressureNone || (maxRetries > 0 && i >= maxRetries) {
			return result
		}
		if strategy == BackpressureYield {
			runtime.Gosched()
		}
	}
}

// TryClaim attempts to claim a region of the log buffer for zero-copy writing.
// On success, write into claim.Buffer() and call claim.Commit().
// Returns the new stream position on success, or a negative error code.
func (p *Publication) TryClaim(length int) (*BufferClaim, int64) {
	if p.closed {
		return nil, driver.PublicationClosed
	}
	claim := &BufferClaim{}
	result := driver.PublicationTryClaim(p.ptr, uintptr(length), unsafe.Pointer(&claim.claim[0]))
	if result < 0 {
		return nil, result
	}
	return claim, result
}

// IsConnected returns true if there are active subscribers.
func (p *Publication) IsConnected() bool {
	return driver.PublicationIsConnected(p.ptr)
}

// IsClosed returns true if the publication has been closed.
func (p *Publication) IsClosed() bool {
	return p.closed || driver.PublicationIsClosed(p.ptr)
}

// StreamId returns the stream ID of this publication.
func (p *Publication) StreamId() int32 {
	return p.streamId
}

// SessionId returns the session ID of this publication.
func (p *Publication) SessionId() int32 {
	return driver.PublicationSessionId(p.ptr)
}

// ChannelStatus returns the channel status.
func (p *Publication) ChannelStatus() int64 {
	return driver.PublicationChannelStatus(p.ptr)
}

// Close releases the publication.
func (p *Publication) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	return driver.PublicationClose(p.ptr)
}

// String returns a human-readable description.
func (p *Publication) String() string {
	return fmt.Sprintf("Publication{uri=%s, stream=%d}", p.uri, p.streamId)
}

// ---------------------------------------------------------------------------
// ExclusivePublication
// ---------------------------------------------------------------------------

// ExclusivePublication wraps an Aeron exclusive publication.
// Exclusive publications provide better throughput for single-writer scenarios
// by avoiding compare-and-set on the tail position.
type ExclusivePublication struct {
	ptr      unsafe.Pointer
	streamId int32
	uri      string
	closed   bool
}

// Offer sends a message buffer to the exclusive publication.
func (p *ExclusivePublication) Offer(buf []byte) int64 {
	if p.closed {
		return driver.PublicationClosed
	}
	ptr := unsafeutil.BytesToPointer(buf)
	return driver.ExclusivePublicationOffer(p.ptr, ptr, uintptr(len(buf)))
}

// TryClaim attempts to claim a region of the log buffer for zero-copy writing.
func (p *ExclusivePublication) TryClaim(length int) (*BufferClaim, int64) {
	if p.closed {
		return nil, driver.PublicationClosed
	}
	claim := &BufferClaim{}
	result := driver.ExclusivePublicationTryClaim(p.ptr, uintptr(length), unsafe.Pointer(&claim.claim[0]))
	if result < 0 {
		return nil, result
	}
	return claim, result
}

// IsConnected returns true if there are active subscribers.
func (p *ExclusivePublication) IsConnected() bool {
	return driver.ExclusivePublicationIsConnected(p.ptr)
}

// IsClosed returns true if the publication has been closed.
func (p *ExclusivePublication) IsClosed() bool {
	return p.closed || driver.ExclusivePublicationIsClosed(p.ptr)
}

// Close releases the exclusive publication.
func (p *ExclusivePublication) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	return driver.ExclusivePublicationClose(p.ptr)
}

// ---------------------------------------------------------------------------
// Async publication resolution
// ---------------------------------------------------------------------------

func awaitPublication(async unsafe.Pointer) (*Publication, error) {
	var pub unsafe.Pointer
	for {
		result := driver.AsyncAddPublicationPoll(&pub, async)
		if result == 1 {
			return &Publication{ptr: pub}, nil
		}
		if result < 0 {
			return nil, errors.New("failed to add publication")
		}
	}
}

func awaitExclusivePublication(async unsafe.Pointer) (*ExclusivePublication, error) {
	var pub unsafe.Pointer
	for {
		result := driver.AsyncAddExclusivePublicationPoll(&pub, async)
		if result == 1 {
			return &ExclusivePublication{ptr: pub}, nil
		}
		if result < 0 {
			return nil, errors.New("failed to add exclusive publication")
		}
	}
}

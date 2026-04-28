package client

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/andrewwormald/aergo/pkg/aeron/driver"
)

// Subscription wraps an Aeron subscription for receiving messages.
type Subscription struct {
	ptr       unsafe.Pointer
	streamId  int32
	uri       string
	closed    bool

	// Bound handler for zero-alloc polling.
	// Set via Bind(), used by PollBound().
	boundId  uintptr
	hasBound bool
}

// Poll polls for new messages, invoking handler for each fragment up to fragmentLimit.
// Returns the number of fragments received.
// Note: this registers/unregisters the handler per call. For zero-alloc hot-path
// polling, use Bind() + PollBound() instead.
func (s *Subscription) Poll(handler FragmentHandler, fragmentLimit int) int {
	if s.closed {
		return 0
	}

	id := registry.Register(handler)
	defer registry.Unregister(id)

	result := driver.SubscriptionPoll(
		s.ptr,
		FragmentHandlerCallback(),
		uintptrToClientd(id),
		int32(fragmentLimit),
	)
	return int(result)
}

// Bind registers a fragment handler for this subscription. The handler stays
// registered until Unbind() or Close() is called. Use with PollBound() for
// zero-alloc polling on the hot path.
func (s *Subscription) Bind(handler FragmentHandler) {
	if s.hasBound {
		registry.Unregister(s.boundId)
	}
	s.boundId = registry.Register(handler)
	s.hasBound = true
}

// Unbind removes the bound handler.
func (s *Subscription) Unbind() {
	if s.hasBound {
		registry.Unregister(s.boundId)
		s.hasBound = false
	}
}

// PollBound polls using the previously bound handler. Zero allocations.
// Returns the number of fragments received.
func (s *Subscription) PollBound(fragmentLimit int) int {
	if s.closed || !s.hasBound {
		return 0
	}

	result := driver.SubscriptionPoll(
		s.ptr,
		FragmentHandlerCallback(),
		uintptrToClientd(s.boundId),
		int32(fragmentLimit),
	)
	return int(result)
}

// PollWithAssembler polls using a fragment assembler for multi-fragment messages.
func (s *Subscription) PollWithAssembler(assembler unsafe.Pointer, fragmentLimit int) int {
	if s.closed {
		return 0
	}

	if driver.FragmentAssemblerHandlerPtr == 0 {
		return 0
	}

	result := driver.SubscriptionPoll(
		s.ptr,
		driver.FragmentAssemblerHandlerPtr,
		assembler,
		int32(fragmentLimit),
	)
	return int(result)
}

// IsConnected returns true if there are active publishers.
func (s *Subscription) IsConnected() bool {
	return driver.SubscriptionIsConnected(s.ptr)
}

// IsClosed returns true if the subscription has been closed.
func (s *Subscription) IsClosed() bool {
	return s.closed || driver.SubscriptionIsClosed(s.ptr)
}

// StreamId returns the stream ID.
func (s *Subscription) StreamId() int32 {
	return s.streamId
}

// ChannelStatus returns the channel status.
func (s *Subscription) ChannelStatus() int64 {
	return driver.SubscriptionChannelStatus(s.ptr)
}

// Close releases the subscription.
func (s *Subscription) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.Unbind()
	return driver.SubscriptionClose(s.ptr)
}

// String returns a human-readable description.
func (s *Subscription) String() string {
	return fmt.Sprintf("Subscription{uri=%s, stream=%d}", s.uri, s.streamId)
}

func awaitSubscription(async unsafe.Pointer) (*Subscription, error) {
	var sub unsafe.Pointer
	for {
		result := driver.AsyncAddSubscriptionPoll(&sub, async)
		if result == 1 {
			return &Subscription{ptr: sub}, nil
		}
		if result < 0 {
			return nil, errors.New("failed to add subscription")
		}
	}
}

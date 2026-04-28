package client

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ebitengine/purego"
)

// FragmentHandler is called for each message fragment received.
// buffer is a view into the Aeron log buffer -- do not retain past the call.
type FragmentHandler func(buffer []byte, header *Header)

// Header contains metadata from the Aeron data frame header.
type Header struct {
	Position            int64
	SessionId           int32
	StreamId            int32
	TermId              int32
	TermOffset          int32
	FrameLength         int32
	InitialTermId       int32
	PositionBitsToShift int32
	Flags               uint8
}

// IsBeginFragment returns true if this is the first fragment of a message.
func (h *Header) IsBeginFragment() bool { return h != nil && h.Flags&0x80 != 0 }

// IsEndFragment returns true if this is the last fragment of a message.
func (h *Header) IsEndFragment() bool { return h != nil && h.Flags&0x40 != 0 }

// IsUnfragmented returns true if this message is not fragmented.
func (h *Header) IsUnfragmented() bool { return h != nil && h.Flags&0xC0 == 0xC0 }

// ---------------------------------------------------------------------------
// Fixed-index callback registry -- zero-alloc hot path dispatch.
//
// Instead of a map[uintptr]FragmentHandler with mutex, we use a fixed-size
// array indexed by slot ID. The C clientd carries the slot index directly.
// No lock needed on the read path because slots are written before the
// C callback can fire, and cleared after the C callback returns.
// ---------------------------------------------------------------------------

const maxCallbackSlots = 256

type callbackSlot struct {
	handler FragmentHandler
	active  atomic.Bool
}

type callbackRegistry struct {
	slots  [maxCallbackSlots]callbackSlot
	nextId atomic.Uint64
}

var registry = &callbackRegistry{}

// Register adds a Go handler to the next available slot.
// Returns the slot index for use as clientd.
func (r *callbackRegistry) Register(handler FragmentHandler) uintptr {
	id := r.nextId.Add(1)
	idx := id % maxCallbackSlots
	r.slots[idx].handler = handler
	r.slots[idx].active.Store(true)
	return uintptr(idx)
}

// Unregister clears a slot.
func (r *callbackRegistry) Unregister(id uintptr) {
	if id < maxCallbackSlots {
		r.slots[id].active.Store(false)
		r.slots[id].handler = nil
	}
}

// Lookup returns the handler at a given slot index. No lock, no map.
func (r *callbackRegistry) Lookup(id uintptr) (FragmentHandler, bool) {
	if id >= maxCallbackSlots {
		return nil, false
	}
	if !r.slots[id].active.Load() {
		return nil, false
	}
	return r.slots[id].handler, true
}

// ---------------------------------------------------------------------------
// C callback trampolines -- allocated once via purego.NewCallback.
// These are the actual function pointers passed to the C library.
// ---------------------------------------------------------------------------

// fragmentHandlerCCallback is the single C-level fragment handler.
//
// C signature:
//
//	typedef void (*aeron_fragment_handler_t)(
//	    void *clientd,
//	    const uint8_t *buffer,
//	    size_t length,
//	    aeron_header_t *header);
var fragmentHandlerCCallback uintptr

func init() {
	fragmentHandlerCCallback = purego.NewCallback(func(clientd unsafe.Pointer, buffer unsafe.Pointer, length uintptr, cHeader unsafe.Pointer) {
		id := uintptr(clientd)
		h, ok := registry.Lookup(id)
		if !ok {
			return
		}
		buf := unsafe.Slice((*byte)(buffer), int(length))
		hdr := parseHeader(cHeader)
		h(buf, hdr)
	})
}

// parseHeader extracts fields from the C aeron_header_t struct.
//
// aeron_header_t layout (arm64/amd64, 40 bytes):
//
//	offset  0: *aeron_data_header_t frame     (8 bytes, pointer)
//	offset  8: int32_t              initial_term_id (4 bytes)
//	offset 12: [4 bytes padding]
//	offset 16: size_t               position_bits_to_shift (8 bytes)
//	offset 24: int32_t              fragmented_frame_length (4 bytes)
//	offset 28: [4 bytes padding]
//	offset 32: void*                context (8 bytes)
//
// aeron_data_header_t layout (32 bytes):
//
//	offset  0: int32_t  frame_length (4 bytes)
//	offset  4: int8_t   version      (1 byte)
//	offset  5: uint8_t  flags        (1 byte)
//	offset  6: int16_t  type         (2 bytes)
//	offset  8: int32_t  term_offset  (4 bytes)
//	offset 12: int32_t  session_id   (4 bytes)
//	offset 16: int32_t  stream_id    (4 bytes)
//	offset 20: int32_t  term_id      (4 bytes)
//	offset 24: int64_t  reserved_value (8 bytes)
func parseHeader(cHeader unsafe.Pointer) *Header {
	if cHeader == nil {
		return nil
	}

	// Read fields from aeron_header_t
	framePtr := *(*unsafe.Pointer)(cHeader)                                         // offset 0
	initialTermId := *(*int32)(unsafe.Add(cHeader, 8))                              // offset 8
	positionBitsToShift := *(*uintptr)(unsafe.Add(cHeader, 16))                     // offset 16
	fragmentedFrameLength := *(*int32)(unsafe.Add(cHeader, 24))                     // offset 24

	if framePtr == nil {
		return &Header{
			InitialTermId:       initialTermId,
			PositionBitsToShift: int32(positionBitsToShift),
		}
	}

	// Read fields from aeron_data_header_t (pointed to by frame)
	frameLength := *(*int32)(framePtr)                                              // offset 0
	flags := *(*uint8)(unsafe.Add(framePtr, 5))                                     // offset 5
	termOffset := *(*int32)(unsafe.Add(framePtr, 8))                                // offset 8
	sessionId := *(*int32)(unsafe.Add(framePtr, 12))                                // offset 12
	streamId := *(*int32)(unsafe.Add(framePtr, 16))                                 // offset 16
	termId := *(*int32)(unsafe.Add(framePtr, 20))                                   // offset 20

	// Compute position from term_id, term_offset, initial_term_id, position_bits_to_shift
	activeTermCount := int64(termId) - int64(initialTermId)
	termLength := int64(1) << positionBitsToShift
	position := activeTermCount*termLength + int64(termOffset) + int64(frameLength)

	_ = fragmentedFrameLength

	return &Header{
		Position:            position,
		SessionId:           sessionId,
		StreamId:            streamId,
		TermId:              termId,
		TermOffset:          termOffset,
		FrameLength:         frameLength,
		InitialTermId:       initialTermId,
		PositionBitsToShift: int32(positionBitsToShift),
		Flags:               flags,
	}
}

// FragmentHandlerCallback returns the single C callback pointer for fragment handling.
func FragmentHandlerCallback() uintptr {
	return fragmentHandlerCCallback
}

// ---------------------------------------------------------------------------
// Image callbacks
// ---------------------------------------------------------------------------

type ImageHandler func(image unsafe.Pointer)

var (
	imageAvailableCCallback   uintptr
	imageUnavailableCCallback uintptr

	onImageAvailable   ImageHandler
	onImageUnavailable ImageHandler
	imageMu            sync.Mutex
)

func init() {
	imageAvailableCCallback = purego.NewCallback(func(clientd unsafe.Pointer, image unsafe.Pointer) {
		imageMu.Lock()
		fn := onImageAvailable
		imageMu.Unlock()
		if fn != nil {
			fn(image)
		}
	})
	imageUnavailableCCallback = purego.NewCallback(func(clientd unsafe.Pointer, image unsafe.Pointer) {
		imageMu.Lock()
		fn := onImageUnavailable
		imageMu.Unlock()
		if fn != nil {
			fn(image)
		}
	})
}

// SetImageHandlers sets callbacks for image availability changes.
func SetImageHandlers(available, unavailable ImageHandler) {
	imageMu.Lock()
	onImageAvailable = available
	onImageUnavailable = unavailable
	imageMu.Unlock()
}

// ---------------------------------------------------------------------------
// Error handler callback
// ---------------------------------------------------------------------------

var (
	errorHandlerCCallback uintptr
	errorHandlerFn        func(errcode int32, message string)
	errorHandlerMu        sync.Mutex
)

func init() {
	errorHandlerCCallback = purego.NewCallback(func(clientd unsafe.Pointer, errcode int32, message unsafe.Pointer) {
		errorHandlerMu.Lock()
		fn := errorHandlerFn
		errorHandlerMu.Unlock()
		if fn != nil {
			fn(errcode, goStringFromCPtr(message))
		}
	})
}

// goStringFromCPtr reads a null-terminated C string from a pointer.
func goStringFromCPtr(ptr unsafe.Pointer) string {
	if ptr == nil {
		return ""
	}
	// Walk forward until we find a null byte
	var length int
	for {
		b := *(*byte)(unsafe.Add(ptr, length))
		if b == 0 {
			break
		}
		length++
		if length > 4096 { // safety cap
			break
		}
	}
	if length == 0 {
		return ""
	}
	return string(unsafe.Slice((*byte)(ptr), length))
}

// SetErrorHandler sets the Go-level error handler for Aeron errors.
func SetErrorHandler(fn func(errcode int32, message string)) {
	errorHandlerMu.Lock()
	errorHandlerFn = fn
	errorHandlerMu.Unlock()
}

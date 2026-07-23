package aeron

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// Log buffer constants.
const (
	PartitionCount        = 3
	LogMetaDataLength     = 4 * 1024 // PAGE_MIN_SIZE (Java LogBufferDescriptor)
	DataFrameHeaderLen    = 32       // aeron_data_header_t
	FrameLengthOffset     = 0
	FrameVersionOffset    = 4
	FrameFlagsOffset      = 5
	FrameTypeOffset       = 6
	FrameTermOffsetOff    = 8
	FrameSessionIDOff     = 12
	FrameStreamIDOff      = 16
	FrameTermIDOff        = 20
	FrameReservedValueOff = 24

	// Frame types
	FrameTypePadding = 0x00
	FrameTypeData    = 0x06

	// Fragment flags
	FlagBeginFrag = 0x80
	FlagEndFrag   = 0x40
	FlagUnfrag    = FlagBeginFrag | FlagEndFrag

	// Metadata offsets within the log metadata section.
	// Fields are padded to cache-line boundaries (PADDING_SIZE=64).
	// Matches Java LogBufferDescriptor exactly.
	MetaTermTailCounterOff = 0   // 3 x int64 (packed: termID in high 32, offset in low 32)
	MetaActiveTermCountOff = 24  // int32
	MetaEndOfStreamOff     = 128 // int64 (PADDING_SIZE * 2)
	MetaIsConnectedOff     = 136 // int32
	MetaActiveTransportOff = 140 // int32
	MetaCorrelationIDOff   = 256 // int64 (PADDING_SIZE * 4)
	MetaInitialTermIDOff   = 264 // int32
	MetaDefaultHdrLenOff   = 268 // int32
	MetaMtuLenOff          = 272 // int32
	MetaTermLenOff         = 276 // int32
	MetaPageSizeOff        = 280 // int32
)

// LogBuffers represents a memory-mapped log buffer file.
type LogBuffers struct {
	data    []byte
	termLen int32
	terms   [PartitionCount]*AtomicBuffer
	meta    *AtomicBuffer
}

// MapLogBuffers opens and memory-maps a log buffer file.
func MapLogBuffers(path string) (*LogBuffers, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open log buffer: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat log buffer: %w", err)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap log buffer: %w", err)
	}

	fileLen := int32(fi.Size())
	termLen := (fileLen - LogMetaDataLength) / PartitionCount

	lb := &LogBuffers{data: data, termLen: termLen}
	for i := 0; i < PartitionCount; i++ {
		offset := int32(i) * termLen
		lb.terms[i] = WrapPtr(unsafe.Pointer(&data[offset]), termLen)
	}
	lb.meta = WrapPtr(unsafe.Pointer(&data[fileLen-LogMetaDataLength]), LogMetaDataLength)

	return lb, nil
}

// Term returns the buffer for the given partition index (0-2).
func (lb *LogBuffers) Term(index int) *AtomicBuffer { return lb.terms[index] }

// Meta returns the metadata buffer.
func (lb *LogBuffers) Meta() *AtomicBuffer { return lb.meta }

// TermLength returns the size of each term buffer.
func (lb *LogBuffers) TermLength() int32 { return lb.termLen }

// Close unmaps the log buffer file.
func (lb *LogBuffers) Close() error {
	if lb.data != nil {
		err := syscall.Munmap(lb.data)
		lb.data = nil
		return err
	}
	return nil
}

// IsConnected reads the is_connected flag from log metadata.
func (lb *LogBuffers) IsConnected() bool {
	return lb.meta.GetInt32Volatile(MetaIsConnectedOff) == 1
}

// ActiveTermCount returns the current active term rotation counter.
func (lb *LogBuffers) ActiveTermCount() int32 {
	return lb.meta.GetInt32Volatile(MetaActiveTermCountOff)
}

// InitialTermID returns the initial term ID.
func (lb *LogBuffers) InitialTermID() int32 {
	return lb.meta.GetInt32(MetaInitialTermIDOff)
}

// TermTailCounter reads the packed tail counter for a partition.
// Returns (termID, tailOffset).
func (lb *LogBuffers) TermTailCounter(index int) (int32, int32) {
	raw := lb.meta.GetInt64Volatile(int32(MetaTermTailCounterOff + index*8))
	termID := int32(raw >> 32)
	tailOffset := int32(raw) // low 32 bits
	return termID, tailOffset
}

// packTail packs a termID and termOffset into a raw tail value
// (termID in the high 32 bits, offset in the low 32 bits).
func packTail(termID, termOffset int32) int64 {
	return int64(termID)<<32 | int64(uint32(termOffset))
}

// rawTailTermID extracts the termID from a packed raw tail value.
func rawTailTermID(rawTail int64) int32 {
	return int32(rawTail >> 32)
}

// rawTailTermOffset extracts the termOffset from a packed raw tail value,
// clamped to termLen (the tail can exceed the term length after concurrent
// getAndAdd claims trip the end of the term).
func rawTailTermOffset(rawTail int64, termLen int32) int32 {
	tail := rawTail & 0xFFFF_FFFF
	if tail > int64(termLen) {
		return termLen
	}
	return int32(tail)
}

// rotateLog rotates the log to the next term. It prepares the next
// partition's tail counter with the next termID (via CAS from the expected
// stale termID) and then advances the active term count. Mirrors Java
// LogBufferDescriptor.rotateLog. Safe for concurrent use — only one caller
// wins each CAS; losers observe the already-rotated state.
func rotateLog(meta *AtomicBuffer, termCount, termID int32) bool {
	nextTermID := termID + 1
	nextTermCount := termCount + 1
	nextIndex := int(nextTermCount % PartitionCount)
	expectedTermID := nextTermID - PartitionCount
	tailOff := int32(MetaTermTailCounterOff + nextIndex*8)

	for {
		rawTail := meta.GetInt64Volatile(tailOff)
		if expectedTermID != rawTailTermID(rawTail) {
			break
		}
		if meta.CompareAndSetInt64(tailOff, rawTail, packTail(nextTermID, 0)) {
			break
		}
	}

	return meta.CompareAndSetInt32(MetaActiveTermCountOff, termCount, nextTermCount)
}

// --- Term Appender ---

// TermAppender writes messages to a term buffer.
type TermAppender struct {
	term      *AtomicBuffer
	meta      *AtomicBuffer
	partIndex int
}

// NewTermAppender creates an appender for the given partition.
func NewTermAppender(lb *LogBuffers, partIndex int) *TermAppender {
	return &TermAppender{
		term:      lb.terms[partIndex],
		meta:      lb.meta,
		partIndex: partIndex,
	}
}

// Append writes a message frame to the term buffer.
// Returns the new stream position, or a negative value on failure.
func (a *TermAppender) Append(
	termID, sessionID, streamID int32,
	src []byte,
) int64 {
	frameLen := int32(DataFrameHeaderLen + len(src))
	alignedLen := align(frameLen, DataFrameHeaderLen) // align to 32-byte frame boundary

	// Claim space: atomically add to tail
	tailOff := int32(MetaTermTailCounterOff + a.partIndex*8)
	rawTailBefore := a.meta.GetAndAddInt64(tailOff, int64(alignedLen))

	termOffset := int32(rawTailBefore) // low 32 bits
	termLen := a.term.Capacity()

	if termOffset+alignedLen > termLen {
		return BackPressured // term full
	}

	// Write frame header
	a.term.PutInt32(termOffset+FrameLengthOffset, 0) // uncommitted
	a.term.PutUint8(termOffset+FrameVersionOffset, 0)
	a.term.PutUint8(termOffset+FrameFlagsOffset, FlagUnfrag)
	a.term.PutInt32(termOffset+FrameTypeOffset, FrameTypeData)
	a.term.PutInt32(termOffset+FrameTermOffsetOff, termOffset)
	a.term.PutInt32(termOffset+FrameSessionIDOff, sessionID)
	a.term.PutInt32(termOffset+FrameStreamIDOff, streamID)
	a.term.PutInt32(termOffset+FrameTermIDOff, termID)

	// Write payload
	a.term.PutBytes(termOffset+DataFrameHeaderLen, src)

	// Commit: set frame length (ordered store)
	a.term.PutInt32Ordered(termOffset+FrameLengthOffset, frameLen)

	return rawTailBefore + int64(alignedLen)
}

// --- Term Reader ---

// TermFragmentHandler is called for each message fragment read from a term.
type TermFragmentHandler func(buffer *AtomicBuffer, offset, length int32, header *DataFrameHeader)

// DataFrameHeader is a parsed frame header.
type DataFrameHeader struct {
	FrameLength   int32
	Version       uint8
	Flags         uint8
	Type          int16
	TermOffset    int32
	SessionID     int32
	StreamID      int32
	TermID        int32
	ReservedValue int64
}

// ReadTerm reads messages from a term buffer starting at the given offset.
// Returns (fragmentsRead, newOffset).
func ReadTerm(
	term *AtomicBuffer,
	termOffset int32,
	handler TermFragmentHandler,
	fragmentLimit int,
) (int, int32) {
	fragmentsRead := 0
	offset := termOffset
	capacity := term.Capacity()

	for fragmentsRead < fragmentLimit && offset < capacity {
		frameLen := term.GetInt32Volatile(offset + FrameLengthOffset)
		if frameLen <= 0 {
			break // no more committed frames
		}

		alignedLen := align(frameLen, DataFrameHeaderLen)
		frameType := int16(term.GetInt32(offset + FrameTypeOffset))

		if frameType != int16(FrameTypePadding) {
			hdr := DataFrameHeader{
				FrameLength:   frameLen,
				Version:       term.GetUint8(offset + FrameVersionOffset),
				Flags:         term.GetUint8(offset + FrameFlagsOffset),
				Type:          frameType,
				TermOffset:    term.GetInt32(offset + FrameTermOffsetOff),
				SessionID:     term.GetInt32(offset + FrameSessionIDOff),
				StreamID:      term.GetInt32(offset + FrameStreamIDOff),
				TermID:        term.GetInt32(offset + FrameTermIDOff),
				ReservedValue: term.GetInt64(offset + FrameReservedValueOff),
			}

			payloadOffset := offset + DataFrameHeaderLen
			payloadLen := frameLen - DataFrameHeaderLen
			handler(term, payloadOffset, payloadLen, &hdr)
			fragmentsRead++
		}

		offset += alignedLen
	}

	return fragmentsRead, offset
}

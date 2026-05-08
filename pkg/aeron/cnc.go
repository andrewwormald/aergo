package aeron

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

// CnC (Command and Control) file layout.
const (
	CncVersion          = 26 // Aeron CnC version we support (1.46.x)
	CncFilename         = "cnc.dat"
	CncMetadataLength   = 2 * CacheLineLength // 128 bytes, padded to 2 cache lines
	CncVersionOffset    = 0
	CncToDriverLenOff   = 4
	CncToClientsLenOff  = 8
	CncCounterMetaOff   = 12
	CncCounterValuesOff = 16
	CncErrorLogLenOff   = 20
	CncClientTimeoutOff = 24
	CncStartTimestampOff = 32
	CncPidOffset        = 40
)

// CncMetadata holds the parsed CnC file header.
type CncMetadata struct {
	Version              int32
	ToDriverBufLen       int32
	ToClientsBufLen      int32
	CounterMetadataLen   int32
	CounterValuesLen     int32
	ErrorLogLen          int32
	ClientLivenessTimeNs int64
	StartTimestamp       int64
	Pid                  int64
}

// MappedCnc represents a memory-mapped CnC file with all buffers accessible.
type MappedCnc struct {
	data     []byte
	metadata CncMetadata

	ToDriverBuffer   *AtomicBuffer
	ToClientsBuffer  *AtomicBuffer
	CounterMetadata  *AtomicBuffer
	CounterValues    *AtomicBuffer
	ErrorLogBuffer   *AtomicBuffer
}

// MapCnc opens and memory-maps the CnC file from an Aeron media driver directory.
func MapCnc(aeronDir string) (*MappedCnc, error) {
	path := filepath.Join(aeronDir, CncFilename)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open cnc: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat cnc: %w", err)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap cnc: %w", err)
	}

	cnc := &MappedCnc{data: data}

	// Parse metadata header
	cnc.metadata = CncMetadata{
		Version:              int32(binary.LittleEndian.Uint32(data[CncVersionOffset:])),
		ToDriverBufLen:       int32(binary.LittleEndian.Uint32(data[CncToDriverLenOff:])),
		ToClientsBufLen:      int32(binary.LittleEndian.Uint32(data[CncToClientsLenOff:])),
		CounterMetadataLen:   int32(binary.LittleEndian.Uint32(data[CncCounterMetaOff:])),
		CounterValuesLen:     int32(binary.LittleEndian.Uint32(data[CncCounterValuesOff:])),
		ErrorLogLen:          int32(binary.LittleEndian.Uint32(data[CncErrorLogLenOff:])),
		ClientLivenessTimeNs: int64(binary.LittleEndian.Uint64(data[CncClientTimeoutOff:])),
		StartTimestamp:       int64(binary.LittleEndian.Uint64(data[CncStartTimestampOff:])),
		Pid:                  int64(binary.LittleEndian.Uint64(data[CncPidOffset:])),
	}

	// Slice the mmap'd region into component buffers
	offset := int32(CncMetadataLength)

	toDriverPtr := unsafe.Pointer(&data[offset])
	cnc.ToDriverBuffer = WrapPtr(toDriverPtr, cnc.metadata.ToDriverBufLen)
	offset += cnc.metadata.ToDriverBufLen

	toClientsPtr := unsafe.Pointer(&data[offset])
	cnc.ToClientsBuffer = WrapPtr(toClientsPtr, cnc.metadata.ToClientsBufLen)
	offset += cnc.metadata.ToClientsBufLen

	counterMetaPtr := unsafe.Pointer(&data[offset])
	cnc.CounterMetadata = WrapPtr(counterMetaPtr, cnc.metadata.CounterMetadataLen)
	offset += cnc.metadata.CounterMetadataLen

	counterValPtr := unsafe.Pointer(&data[offset])
	cnc.CounterValues = WrapPtr(counterValPtr, cnc.metadata.CounterValuesLen)
	offset += cnc.metadata.CounterValuesLen

	errorLogPtr := unsafe.Pointer(&data[offset])
	cnc.ErrorLogBuffer = WrapPtr(errorLogPtr, cnc.metadata.ErrorLogLen)

	return cnc, nil
}

// Metadata returns the parsed CnC header.
func (c *MappedCnc) Metadata() CncMetadata { return c.metadata }

// Close unmaps the CnC file.
func (c *MappedCnc) Close() error {
	if c.data != nil {
		err := syscall.Munmap(c.data)
		c.data = nil
		return err
	}
	return nil
}

// DriverHeartbeat reads the driver's heartbeat timestamp from the CnC counters.
// The heartbeat counter is typically counter ID 0.
func (c *MappedCnc) DriverHeartbeat() int64 {
	return c.CounterValues.GetInt64Volatile(0)
}

// ReadErrorLog reads error entries from the driver's error log buffer.
// Each entry: [length 4B][observationCount 4B][lastObservation 8B][firstObservation 8B][message varlen]
func (c *MappedCnc) ReadErrorLog() []string {
	buf := c.ErrorLogBuffer
	capacity := buf.Capacity()
	var errors []string
	offset := int32(0)

	for offset < capacity {
		length := buf.GetInt32Volatile(offset)
		if length == 0 {
			break
		}
		if length < 24 || offset+length > capacity {
			break
		}
		// Message starts at offset+24
		msgLen := length - 24
		if msgLen > 0 {
			msg := make([]byte, msgLen)
			buf.GetBytes(offset+24, msg)
			// Trim trailing zeros
			end := len(msg)
			for end > 0 && msg[end-1] == 0 {
				end--
			}
			if end > 0 {
				errors = append(errors, string(msg[:end]))
			}
		}
		offset += align(length, 8)
	}
	return errors
}

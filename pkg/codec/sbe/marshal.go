package sbe

import "encoding/binary"

// SBE encoding primitives -- little-endian read/write at fixed offsets.
// All functions operate on pre-allocated byte slices. Zero-alloc by design.

func PutUint8(buf []byte, offset int, v uint8) {
	buf[offset] = v
}

func GetUint8(buf []byte, offset int) uint8 {
	return buf[offset]
}

func PutInt8(buf []byte, offset int, v int8) {
	buf[offset] = byte(v)
}

func GetInt8(buf []byte, offset int) int8 {
	return int8(buf[offset])
}

func PutUint16(buf []byte, offset int, v uint16) {
	binary.LittleEndian.PutUint16(buf[offset:], v)
}

func GetUint16(buf []byte, offset int) uint16 {
	return binary.LittleEndian.Uint16(buf[offset:])
}

func PutInt16(buf []byte, offset int, v int16) {
	binary.LittleEndian.PutUint16(buf[offset:], uint16(v))
}

func GetInt16(buf []byte, offset int) int16 {
	return int16(binary.LittleEndian.Uint16(buf[offset:]))
}

func PutUint32(buf []byte, offset int, v uint32) {
	binary.LittleEndian.PutUint32(buf[offset:], v)
}

func GetUint32(buf []byte, offset int) uint32 {
	return binary.LittleEndian.Uint32(buf[offset:])
}

func PutInt32(buf []byte, offset int, v int32) {
	binary.LittleEndian.PutUint32(buf[offset:], uint32(v))
}

func GetInt32(buf []byte, offset int) int32 {
	return int32(binary.LittleEndian.Uint32(buf[offset:]))
}

func PutUint64(buf []byte, offset int, v uint64) {
	binary.LittleEndian.PutUint64(buf[offset:], v)
}

func GetUint64(buf []byte, offset int) uint64 {
	return binary.LittleEndian.Uint64(buf[offset:])
}

func PutInt64(buf []byte, offset int, v int64) {
	binary.LittleEndian.PutUint64(buf[offset:], uint64(v))
}

func GetInt64(buf []byte, offset int) int64 {
	return int64(binary.LittleEndian.Uint64(buf[offset:]))
}

// PutVarString writes a variable-length string with a uint32 length prefix.
// Returns the total bytes written (4 + len(s)).
func PutVarString(buf []byte, offset int, s string) int {
	PutUint32(buf, offset, uint32(len(s)))
	copy(buf[offset+4:], s)
	return 4 + len(s)
}

// GetVarString reads a variable-length string with a uint32 length prefix.
// Returns the string and total bytes consumed (4 + len).
func GetVarString(buf []byte, offset int) (string, int) {
	length := int(GetUint32(buf, offset))
	s := string(buf[offset+4 : offset+4+length])
	return s, 4 + length
}

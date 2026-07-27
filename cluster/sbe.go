package cluster

import "encoding/binary"

// SBE encoding primitives -- little-endian read/write at fixed offsets.

const HeaderSize = 8

type MessageHeader struct {
	BlockLength uint16
	TemplateId  uint16
	SchemaId    uint16
	Version     uint16
}

func (h *MessageHeader) Encode(buf []byte, offset int) int {
	binary.LittleEndian.PutUint16(buf[offset+0:], h.BlockLength)
	binary.LittleEndian.PutUint16(buf[offset+2:], h.TemplateId)
	binary.LittleEndian.PutUint16(buf[offset+4:], h.SchemaId)
	binary.LittleEndian.PutUint16(buf[offset+6:], h.Version)
	return HeaderSize
}

func (h *MessageHeader) Decode(buf []byte, offset int) {
	h.BlockLength = binary.LittleEndian.Uint16(buf[offset+0:])
	h.TemplateId = binary.LittleEndian.Uint16(buf[offset+2:])
	h.SchemaId = binary.LittleEndian.Uint16(buf[offset+4:])
	h.Version = binary.LittleEndian.Uint16(buf[offset+6:])
}

func putInt32(buf []byte, offset int, v int32) {
	binary.LittleEndian.PutUint32(buf[offset:], uint32(v))
}

func getInt32(buf []byte, offset int) int32 {
	return int32(binary.LittleEndian.Uint32(buf[offset:]))
}

func putUint32(buf []byte, offset int, v uint32) {
	binary.LittleEndian.PutUint32(buf[offset:], v)
}

func getUint32(buf []byte, offset int) uint32 {
	return binary.LittleEndian.Uint32(buf[offset:])
}

func putInt64(buf []byte, offset int, v int64) {
	binary.LittleEndian.PutUint64(buf[offset:], uint64(v))
}

func getInt64(buf []byte, offset int) int64 {
	return int64(binary.LittleEndian.Uint64(buf[offset:]))
}

func putVarString(buf []byte, offset int, s string) int {
	putUint32(buf, offset, uint32(len(s)))
	copy(buf[offset+4:], s)
	return 4 + len(s)
}

func getVarString(buf []byte, offset int) (string, int) {
	length := int(getUint32(buf, offset))
	s := string(buf[offset+4 : offset+4+length])
	return s, 4 + length
}

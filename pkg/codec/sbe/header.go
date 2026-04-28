package sbe

// MessageHeader is the 8-byte SBE message header present on every message.
//
// Layout (little-endian):
//   offset 0: BlockLength  uint16  -- length of the fixed-size fields in the message body
//   offset 2: TemplateId   uint16  -- identifies the message type
//   offset 4: SchemaId     uint16  -- schema identifier (111 for Aeron cluster)
//   offset 6: Version      uint16  -- schema version
const HeaderSize = 8

type MessageHeader struct {
	BlockLength uint16
	TemplateId  uint16
	SchemaId    uint16
	Version     uint16
}

// Encode writes the header to buf at the given offset.
// Returns HeaderSize (8).
func (h *MessageHeader) Encode(buf []byte, offset int) int {
	PutUint16(buf, offset+0, h.BlockLength)
	PutUint16(buf, offset+2, h.TemplateId)
	PutUint16(buf, offset+4, h.SchemaId)
	PutUint16(buf, offset+6, h.Version)
	return HeaderSize
}

// Decode reads the header from buf at the given offset.
func (h *MessageHeader) Decode(buf []byte, offset int) {
	h.BlockLength = GetUint16(buf, offset+0)
	h.TemplateId = GetUint16(buf, offset+2)
	h.SchemaId = GetUint16(buf, offset+4)
	h.Version = GetUint16(buf, offset+6)
}

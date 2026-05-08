package aeron

import "encoding/binary"

// Command type IDs for the to-driver ring buffer.
const (
	CmdAddPublication          int32 = 0x01
	CmdRemovePublication       int32 = 0x02
	CmdAddExclusivePublication int32 = 0x03
	CmdAddSubscription         int32 = 0x04
	CmdRemoveSubscription      int32 = 0x05
	CmdClientKeepalive         int32 = 0x06
	CmdAddDestination          int32 = 0x07
	CmdRemoveDestination       int32 = 0x08
	CmdAddCounter              int32 = 0x09
	CmdRemoveCounter           int32 = 0x0A
	CmdClientClose             int32 = 0x0B
	CmdAddRcvDestination       int32 = 0x0C
	CmdRemoveRcvDestination    int32 = 0x0D
	CmdTerminateDriver         int32 = 0x0E
)

// Response type IDs from the to-clients broadcast buffer.
// These must match io.aeron.command.ControlProtocolEvents in the Java driver.
const (
	RespOnError                int32 = 0x0F03
	RespOnAvailableImage       int32 = 0x0F04
	RespOnPublication          int32 = 0x0F05
	RespOnOperationSuccess     int32 = 0x0F06
	RespOnUnavailableImage     int32 = 0x0F07
	RespOnExclusivePublication int32 = 0x0F08
	RespOnSubscription         int32 = 0x0F09
	RespOnCounter              int32 = 0x0F0A
	RespOnUnavailableCounter   int32 = 0x0F0B
	RespOnClientTimeout        int32 = 0x0F0C
)

// Publication command message layout.
//
//	offset 0:   clientID       int64
//	offset 8:   correlationID  int64
//	offset 16:  streamID       int32
//	offset 20:  channelLength  int32
//	offset 24:  channel        []byte (variable)
const publicationMsgFixedLen = 24

// Subscription command message layout.
//
//	offset 0:   clientID       int64
//	offset 8:   correlationID  int64
//	offset 16:  registrationCorrelationID int64
//	offset 24:  streamID       int32
//	offset 28:  channelLength  int32
//	offset 32:  channel        []byte (variable)
const subscriptionMsgFixedLen = 32

// Remove command message layout.
//
//	offset 0:   clientID       int64
//	offset 8:   correlationID  int64
//	offset 16:  registrationID int64
const removeMsgLen = 24

// Keepalive command message layout.
//
//	offset 0:   clientID       int64
//	offset 8:   correlationID  int64
const keepaliveMsgLen = 16

// ClientClose command message layout.
//
//	offset 0:   clientID       int64
//	offset 8:   correlationID  int64
const clientCloseMsgLen = 16

// DriverProxy sends commands to the media driver via the to-driver ring buffer.
type DriverProxy struct {
	rb       *ManyToOneRingBuffer
	clientID int64
}

// NewDriverProxy creates a new driver proxy.
func NewDriverProxy(rb *ManyToOneRingBuffer, clientID int64) *DriverProxy {
	return &DriverProxy{rb: rb, clientID: clientID}
}

// NextCorrelationID generates a unique correlation ID.
func (p *DriverProxy) NextCorrelationID() int64 {
	return p.rb.NextCorrelationID()
}

// AddPublication sends an add-publication command. Returns the correlationID.
func (p *DriverProxy) AddPublication(channel string, streamID int32) int64 {
	return p.sendPublicationCmd(CmdAddPublication, channel, streamID)
}

// AddExclusivePublication sends an add-exclusive-publication command.
func (p *DriverProxy) AddExclusivePublication(channel string, streamID int32) int64 {
	return p.sendPublicationCmd(CmdAddExclusivePublication, channel, streamID)
}

// AddSubscription sends an add-subscription command. Returns the correlationID.
func (p *DriverProxy) AddSubscription(channel string, streamID int32) int64 {
	corrID := p.rb.NextCorrelationID()

	buf := make([]byte, subscriptionMsgFixedLen+len(channel))
	binary.LittleEndian.PutUint64(buf[0:], uint64(p.clientID))
	binary.LittleEndian.PutUint64(buf[8:], uint64(corrID))
	binary.LittleEndian.PutUint64(buf[16:], ^uint64(0)) // registrationCorrelationID = NULL_VALUE (-1)
	binary.LittleEndian.PutUint32(buf[24:], uint32(streamID))
	binary.LittleEndian.PutUint32(buf[28:], uint32(len(channel)))
	copy(buf[32:], channel)

	if !p.rb.Write(CmdAddSubscription, buf) {
		return -1
	}
	return corrID
}

// RemovePublication sends a remove-publication command.
func (p *DriverProxy) RemovePublication(registrationID int64) int64 {
	return p.sendRemoveCmd(CmdRemovePublication, registrationID)
}

// RemoveSubscription sends a remove-subscription command.
func (p *DriverProxy) RemoveSubscription(registrationID int64) int64 {
	return p.sendRemoveCmd(CmdRemoveSubscription, registrationID)
}

// SendClientKeepalive sends a keepalive command.
func (p *DriverProxy) SendClientKeepalive() bool {
	buf := make([]byte, keepaliveMsgLen)
	binary.LittleEndian.PutUint64(buf[0:], uint64(p.clientID))
	binary.LittleEndian.PutUint64(buf[8:], uint64(p.rb.NextCorrelationID()))
	return p.rb.Write(CmdClientKeepalive, buf)
}

// ClientClose sends a client-close command.
func (p *DriverProxy) ClientClose() bool {
	buf := make([]byte, clientCloseMsgLen)
	binary.LittleEndian.PutUint64(buf[0:], uint64(p.clientID))
	binary.LittleEndian.PutUint64(buf[8:], uint64(p.rb.NextCorrelationID()))
	return p.rb.Write(CmdClientClose, buf)
}

func (p *DriverProxy) sendPublicationCmd(cmdType int32, channel string, streamID int32) int64 {
	corrID := p.rb.NextCorrelationID()

	buf := make([]byte, publicationMsgFixedLen+len(channel))
	binary.LittleEndian.PutUint64(buf[0:], uint64(p.clientID))
	binary.LittleEndian.PutUint64(buf[8:], uint64(corrID))
	binary.LittleEndian.PutUint32(buf[16:], uint32(streamID))
	binary.LittleEndian.PutUint32(buf[20:], uint32(len(channel)))
	copy(buf[24:], channel)

	if !p.rb.Write(cmdType, buf) {
		return -1
	}
	return corrID
}

func (p *DriverProxy) sendRemoveCmd(cmdType int32, registrationID int64) int64 {
	corrID := p.rb.NextCorrelationID()

	buf := make([]byte, removeMsgLen)
	binary.LittleEndian.PutUint64(buf[0:], uint64(p.clientID))
	binary.LittleEndian.PutUint64(buf[8:], uint64(corrID))
	binary.LittleEndian.PutUint64(buf[16:], uint64(registrationID))

	if !p.rb.Write(cmdType, buf) {
		return -1
	}
	return corrID
}

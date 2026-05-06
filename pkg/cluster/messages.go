package cluster




// Aeron cluster protocol message template IDs.
const (
	TemplateIdSessionMessageHeader = 1
	TemplateIdSessionEvent         = 2
	TemplateIdSessionConnectRequest = 3
	TemplateIdSessionCloseRequest  = 4
	TemplateIdSessionKeepAlive     = 5
	TemplateIdNewLeaderEvent       = 6
	TemplateIdChallenge            = 7
	TemplateIdChallengeResponse    = 8
)

// Cluster protocol schema constants.
const (
	SchemaId      uint16 = 111
	SchemaVersion uint16 = 8
)

// EventCode represents the cluster session event codes.
type EventCode int32

const (
	EventCodeOK             EventCode = 0
	EventCodeError          EventCode = 1
	EventCodeRedirect       EventCode = 2
	EventCodeAuthentication EventCode = 3
	EventCodeRejected       EventCode = 4
	EventCodeClosed         EventCode = 5
)

// ---------------------------------------------------------------------------
// SessionMessageHeader (Template 1)
//
// Wraps every application message sent to/from the cluster.
//
// Fixed fields (24 bytes):
//   offset 0:  LeadershipTermId  int64
//   offset 8:  ClusterSessionId  int64
//   offset 16: Timestamp         int64
// ---------------------------------------------------------------------------

type SessionMessageHeader struct {
	LeadershipTermId int64
	ClusterSessionId int64
	Timestamp        int64
}

const sessionMessageHeaderBlockLength = 24

func (m *SessionMessageHeader) Encode(buf []byte, offset int) int {
	hdr := MessageHeader{
		BlockLength: sessionMessageHeaderBlockLength,
		TemplateId:  TemplateIdSessionMessageHeader,
		SchemaId:    SchemaId,
		Version:     SchemaVersion,
	}
	n := hdr.Encode(buf, offset)
	putInt64(buf, offset+n+0, m.LeadershipTermId)
	putInt64(buf, offset+n+8, m.ClusterSessionId)
	putInt64(buf, offset+n+16, m.Timestamp)
	return n + sessionMessageHeaderBlockLength
}

func (m *SessionMessageHeader) Decode(buf []byte, offset int) int {
	return m.DecodeWithBlockLength(buf, offset, sessionMessageHeaderBlockLength)
}

func (m *SessionMessageHeader) DecodeWithBlockLength(buf []byte, offset int, blockLength int) int {
	m.LeadershipTermId = getInt64(buf, offset+0)
	m.ClusterSessionId = getInt64(buf, offset+8)
	m.Timestamp = getInt64(buf, offset+16)
	return blockLength
}

// ---------------------------------------------------------------------------
// SessionEvent (Template 2)
//
// Sent from cluster to client in response to connect/close/error.
//
// Fixed fields (36 bytes):
//   offset 0:  ClusterSessionId  int64
//   offset 8:  CorrelationId     int64
//   offset 16: LeadershipTermId  int64
//   offset 24: LeaderMemberId    int32
//   offset 28: EventCode         int32
//
// Variable fields:
//   Detail (var string with uint32 length prefix)
// ---------------------------------------------------------------------------

type SessionEvent struct {
	ClusterSessionId int64
	CorrelationId    int64
	LeadershipTermId int64
	LeaderMemberId   int32
	Code             EventCode
	Detail           string
}

const sessionEventBlockLength = 32

func (m *SessionEvent) Encode(buf []byte, offset int) int {
	hdr := MessageHeader{
		BlockLength: sessionEventBlockLength,
		TemplateId:  TemplateIdSessionEvent,
		SchemaId:    SchemaId,
		Version:     SchemaVersion,
	}
	n := hdr.Encode(buf, offset)
	base := offset + n
	putInt64(buf, base+0, m.ClusterSessionId)
	putInt64(buf, base+8, m.CorrelationId)
	putInt64(buf, base+16, m.LeadershipTermId)
	putInt32(buf, base+24, m.LeaderMemberId)
	putInt32(buf, base+28, int32(m.Code))
	varOffset := base + sessionEventBlockLength
	varN := putVarString(buf, varOffset, m.Detail)
	return n + sessionEventBlockLength + varN
}

func (m *SessionEvent) Decode(buf []byte, offset int) int {
	return m.DecodeWithBlockLength(buf, offset, sessionEventBlockLength)
}

func (m *SessionEvent) DecodeWithBlockLength(buf []byte, offset int, blockLength int) int {
	m.ClusterSessionId = getInt64(buf, offset+0)
	m.CorrelationId = getInt64(buf, offset+8)
	m.LeadershipTermId = getInt64(buf, offset+16)
	m.LeaderMemberId = getInt32(buf, offset+24)
	m.Code = EventCode(getInt32(buf, offset+28))
	varOffset := offset + blockLength
	detail, varN := getVarString(buf, varOffset)
	m.Detail = detail
	return blockLength + varN
}

// ---------------------------------------------------------------------------
// SessionConnectRequest (Template 3)
//
// Sent from client to cluster to establish a session.
//
// Fixed fields (16 bytes):
//   offset 0:  CorrelationId    int64
//   offset 8:  ResponseStreamId int32
//   offset 12: Version          int32
//
// Variable fields:
//   ResponseChannel (var string)
// ---------------------------------------------------------------------------

type SessionConnectRequest struct {
	CorrelationId    int64
	ResponseStreamId int32
	Version          int32
	ResponseChannel  string
}

const sessionConnectRequestBlockLength = 16

func (m *SessionConnectRequest) Encode(buf []byte, offset int) int {
	hdr := MessageHeader{
		BlockLength: sessionConnectRequestBlockLength,
		TemplateId:  TemplateIdSessionConnectRequest,
		SchemaId:    SchemaId,
		Version:     SchemaVersion,
	}
	n := hdr.Encode(buf, offset)
	base := offset + n
	putInt64(buf, base+0, m.CorrelationId)
	putInt32(buf, base+8, m.ResponseStreamId)
	putInt32(buf, base+12, m.Version)
	varOffset := base + sessionConnectRequestBlockLength
	varN := putVarString(buf, varOffset, m.ResponseChannel)
	return n + sessionConnectRequestBlockLength + varN
}

func (m *SessionConnectRequest) Decode(buf []byte, offset int) int {
	m.CorrelationId = getInt64(buf, offset+0)
	m.ResponseStreamId = getInt32(buf, offset+8)
	m.Version = getInt32(buf, offset+12)
	varOffset := offset + sessionConnectRequestBlockLength
	ch, varN := getVarString(buf, varOffset)
	m.ResponseChannel = ch
	return sessionConnectRequestBlockLength + varN
}

// ---------------------------------------------------------------------------
// SessionCloseRequest (Template 4)
//
// Sent from client to cluster to close an existing session.
//
// Fixed fields (16 bytes):
//   offset 0:  ClusterSessionId int64
//   offset 8:  LeadershipTermId int64
// ---------------------------------------------------------------------------

type SessionCloseRequest struct {
	ClusterSessionId int64
	LeadershipTermId int64
}

const sessionCloseRequestBlockLength = 16

func (m *SessionCloseRequest) Encode(buf []byte, offset int) int {
	hdr := MessageHeader{
		BlockLength: sessionCloseRequestBlockLength,
		TemplateId:  TemplateIdSessionCloseRequest,
		SchemaId:    SchemaId,
		Version:     SchemaVersion,
	}
	n := hdr.Encode(buf, offset)
	base := offset + n
	putInt64(buf, base+0, m.ClusterSessionId)
	putInt64(buf, base+8, m.LeadershipTermId)
	return n + sessionCloseRequestBlockLength
}

func (m *SessionCloseRequest) Decode(buf []byte, offset int) int {
	m.ClusterSessionId = getInt64(buf, offset+0)
	m.LeadershipTermId = getInt64(buf, offset+8)
	return sessionCloseRequestBlockLength
}

// ---------------------------------------------------------------------------
// SessionKeepAlive (Template 5)
//
// Sent from client to cluster to maintain session liveness.
//
// Fixed fields (16 bytes):
//   offset 0:  LeadershipTermId int64
//   offset 8:  ClusterSessionId int64
// ---------------------------------------------------------------------------

type SessionKeepAlive struct {
	LeadershipTermId int64
	ClusterSessionId int64
}

const sessionKeepAliveBlockLength = 16

func (m *SessionKeepAlive) Encode(buf []byte, offset int) int {
	hdr := MessageHeader{
		BlockLength: sessionKeepAliveBlockLength,
		TemplateId:  TemplateIdSessionKeepAlive,
		SchemaId:    SchemaId,
		Version:     SchemaVersion,
	}
	n := hdr.Encode(buf, offset)
	base := offset + n
	putInt64(buf, base+0, m.LeadershipTermId)
	putInt64(buf, base+8, m.ClusterSessionId)
	return n + sessionKeepAliveBlockLength
}

func (m *SessionKeepAlive) Decode(buf []byte, offset int) int {
	m.LeadershipTermId = getInt64(buf, offset+0)
	m.ClusterSessionId = getInt64(buf, offset+8)
	return sessionKeepAliveBlockLength
}

// ---------------------------------------------------------------------------
// NewLeaderEvent (Template 6)
//
// Sent from cluster to client when leadership changes.
//
// Fixed fields (20 bytes):
//   offset 0:  ClusterSessionId int64
//   offset 8:  LeadershipTermId int64
//   offset 16: LeaderMemberId   int32
//
// Variable fields:
//   IngressEndpoints (var string)
// ---------------------------------------------------------------------------

type NewLeaderEvent struct {
	ClusterSessionId int64
	LeadershipTermId int64
	LeaderMemberId   int32
	IngressEndpoints string
}

const newLeaderEventBlockLength = 20

func (m *NewLeaderEvent) Encode(buf []byte, offset int) int {
	hdr := MessageHeader{
		BlockLength: newLeaderEventBlockLength,
		TemplateId:  TemplateIdNewLeaderEvent,
		SchemaId:    SchemaId,
		Version:     SchemaVersion,
	}
	n := hdr.Encode(buf, offset)
	base := offset + n
	putInt64(buf, base+0, m.ClusterSessionId)
	putInt64(buf, base+8, m.LeadershipTermId)
	putInt32(buf, base+16, m.LeaderMemberId)
	varOffset := base + newLeaderEventBlockLength
	varN := putVarString(buf, varOffset, m.IngressEndpoints)
	return n + newLeaderEventBlockLength + varN
}

func (m *NewLeaderEvent) Decode(buf []byte, offset int) int {
	return m.DecodeWithBlockLength(buf, offset, newLeaderEventBlockLength)
}

func (m *NewLeaderEvent) DecodeWithBlockLength(buf []byte, offset int, blockLength int) int {
	m.ClusterSessionId = getInt64(buf, offset+0)
	m.LeadershipTermId = getInt64(buf, offset+8)
	m.LeaderMemberId = getInt32(buf, offset+16)
	varOffset := offset + blockLength
	ep, varN := getVarString(buf, varOffset)
	m.IngressEndpoints = ep
	return blockLength + varN
}

// ---------------------------------------------------------------------------
// Challenge (Template 7)
//
// Sent from cluster to client as an authentication challenge.
//
// Fixed fields (24 bytes):
//   offset 0:  ClusterSessionId int64
//   offset 8:  CorrelationId    int64
//   offset 16: LeadershipTermId int64 (v8+)
//
// Variable fields:
//   ChallengeData (var data with uint32 length prefix)
// ---------------------------------------------------------------------------

type Challenge struct {
	ClusterSessionId int64
	CorrelationId    int64
	LeadershipTermId int64
	ChallengeData    []byte
}

const challengeBlockLength = 24

func (m *Challenge) Encode(buf []byte, offset int) int {
	hdr := MessageHeader{
		BlockLength: challengeBlockLength,
		TemplateId:  TemplateIdChallenge,
		SchemaId:    SchemaId,
		Version:     SchemaVersion,
	}
	n := hdr.Encode(buf, offset)
	base := offset + n
	putInt64(buf, base+0, m.ClusterSessionId)
	putInt64(buf, base+8, m.CorrelationId)
	putInt64(buf, base+16, m.LeadershipTermId)
	varOffset := base + challengeBlockLength
	putUint32(buf, varOffset, uint32(len(m.ChallengeData)))
	copy(buf[varOffset+4:], m.ChallengeData)
	return n + challengeBlockLength + 4 + len(m.ChallengeData)
}

func (m *Challenge) Decode(buf []byte, offset int) int {
	return m.DecodeWithBlockLength(buf, offset, challengeBlockLength)
}

func (m *Challenge) DecodeWithBlockLength(buf []byte, offset int, blockLength int) int {
	m.ClusterSessionId = getInt64(buf, offset+0)
	m.CorrelationId = getInt64(buf, offset+8)
	m.LeadershipTermId = getInt64(buf, offset+16)
	varOffset := offset + blockLength
	length := int(getUint32(buf, varOffset))
	m.ChallengeData = make([]byte, length)
	copy(m.ChallengeData, buf[varOffset+4:varOffset+4+length])
	return blockLength + 4 + length
}

// ---------------------------------------------------------------------------
// ChallengeResponse (Template 8)
//
// Sent from client to cluster in response to a Challenge.
//
// Fixed fields (16 bytes):
//   offset 0:  CorrelationId    int64
//   offset 8:  ClusterSessionId int64
//
// Variable fields:
//   ChallengeData (var data with uint32 length prefix)
// ---------------------------------------------------------------------------

type ChallengeResponse struct {
	CorrelationId    int64
	ClusterSessionId int64
	ChallengeData    []byte
}

const challengeResponseBlockLength = 16

func (m *ChallengeResponse) Encode(buf []byte, offset int) int {
	hdr := MessageHeader{
		BlockLength: challengeResponseBlockLength,
		TemplateId:  TemplateIdChallengeResponse,
		SchemaId:    SchemaId,
		Version:     SchemaVersion,
	}
	n := hdr.Encode(buf, offset)
	base := offset + n
	putInt64(buf, base+0, m.CorrelationId)
	putInt64(buf, base+8, m.ClusterSessionId)
	varOffset := base + challengeResponseBlockLength
	putUint32(buf, varOffset, uint32(len(m.ChallengeData)))
	copy(buf[varOffset+4:], m.ChallengeData)
	return n + challengeResponseBlockLength + 4 + len(m.ChallengeData)
}

func (m *ChallengeResponse) Decode(buf []byte, offset int) int {
	m.CorrelationId = getInt64(buf, offset+0)
	m.ClusterSessionId = getInt64(buf, offset+8)
	varOffset := offset + challengeResponseBlockLength
	length := int(getUint32(buf, varOffset))
	m.ChallengeData = make([]byte, length)
	copy(m.ChallengeData, buf[varOffset+4:varOffset+4+length])
	return challengeResponseBlockLength + 4 + length
}

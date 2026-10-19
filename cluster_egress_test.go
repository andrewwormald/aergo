package aergo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

type recordingListener struct {
	NoopListener
	messages [][]byte
	stamps   []int64
	events   []*SessionEvent
}

func (r *recordingListener) OnMessage(_ Cluster, ts int64, buf []byte, off, n int) {
	r.messages = append(r.messages, append([]byte(nil), buf[off:off+n]...))
	r.stamps = append(r.stamps, ts)
}

func (r *recordingListener) OnSessionEvent(_ Cluster, e *SessionEvent) {
	r.events = append(r.events, e)
}

// egressMessage encodes what the cluster sends a client: a session message
// header followed by the application payload.
func egressMessage(ts int64, payload []byte) []byte {
	smh := SessionMessageHeader{LeadershipTermId: 1, ClusterSessionId: 7, Timestamp: ts}
	buf := make([]byte, HeaderSize+sessionMessageHeaderBlockLength, HeaderSize+sessionMessageHeaderBlockLength+len(payload))
	smh.Encode(buf, 0)
	return append(buf, payload...)
}

func newEgressTestCluster(state ClusterState) (*AeronCluster, *Publication, *recordingListener) {
	pub, sub, _ := newFragPair(fragTestTermLen)
	l := &recordingListener{}
	c := &AeronCluster{
		cfg:             ClusterConfig{Listener: l, KeepAliveIntervalMs: int64(time.Hour / time.Millisecond)},
		state:           state,
		egressSub:       sub,
		lastKeepAliveMs: time.Now().UnixMilli(),
	}
	c.egressAssembler = NewFragmentAssembler(c.dispatchEgress)
	c.egressFragments = c.egressAssembler.OnFragment
	return c, pub, l
}

// Egress messages longer than an MTU arrive as fragments. They must reach the
// listener once, whole and in order, not as separate pieces each decoded as
// if it began with a session header.
func TestClusterEgressReassemblesFragmentedMessages(t *testing.T) {
	c, pub, l := newEgressTestCluster(StateConnected)

	payloads := [][]byte{
		patterned(10, 1),
		patterned(3000, 2), // three fragments
		patterned(0, 3),
		patterned(fragTestMaxPayload, 4),
		patterned(5000, 5),
		patterned(20, 6),
	}
	for i, p := range payloads {
		if r, err := pub.Offer(egressMessage(int64(100+i), p)); err != nil || r <= 0 {
			t.Fatalf("setup offer %d: %d, %v", i, r, err)
		}
	}

	for i := 0; i < 10 && len(l.messages) < len(payloads); i++ {
		c.pollConnected()
	}

	if len(l.messages) != len(payloads) {
		t.Fatalf("listener got %d messages, want %d", len(l.messages), len(payloads))
	}
	for i, p := range payloads {
		if !bytes.Equal(l.messages[i], p) {
			t.Errorf("message %d: got %d bytes, want %d (content equal=%v)", i, len(l.messages[i]), len(p), bytes.Equal(l.messages[i], p))
		}
		if l.stamps[i] != int64(100+i) {
			t.Errorf("message %d: timestamp %d, want %d", i, l.stamps[i], 100+i)
		}
	}
}

// A message split across two Poll calls (the fragment limit is 10 per poll)
// must still be delivered once, which needs the assembler to keep its state
// between polls.
func TestClusterEgressFragmentsSpanPolls(t *testing.T) {
	c, pub, l := newEgressTestCluster(StateConnected)
	big := patterned(8000, 9) // 6 fragments
	for i := 0; i < 3; i++ {  // 18 fragments > 10 per poll
		if _, err := pub.Offer(egressMessage(int64(i), big)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10 && len(l.messages) < 3; i++ {
		c.pollConnected()
	}
	if len(l.messages) != 3 {
		t.Fatalf("listener got %d messages, want 3", len(l.messages))
	}
	for i, m := range l.messages {
		if !bytes.Equal(m, big) {
			t.Errorf("message %d corrupted (%d bytes)", i, len(m))
		}
	}
}

// The connect reply is an egress message too; it must still complete the
// handshake through the assembler.
func TestClusterEgressConnectReply(t *testing.T) {
	c, pub, l := newEgressTestCluster(StateAwaitConnectReply)
	c.correlationId = 42
	c.connectStartMs = time.Now().UnixMilli()
	c.cfg.ConnectTimeoutMs = int64(time.Hour / time.Millisecond)

	evt := SessionEvent{CorrelationId: 42, ClusterSessionId: 99, LeadershipTermId: 3, LeaderMemberId: 1, Code: EventCodeOK}
	buf := make([]byte, 512)
	n := evt.Encode(buf, 0)
	if r, err := pub.Offer(buf[:n]); err != nil || r <= 0 {
		t.Fatalf("setup offer: %d, %v", r, err)
	}

	c.awaitConnectReply()

	if c.State() != StateConnected {
		t.Fatalf("state = %v, want StateConnected", c.State())
	}
	if c.ClusterSessionId() != 99 || c.LeadershipTermId() != 3 || c.LeaderMemberId() != 1 {
		t.Errorf("session = %d term = %d leader = %d", c.ClusterSessionId(), c.LeadershipTermId(), c.LeaderMemberId())
	}
	if len(l.events) != 1 {
		t.Errorf("listener saw %d session events, want 1", len(l.events))
	}
}

// Handling egress must not allocate per poll once connected and idle.
func TestClusterEgressIdlePollDoesNotAllocate(t *testing.T) {
	c, _, _ := newEgressTestCluster(StateConnected)
	if allocs := testing.AllocsPerRun(100, func() { c.pollEgress(StateConnected) }); allocs != 0 {
		t.Errorf("idle pollEgress allocates %v times per call", allocs)
	}
}

type errorRecordingListener struct {
	recordingListener
	errs []error
}

func (r *errorRecordingListener) OnError(_ Cluster, err error) { r.errs = append(r.errs, err) }

// A frame too short for what its header says it holds comes off the network,
// not from this client, so it must be dropped and reported, not decoded past
// its end (which panicked) or handed to the listener as garbage.
func TestClusterEgressShortFrameIsDroppedAndReported(t *testing.T) {
	frame := func(blockLength, templateID uint16, body ...byte) []byte {
		buf := make([]byte, HeaderSize, HeaderSize+len(body))
		(&MessageHeader{BlockLength: blockLength, TemplateId: templateID, SchemaId: SchemaId, Version: SchemaVersion}).Encode(buf, 0)
		return append(buf, body...)
	}
	varLength := func(n uint32) []byte { return binary.LittleEndian.AppendUint32(nil, n) }
	cases := []struct {
		name  string
		stage ClusterState
		buf   []byte
	}{
		{"message header only", StateConnected, frame(0xFFFF, TemplateIdSessionMessageHeader)},
		{"session event header only", StateConnected, frame(0xFFFF, TemplateIdSessionEvent)},
		{"new leader header only", StateConnected, frame(0xFFFF, TemplateIdNewLeaderEvent)},
		{"challenge header only", StateConnected, frame(0xFFFF, TemplateIdChallenge)},
		{"connect reply header only", StateAwaitConnectReply, frame(0xFFFF, TemplateIdSessionEvent)},
		{"closing header only", StateClosing, frame(0xFFFF, TemplateIdSessionEvent)},
		{"message block shorter than its fields", StateConnected, frame(4, TemplateIdSessionMessageHeader, make([]byte, 4)...)},
		{"session event without its detail length", StateConnected, frame(sessionEventBlockLength, TemplateIdSessionEvent, make([]byte, sessionEventBlockLength)...)},
		{"session event detail past the end", StateConnected, frame(sessionEventBlockLength, TemplateIdSessionEvent, append(make([]byte, sessionEventBlockLength), varLength(1000)...)...)},
		{"new leader endpoints past the end", StateConnected, frame(newLeaderEventBlockLength, TemplateIdNewLeaderEvent, append(make([]byte, newLeaderEventBlockLength), varLength(1000)...)...)},
		{"challenge data past the end", StateConnected, frame(challengeBlockLength, TemplateIdChallenge, append(make([]byte, challengeBlockLength), varLength(1000)...)...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := &errorRecordingListener{}
			c := &AeronCluster{cfg: ClusterConfig{Listener: l}, state: tc.stage, egressStage: tc.stage}
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("dispatchEgress panicked: %v", r)
					}
				}()
				c.dispatchEgress(tc.buf, &Header{})
			}()
			if len(l.messages) != 0 || len(l.events) != 0 {
				t.Errorf("listener got %d messages and %d events from a short frame", len(l.messages), len(l.events))
			}
			if len(l.errs) != 1 || !errors.Is(l.errs[0], ErrCorruptFrame) {
				t.Errorf("errors reported = %v, want one matching ErrCorruptFrame", l.errs)
			}
		})
	}
}

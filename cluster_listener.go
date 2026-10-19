package aergo

// EgressListener receives messages from the Aeron cluster.
type EgressListener interface {
	// OnMessage delivers an application message from the cluster as
	// buffer[offset:offset+length]. As in the Java client, buffer is a view into
	// the log buffer (or the reassembly buffer for a fragmented message), not a
	// copy: it is valid only until OnMessage returns and must not be modified or
	// retained. Copy what you need to keep, for example before handing it to
	// another goroutine.
	OnMessage(cluster Cluster, timestamp int64, buffer []byte, offset int, length int)
	OnSessionEvent(cluster Cluster, event *SessionEvent)
	OnNewLeader(cluster Cluster, event *NewLeaderEvent)
	OnChallenge(cluster Cluster, challenge *Challenge) []byte
	// OnError reports a failure the poll loop encountered (subscription/
	// publication setup, connect rejection, disconnect, send failure). The
	// cluster has no synchronous caller to return these to -- Poll() just
	// drives the state machine forward -- so this is the channel for them
	// instead of an internal logger the caller can't control or silence.
	OnError(cluster Cluster, err error)
}

// NoopListener is a default no-op implementation of EgressListener.
type NoopListener struct{}

func (n *NoopListener) OnMessage(_ Cluster, _ int64, _ []byte, _ int, _ int) {}
func (n *NoopListener) OnSessionEvent(_ Cluster, _ *SessionEvent)            {}
func (n *NoopListener) OnNewLeader(_ Cluster, _ *NewLeaderEvent)             {}
func (n *NoopListener) OnChallenge(_ Cluster, _ *Challenge) []byte           { return nil }
func (n *NoopListener) OnError(_ Cluster, _ error)                           {}

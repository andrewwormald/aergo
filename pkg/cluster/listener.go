package cluster

import codec "github.com/andrewwormald/aergo/pkg/codec/cluster"

// EgressListener receives messages from the Aeron cluster.
type EgressListener interface {
	// OnMessage is called for each application message received from the cluster.
	// buffer[offset:offset+length] contains the application payload (after SessionMessageHeader).
	// Do not retain the buffer past this call.
	OnMessage(cluster Cluster, timestamp int64, buffer []byte, offset int, length int)

	// OnSessionEvent is called for session lifecycle events (connect, close, error, redirect).
	OnSessionEvent(cluster Cluster, event *codec.SessionEvent)

	// OnNewLeader is called when cluster leadership changes.
	OnNewLeader(cluster Cluster, event *codec.NewLeaderEvent)

	// OnChallenge is called when the cluster sends an authentication challenge.
	// Return the challenge response data, or nil to reject.
	OnChallenge(cluster Cluster, challenge *codec.Challenge) []byte
}

// NoopListener is a default no-op implementation of EgressListener.
type NoopListener struct{}

func (n *NoopListener) OnMessage(_ Cluster, _ int64, _ []byte, _ int, _ int)   {}
func (n *NoopListener) OnSessionEvent(_ Cluster, _ *codec.SessionEvent)        {}
func (n *NoopListener) OnNewLeader(_ Cluster, _ *codec.NewLeaderEvent)         {}
func (n *NoopListener) OnChallenge(_ Cluster, _ *codec.Challenge) []byte       { return nil }

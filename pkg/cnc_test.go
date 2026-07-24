package aeron

import "testing"

// DriverHeartbeat must read the same trailer field the driver conductor
// refreshes: the to-driver ring buffer's consumer heartbeat (what Java's
// CommonContext.isDriverActive reads via consumerHeartbeatTime()).
func TestDriverHeartbeatReadsToDriverConsumerHeartbeat(t *testing.T) {
	cnc := newInMemCnc(1)
	rb, err := NewManyToOneRingBuffer(cnc.ToDriverBuffer)
	if err != nil {
		t.Fatalf("NewManyToOneRingBuffer: %v", err)
	}

	if got := cnc.DriverHeartbeat(); got != 0 {
		t.Fatalf("DriverHeartbeat before any keepalive: got %d, want 0", got)
	}

	const wantMs = int64(1_234_567_890)
	setInMemDriverHeartbeat(cnc, wantMs)

	if got := rb.ConsumerHeartbeatTime(); got != wantMs {
		t.Fatalf("ConsumerHeartbeatTime: got %d, want %d", got, wantMs)
	}
	if got := cnc.DriverHeartbeat(); got != wantMs {
		t.Fatalf("DriverHeartbeat: got %d, want %d", got, wantMs)
	}
}

// The driver heartbeat must not be read from the counter values buffer:
// counter 0 on a real driver is a system counter (bytes sent), not a
// timestamp, which caused false "media driver inactive" errors.
func TestDriverHeartbeatIgnoresCounterZero(t *testing.T) {
	cnc := newInMemCnc(1)
	cnc.CounterValues.PutInt64Ordered(0, 42) // system counter noise

	if got := cnc.DriverHeartbeat(); got != 0 {
		t.Fatalf("DriverHeartbeat: got %d, want 0 (must ignore counter 0)", got)
	}
}

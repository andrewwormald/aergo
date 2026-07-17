package aeron

import (
	"errors"
	"testing"
	"time"
)

// TestAddPublicationReturnsDriverError delivers a RespOnError for the
// pending correlation ID through a fake to-clients broadcast buffer and
// asserts the blocked Add* call fails with the driver's rejection instead of
// hanging until the wait timeout.
func TestAddPublicationReturnsDriverError(t *testing.T) {
	prev := PublicationWaitTimeoutForTesting(200 * time.Millisecond)
	t.Cleanup(func() { PublicationWaitTimeoutForTesting(prev) })

	tests := []struct {
		name string
		add  func(a *Aeron) error
	}{
		{"publication", func(a *Aeron) error {
			_, err := a.AddPublication("aeron:ipc", 101)
			return err
		}},
		{"exclusive publication", func(a *Aeron) error {
			_, err := a.AddExclusivePublication("aeron:ipc", 101)
			return err
		}},
		{"subscription", func(a *Aeron) error {
			_, err := a.AddSubscription("aeron:ipc", 101)
			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, bcast := newInMemConductor(42)
			a := &Aeron{conductor: c}

			// The next correlation ID the proxy hands out is predictable, so
			// the rejection can be queued in the broadcast buffer before the
			// Add* call starts polling for it.
			nextCorrID := c.proxy.NextCorrelationID() + 1
			driverMsg := "no available channel endpoint"
			bcast.transmit(RespOnError, encodeErrorResponse(nextCorrID, 4 /* CHANNEL_ENDPOINT_ERROR */, driverMsg))

			err := tc.add(a)
			if err == nil {
				t.Fatal("add: got nil error, want driver rejection")
			}
			var regErr *RegistrationError
			if !errors.As(err, &regErr) {
				t.Fatalf("error type: got %T (%v), want *RegistrationError", err, err)
			}
			if regErr.CorrelationID != nextCorrID || regErr.Message != driverMsg {
				t.Errorf("RegistrationError fields: got %+v", regErr)
			}
		})
	}
}

// TestAddFailsFastWhenDriverDead asserts a stale driver heartbeat makes Add*
// return a DriverTimeoutError immediately instead of hanging until the
// publication wait timeout.
func TestAddFailsFastWhenDriverDead(t *testing.T) {
	c, _ := newInMemConductor(42)
	setInMemDriverHeartbeat(c.cnc, time.Now().UnixMilli()-c.driverTimeoutNs/1_000_000-1)
	a := &Aeron{conductor: c}

	start := time.Now()
	_, err := a.AddPublication("aeron:ipc", 101)
	elapsed := time.Since(start)

	var timeoutErr *DriverTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("AddPublication: got %v, want *DriverTimeoutError", err)
	}
	if elapsed > time.Second {
		t.Errorf("AddPublication took %v, want fail-fast", elapsed)
	}
}

// TestAddFailsFastAfterClientTimeout asserts Add* calls fail immediately
// once the driver has timed out this client.
func TestAddFailsFastAfterClientTimeout(t *testing.T) {
	c, bcast := newInMemConductor(42)
	a := &Aeron{conductor: c}

	msg := make([]byte, 8)
	msg[0] = 42 // clientID little-endian
	bcast.transmit(RespOnClientTimeout, msg)
	c.DoWork()

	var timeoutErr *ClientTimeoutError
	if !c.isTerminated() {
		t.Fatal("conductor not terminated after client timeout event")
	}

	if _, err := a.AddPublication("aeron:ipc", 101); !errors.As(err, &timeoutErr) {
		t.Errorf("AddPublication: got %v, want *ClientTimeoutError", err)
	}
	if _, err := a.AddSubscription("aeron:ipc", 101); !errors.As(err, &timeoutErr) {
		t.Errorf("AddSubscription: got %v, want *ClientTimeoutError", err)
	}
}

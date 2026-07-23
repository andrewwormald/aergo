package aeron

import "testing"

func TestDriverProxyAddPublication(t *testing.T) {
	rb := newTestRingBuffer(4096)
	proxy := NewDriverProxy(rb, 42)

	corrID := proxy.AddPublication("aeron:udp?endpoint=localhost:40123", 1001)
	if corrID < 0 {
		t.Fatalf("AddPublication failed: %d", corrID)
	}
}

func TestDriverProxyAddSubscription(t *testing.T) {
	rb := newTestRingBuffer(4096)
	proxy := NewDriverProxy(rb, 42)

	corrID := proxy.AddSubscription("aeron:udp?endpoint=localhost:40123", 1001)
	if corrID < 0 {
		t.Fatalf("AddSubscription failed: %d", corrID)
	}
}

func TestDriverProxyRemovePublication(t *testing.T) {
	rb := newTestRingBuffer(4096)
	proxy := NewDriverProxy(rb, 42)

	corrID := proxy.RemovePublication(100)
	if corrID < 0 {
		t.Fatalf("RemovePublication failed: %d", corrID)
	}
}

func TestDriverProxyKeepalive(t *testing.T) {
	rb := newTestRingBuffer(4096)
	proxy := NewDriverProxy(rb, 42)

	if !proxy.SendClientKeepalive() {
		t.Fatal("keepalive failed")
	}
}

func TestDriverProxyClientClose(t *testing.T) {
	rb := newTestRingBuffer(4096)
	proxy := NewDriverProxy(rb, 42)

	if !proxy.ClientClose() {
		t.Fatal("client close failed")
	}
}

func TestDriverProxyCorrelationIDs(t *testing.T) {
	rb := newTestRingBuffer(4096)
	proxy := NewDriverProxy(rb, 42)

	id1 := proxy.NextCorrelationID()
	id2 := proxy.NextCorrelationID()
	if id2 <= id1 {
		t.Errorf("IDs should be increasing: %d, %d", id1, id2)
	}
}

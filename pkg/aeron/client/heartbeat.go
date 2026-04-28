package client

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/andrewwormald/aergo/pkg/aeron/driver"
)

// HeartbeatMonitor monitors the Aeron media driver heartbeat via the CnC file.
// If the heartbeat stops updating, the media driver has crashed or hung.
type HeartbeatMonitor struct {
	cnc              unsafe.Pointer
	lastHeartbeat    int64
	staleThresholdMs int64
	onStale          func()
}

// NewHeartbeatMonitor creates a monitor for the media driver at the given directory.
// staleThresholdMs is how long the heartbeat can be stale before triggering onStale.
// Typical value: 10000 (10 seconds).
func NewHeartbeatMonitor(aeronDir string, staleThresholdMs int64, onStale func()) (*HeartbeatMonitor, error) {
	var cnc unsafe.Pointer
	if err := driver.CncInit(&cnc, aeronDir, 5000); err != nil {
		return nil, fmt.Errorf("cnc init: %w", err)
	}

	return &HeartbeatMonitor{
		cnc:              cnc,
		staleThresholdMs: staleThresholdMs,
		onStale:          onStale,
	}, nil
}

// Check reads the current heartbeat and returns true if the driver is alive.
// Calls onStale if the heartbeat is older than the threshold.
func (m *HeartbeatMonitor) Check() bool {
	heartbeat := driver.CncToDriverHeartbeat(m.cnc)
	nowMs := time.Now().UnixMilli()

	if heartbeat != m.lastHeartbeat {
		m.lastHeartbeat = heartbeat
		return true
	}

	// Heartbeat hasn't changed -- check staleness
	if m.lastHeartbeat > 0 && nowMs-m.lastHeartbeat > m.staleThresholdMs {
		if m.onStale != nil {
			m.onStale()
		}
		return false
	}

	return true
}

// Close releases the CnC handle.
func (m *HeartbeatMonitor) Close() {
	if m.cnc != nil {
		driver.CncClose(m.cnc)
		m.cnc = nil
	}
}

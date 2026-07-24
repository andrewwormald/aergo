package aeron

import "time"

// Counter metadata layout (per counter).
const (
	CounterStateOffset        = 0
	CounterTypeIdOffset       = 4
	CounterFreeDeadlineOffset = 8
	CounterKeyOffset          = 16
	CounterKeyRegIdOffset     = 0 // HeartbeatTimestamp.REGISTRATION_ID_OFFSET = 0
	CounterLabelOffset        = 128
	CounterFullLabelLength    = 384
	CounterMetadataLength     = CounterLabelOffset + CounterFullLabelLength // 512
	CounterValueLength        = CacheLineLength * 2                         // 128

	CounterRecordAllocated = 1
	CounterRecordUnused    = 0

	// AeronCounters.DRIVER_HEARTBEAT_TYPE_ID: per-client heartbeat counters
	// keyed by registration id. The driver has no heartbeat counter of its
	// own — its liveness signal is the to-driver ring buffer's
	// consumer-heartbeat trailer field (see MappedCnc.DriverHeartbeat).
	HeartbeatTypeID = 11
)

// FindHeartbeatCounter scans the counter metadata buffer for the heartbeat
// counter allocated by the driver for the given clientID.
// Returns the counter ID, or -1 if not found.
func FindHeartbeatCounter(metaBuf, valuesBuf *AtomicBuffer, clientID int64) int32 {
	maxCounterId := (valuesBuf.Capacity() / CounterValueLength) - 1

	for counterId := int32(0); counterId <= maxCounterId; counterId++ {
		metaOffset := counterId * CounterMetadataLength
		if metaOffset+CounterMetadataLength > metaBuf.Capacity() {
			break
		}

		state := metaBuf.GetInt32Volatile(metaOffset + CounterStateOffset)
		if state == CounterRecordUnused {
			break
		}
		if state != CounterRecordAllocated {
			continue
		}

		typeId := metaBuf.GetInt32(metaOffset + CounterTypeIdOffset)
		if typeId != HeartbeatTypeID {
			continue
		}

		regId := metaBuf.GetInt64(metaOffset + CounterKeyOffset + CounterKeyRegIdOffset)
		if regId == clientID {
			return counterId
		}
	}
	return -1
}

// UpdateHeartbeatCounter writes the current time (ms) to the heartbeat counter value.
func UpdateHeartbeatCounter(valuesBuf *AtomicBuffer, counterId int32) {
	offset := counterId * CounterValueLength
	valuesBuf.PutInt64Ordered(offset, time.Now().UnixMilli())
}

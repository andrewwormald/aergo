package aeron

import (
	"log"
	"time"
)

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

	HeartbeatTypeID = 11 // AeronCounters.DRIVER_HEARTBEAT_TYPE_ID
)

// FindHeartbeatCounter scans the counter metadata buffer for the heartbeat
// counter allocated by the driver for the given clientID.
// Returns the counter ID, or -1 if not found.
func FindHeartbeatCounter(metaBuf, valuesBuf *AtomicBuffer, clientID int64) int32 {
	maxCounterId := (valuesBuf.Capacity() / CounterValueLength) - 1

	scanned := 0
	allocated := 0
	typeMatches := 0

	for counterId := int32(0); counterId <= maxCounterId; counterId++ {
		metaOffset := counterId * CounterMetadataLength
		if metaOffset+CounterMetadataLength > metaBuf.Capacity() {
			break
		}

		state := metaBuf.GetInt32Volatile(metaOffset + CounterStateOffset)
		if state == CounterRecordUnused {
			break
		}
		scanned++
		if state != CounterRecordAllocated {
			continue
		}
		allocated++

		typeId := metaBuf.GetInt32(metaOffset + CounterTypeIdOffset)
		if typeId == HeartbeatTypeID {
			typeMatches++
			regId := metaBuf.GetInt64(metaOffset + CounterKeyOffset + CounterKeyRegIdOffset)

			// Read label for diagnostics
			labelLen := metaBuf.GetInt32(metaOffset + CounterLabelOffset)
			label := ""
			if labelLen > 0 && labelLen < 200 {
				labelBytes := make([]byte, labelLen)
				metaBuf.GetBytes(metaOffset+CounterLabelOffset+4, labelBytes)
				label = string(labelBytes)
			}

			log.Printf("counters: heartbeat counter id=%d regId=%d (want %d) label=%q",
				counterId, regId, clientID, label)

			if regId == clientID {
				return counterId
			}
		}
	}

	log.Printf("counters: scan complete: scanned=%d allocated=%d typeMatches=%d looking for clientID=%d",
		scanned, allocated, typeMatches, clientID)
	return -1
}

// UpdateHeartbeatCounter writes the current time (ms) to the heartbeat counter value.
func UpdateHeartbeatCounter(valuesBuf *AtomicBuffer, counterId int32) {
	offset := counterId * CounterValueLength
	valuesBuf.PutInt64Ordered(offset, time.Now().UnixMilli())
}

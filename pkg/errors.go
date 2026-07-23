package aeron

import "fmt"

// Offer return codes. Values and semantics match the Java client's
// io.aeron.Publication constants (io.aeron 1.51.0).
const (
	// NotConnected means the publication is not connected to a subscriber.
	// This can be an intermittent state as subscribers come and go.
	NotConnected int64 = -1

	// BackPressured means the offer failed due to back pressure from the
	// subscribers preventing further transmission; retry once subscribers
	// have consumed.
	BackPressured int64 = -2

	// AdminAction means the offer failed because of an administrative action
	// (such as a log rotation) which is likely to have completed by the next
	// retry attempt; retry the offer.
	AdminAction int64 = -3

	// Closed means the publication has been closed and should no longer be
	// used.
	Closed int64 = -4

	// MaxPositionExceeded means the offer failed because the stream reached
	// its maximum possible position: term buffer length times the total
	// possible number of terms. The publication should be closed and a new
	// one added; a larger term buffer length makes this less likely.
	MaxPositionExceeded int64 = -5
)

// driverErrorCodeNames maps media driver error codes to their names,
// mirroring io.aeron.ErrorCode.
var driverErrorCodeNames = map[int32]string{
	0:  "UNUSED",
	1:  "INVALID_CHANNEL",
	2:  "UNKNOWN_SUBSCRIPTION",
	3:  "UNKNOWN_PUBLICATION",
	4:  "CHANNEL_ENDPOINT_ERROR",
	5:  "UNKNOWN_COUNTER",
	6:  "UNKNOWN_COMMAND_TYPE_ID",
	7:  "MALFORMED_COMMAND",
	8:  "NOT_SUPPORTED",
	9:  "UNKNOWN_HOST",
	10: "RESOURCE_TEMPORARILY_UNAVAILABLE",
	11: "GENERIC_ERROR",
	12: "STORAGE_SPACE",
	13: "IMAGE_REJECTED",
	14: "PUBLICATION_REVOKED",
}

// driverErrorCodeName returns the io.aeron.ErrorCode name for a driver error
// code, or "UNKNOWN_CODE_VALUE" when the code is unknown to this client.
func driverErrorCodeName(code int32) string {
	if name, ok := driverErrorCodeNames[code]; ok {
		return name
	}
	return "UNKNOWN_CODE_VALUE"
}

// RegistrationError is the driver's rejection of a command to register a
// resource (add publication/subscription). Mirrors Java's
// io.aeron.exceptions.RegistrationException.
type RegistrationError struct {
	// CorrelationID of the offending command.
	CorrelationID int64
	// Code is the media driver error code (io.aeron.ErrorCode value).
	Code int32
	// Message is the driver-provided error detail.
	Message string
}

func (e *RegistrationError) Error() string {
	return fmt.Sprintf("driver rejected command (correlationID=%d): %s (errorCode=%s [%d])",
		e.CorrelationID, e.Message, driverErrorCodeName(e.Code), e.Code)
}

// DriverTimeoutError indicates the media driver's heartbeat timestamp is
// older than the driver timeout, i.e. the driver is dead or unresponsive.
// Mirrors Java's io.aeron.exceptions.DriverTimeoutException.
type DriverTimeoutError struct {
	// HeartbeatAgeMs is how old the driver heartbeat timestamp is.
	HeartbeatAgeMs int64
	// TimeoutMs is the configured driver timeout.
	TimeoutMs int64
}

func (e *DriverTimeoutError) Error() string {
	return fmt.Sprintf("media driver inactive: heartbeat age %dms exceeds driver timeout %dms",
		e.HeartbeatAgeMs, e.TimeoutMs)
}

// ClientTimeoutError indicates the driver timed out this client (it stopped
// receiving our keepalives) and released the client's resources. The client
// is closed-equivalent: Offers return Closed and Add* calls fail. Mirrors
// Java's io.aeron.exceptions.ClientTimeoutException.
type ClientTimeoutError struct{}

func (e *ClientTimeoutError) Error() string {
	return "driver timed out this client (keepalives not received by driver); create a new client"
}

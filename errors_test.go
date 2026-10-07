package aergo

import (
	"strings"
	"testing"
)

func TestDriverErrorCodeName(t *testing.T) {
	tests := []struct {
		code int32
		want string
	}{
		{0, "UNUSED"},
		{1, "INVALID_CHANNEL"},
		{10, "RESOURCE_TEMPORARILY_UNAVAILABLE"},
		{14, "PUBLICATION_REVOKED"},
		{99, "UNKNOWN_CODE_VALUE"},
		{-7, "UNKNOWN_CODE_VALUE"},
	}
	for _, tc := range tests {
		if got := driverErrorCodeName(tc.code); got != tc.want {
			t.Errorf("driverErrorCodeName(%d): got %q, want %q", tc.code, got, tc.want)
		}
	}
}

func TestRegistrationErrorMessage(t *testing.T) {
	err := &RegistrationError{CorrelationID: 17, Code: 1, Message: "channel is wonky"}
	for _, want := range []string{"INVALID_CHANNEL", "channel is wonky", "17"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("RegistrationError %q missing %q", err.Error(), want)
		}
	}
}

func TestOfferReturnCodeValues(t *testing.T) {
	// Values must match io.aeron.Publication exactly.
	tests := []struct {
		name string
		code int64
		want int64
	}{
		{"NotConnected", NotConnected, -1},
		{"BackPressured", BackPressured, -2},
		{"AdminAction", AdminAction, -3},
		{"Closed", Closed, -4},
		{"MaxPositionExceeded", MaxPositionExceeded, -5},
	}
	for _, tc := range tests {
		if tc.code != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, tc.code, tc.want)
		}
	}
}

func TestOfferResultName(t *testing.T) {
	tests := []struct {
		result int64
		want   string
	}{
		{NotConnected, "NOT_CONNECTED"},
		{BackPressured, "BACK_PRESSURED"},
		{AdminAction, "ADMIN_ACTION"},
		{Closed, "CLOSED"},
		{MaxPositionExceeded, "MAX_POSITION_EXCEEDED"},
		{0, "UNKNOWN"},
		{128, "UNKNOWN"},
	}
	for _, tc := range tests {
		if got := OfferResultName(tc.result); got != tc.want {
			t.Errorf("OfferResultName(%d): got %q, want %q", tc.result, got, tc.want)
		}
	}
}

func TestOfferResultClassification(t *testing.T) {
	tests := []struct {
		result    int64
		retryable bool
		gone      bool
	}{
		{NotConnected, false, true},
		{BackPressured, true, false},
		{AdminAction, true, false},
		{Closed, false, true},
		{MaxPositionExceeded, false, true},
		{0, false, false},
		{64, false, false},
	}
	for _, tc := range tests {
		if got := OfferRetryable(tc.result); got != tc.retryable {
			t.Errorf("OfferRetryable(%d): got %v, want %v", tc.result, got, tc.retryable)
		}
		if got := OfferGone(tc.result); got != tc.gone {
			t.Errorf("OfferGone(%d): got %v, want %v", tc.result, got, tc.gone)
		}
	}
}

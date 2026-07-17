package aeron

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

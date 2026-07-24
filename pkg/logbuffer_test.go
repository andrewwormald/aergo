package aeron

import "testing"

// TestFrameTypeWireValues pins FrameTypeData/FrameTypePadding to Aeron's real
// wire-format values, per the decompiled io.aeron.protocol.HeaderFlyweight /
// DataHeaderFlyweight reference used during the SessionConnectRequest
// investigation: HDR_TYPE_PAD = 0x00, HDR_TYPE_DATA = 0x01. FrameTypeData was
// previously 0x06, an internal-only value never recognized by a real media
// driver, so publications were silently dropped. A test that only checks
// internal consistency (FrameTypeData used where FrameTypeData is expected)
// would not have caught that regression.
func TestFrameTypeWireValues(t *testing.T) {
	if FrameTypePadding != 0 {
		t.Fatalf("FrameTypePadding = %#x, want 0x00 (HDR_TYPE_PAD)", FrameTypePadding)
	}
	if FrameTypeData != 1 {
		t.Fatalf("FrameTypeData = %#x, want 0x01 (HDR_TYPE_DATA)", FrameTypeData)
	}
}

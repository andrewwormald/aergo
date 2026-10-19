package aergo

import "testing"

// ReadTerm stops at a committed length no frame can have, as Subscription.Poll
// does, instead of handing the handler a negative length or walking past the
// term.
func TestReadTermStopsAtCorruptFrameLength(t *testing.T) {
	for _, length := range []int32{8, DataFrameHeaderLen - 1, 2048} {
		term := NewAtomicBuffer(make([]byte, 1024))
		term.PutInt32(FrameLengthOffset, DataFrameHeaderLen+4) // one good frame
		term.PutInt16(FrameTypeOffset, FrameTypeData)
		term.PutInt32(64+FrameLengthOffset, length)
		term.PutInt16(64+FrameTypeOffset, FrameTypeData)

		var lengths []int32
		n, offset := ReadTerm(term, 0, func(_ *AtomicBuffer, _, l int32, _ *DataFrameHeader) {
			lengths = append(lengths, l)
		}, 10)
		if n != 1 || offset != 64 || len(lengths) != 1 || lengths[0] != 4 {
			t.Errorf("frame length %d: read %d fragments %v, stopped at %d; want 1 [4], 64", length, n, lengths, offset)
		}
	}
}

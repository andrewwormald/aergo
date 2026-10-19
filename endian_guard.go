//go:build armbe || arm64be || mips || mips64 || ppc64 || s390x || sparc64

package aergo

// The log buffer frame header is little-endian and is written and read as
// native 64-bit words (putFrameHeader), as Java's HeaderWriter does on a
// little-endian platform. Building for a big-endian target would write wrong
// headers, so refuse to build.
var _ = bigEndianTargetsAreNotSupported

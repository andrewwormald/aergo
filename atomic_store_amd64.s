//go:build amd64 && !race

#include "textflag.h"

// func storeRelease32(p *int32, v int32)
//
// A plain aligned store. On amd64 (x86-TSO) every store already has release
// semantics: it cannot become visible before an earlier store. Calling an
// assembly function also stops the compiler moving earlier stores past it.
// sync/atomic.StoreInt32 would be an XCHG instead, which waits for every
// pending store (here, a whole message payload) to drain before continuing.
TEXT ·storeRelease32(SB), NOSPLIT, $0-12
	MOVQ p+0(FP), AX
	MOVL v+8(FP), BX
	MOVL BX, (AX)
	RET

// func storeRelease64(p *int64, v int64)
TEXT ·storeRelease64(SB), NOSPLIT, $0-16
	MOVQ p+0(FP), AX
	MOVQ v+8(FP), BX
	MOVQ BX, (AX)
	RET

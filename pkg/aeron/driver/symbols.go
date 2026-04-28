package driver

import (
	"unsafe"

	"github.com/ebitengine/purego"
)

// ---------------------------------------------------------------------------
// Aeron C client function pointers -- resolved at Open() time via purego.
//
// All opaque C types (aeron_t*, aeron_context_t*, aeron_publication_t*, etc.)
// are represented as unsafe.Pointer. We never dereference struct internals.
// ---------------------------------------------------------------------------

// -- Utility ----------------------------------------------------------------

var aeronVersionFull func() string
var aeronErrmsg func() string
var aeronErrcode func() int32

// VersionFull returns the Aeron C client version string.
func VersionFull() string { return aeronVersionFull() }

// -- Context ----------------------------------------------------------------

var aeronContextInit func(ctx *unsafe.Pointer) int32
var aeronContextSetDir func(ctx unsafe.Pointer, dir string) int32
var aeronContextSetDriverTimeoutMs func(ctx unsafe.Pointer, ms uint64) int32
var aeronContextSetErrorHandlerFn func(ctx unsafe.Pointer, handler uintptr, clientd unsafe.Pointer) int32
var aeronContextSetOnNewPublicationFn func(ctx unsafe.Pointer, handler uintptr, clientd unsafe.Pointer) int32
var aeronContextSetOnNewSubscriptionFn func(ctx unsafe.Pointer, handler uintptr, clientd unsafe.Pointer) int32
var aeronContextSetOnAvailableCounterFn func(ctx unsafe.Pointer, handler uintptr, clientd unsafe.Pointer) int32
var aeronContextSetOnUnavailableCounterFn func(ctx unsafe.Pointer, handler uintptr, clientd unsafe.Pointer) int32
var aeronContextClose func(ctx unsafe.Pointer) int32

func ContextInit(ctx *unsafe.Pointer) error {
	return CheckResult(aeronContextInit(ctx))
}

func ContextSetDir(ctx unsafe.Pointer, dir string) error {
	return CheckResult(aeronContextSetDir(ctx, dir))
}

func ContextSetDriverTimeoutMs(ctx unsafe.Pointer, ms uint64) error {
	return CheckResult(aeronContextSetDriverTimeoutMs(ctx, ms))
}

func ContextSetErrorHandler(ctx unsafe.Pointer, handler uintptr, clientd unsafe.Pointer) error {
	return CheckResult(aeronContextSetErrorHandlerFn(ctx, handler, clientd))
}

func ContextClose(ctx unsafe.Pointer) error {
	return CheckResult(aeronContextClose(ctx))
}

// -- Client lifecycle -------------------------------------------------------

var aeronInit func(client *unsafe.Pointer, ctx unsafe.Pointer) int32
var aeronStart func(client unsafe.Pointer) int32
var aeronClose func(client unsafe.Pointer) int32
var aeronMainDoWork func(client unsafe.Pointer) int32
var aeronMainIdleStrategy func(client unsafe.Pointer, workCount int32)
var aeronNextCorrelationId func(client unsafe.Pointer) int64
var aeronClientId func(client unsafe.Pointer) int64

func ClientInit(client *unsafe.Pointer, ctx unsafe.Pointer) error {
	return CheckResult(aeronInit(client, ctx))
}

func ClientStart(client unsafe.Pointer) error {
	return CheckResult(aeronStart(client))
}

func ClientClose(client unsafe.Pointer) error {
	return CheckResult(aeronClose(client))
}

func ClientDoWork(client unsafe.Pointer) int32 {
	return aeronMainDoWork(client)
}

func ClientIdleStrategy(client unsafe.Pointer, workCount int32) {
	aeronMainIdleStrategy(client, workCount)
}

func NextCorrelationId(client unsafe.Pointer) int64 {
	return aeronNextCorrelationId(client)
}

func ClientId(client unsafe.Pointer) int64 {
	return aeronClientId(client)
}

// -- Publication (async) ----------------------------------------------------

var aeronAsyncAddPublication func(async *unsafe.Pointer, client unsafe.Pointer, uri string, streamId int32) int32
var aeronAsyncAddPublicationPoll func(pub *unsafe.Pointer, async unsafe.Pointer) int32
var aeronPublicationOffer func(pub unsafe.Pointer, buffer unsafe.Pointer, length uintptr, reservedValueSupplier uintptr, clientd unsafe.Pointer) int64
var aeronPublicationTryClaim func(pub unsafe.Pointer, length uintptr, claim unsafe.Pointer) int64
var aeronPublicationClose func(pub unsafe.Pointer, onComplete uintptr, clientd unsafe.Pointer) int32
var aeronPublicationIsConnected func(pub unsafe.Pointer) bool
var aeronPublicationIsClosed func(pub unsafe.Pointer) bool
var aeronPublicationChannelStatus func(pub unsafe.Pointer) int64
var aeronPublicationStreamId func(pub unsafe.Pointer) int32
var aeronPublicationSessionId func(pub unsafe.Pointer) int32

// Exclusive publication variants
var aeronAsyncAddExclusivePublication func(async *unsafe.Pointer, client unsafe.Pointer, uri string, streamId int32) int32
var aeronAsyncAddExclusivePublicationPoll func(pub *unsafe.Pointer, async unsafe.Pointer) int32
var aeronExclusivePublicationOffer func(pub unsafe.Pointer, buffer unsafe.Pointer, length uintptr, reservedValueSupplier uintptr, clientd unsafe.Pointer) int64
var aeronExclusivePublicationTryClaim func(pub unsafe.Pointer, length uintptr, claim unsafe.Pointer) int64
var aeronExclusivePublicationClose func(pub unsafe.Pointer, onComplete uintptr, clientd unsafe.Pointer) int32
var aeronExclusivePublicationIsConnected func(pub unsafe.Pointer) bool
var aeronExclusivePublicationIsClosed func(pub unsafe.Pointer) bool

func AsyncAddPublication(async *unsafe.Pointer, client unsafe.Pointer, uri string, streamId int32) error {
	return CheckResult(aeronAsyncAddPublication(async, client, uri, streamId))
}

func AsyncAddPublicationPoll(pub *unsafe.Pointer, async unsafe.Pointer) int32 {
	return aeronAsyncAddPublicationPoll(pub, async)
}

func PublicationOffer(pub unsafe.Pointer, buffer unsafe.Pointer, length uintptr) int64 {
	return aeronPublicationOffer(pub, buffer, length, 0, nil)
}

func PublicationTryClaim(pub unsafe.Pointer, length uintptr, claim unsafe.Pointer) int64 {
	return aeronPublicationTryClaim(pub, length, claim)
}

func PublicationClose(pub unsafe.Pointer) error {
	return CheckResult(aeronPublicationClose(pub, 0, nil))
}

func PublicationIsConnected(pub unsafe.Pointer) bool {
	return aeronPublicationIsConnected(pub)
}

func PublicationIsClosed(pub unsafe.Pointer) bool {
	return aeronPublicationIsClosed(pub)
}

func PublicationChannelStatus(pub unsafe.Pointer) int64 {
	return aeronPublicationChannelStatus(pub)
}

func PublicationStreamId(pub unsafe.Pointer) int32 {
	return aeronPublicationStreamId(pub)
}

func PublicationSessionId(pub unsafe.Pointer) int32 {
	return aeronPublicationSessionId(pub)
}

// Exclusive publication helpers
func AsyncAddExclusivePublication(async *unsafe.Pointer, client unsafe.Pointer, uri string, streamId int32) error {
	return CheckResult(aeronAsyncAddExclusivePublication(async, client, uri, streamId))
}

func AsyncAddExclusivePublicationPoll(pub *unsafe.Pointer, async unsafe.Pointer) int32 {
	return aeronAsyncAddExclusivePublicationPoll(pub, async)
}

func ExclusivePublicationOffer(pub unsafe.Pointer, buffer unsafe.Pointer, length uintptr) int64 {
	return aeronExclusivePublicationOffer(pub, buffer, length, 0, nil)
}

func ExclusivePublicationClose(pub unsafe.Pointer) error {
	return CheckResult(aeronExclusivePublicationClose(pub, 0, nil))
}

func ExclusivePublicationIsConnected(pub unsafe.Pointer) bool {
	return aeronExclusivePublicationIsConnected(pub)
}

func ExclusivePublicationIsClosed(pub unsafe.Pointer) bool {
	return aeronExclusivePublicationIsClosed(pub)
}

func ExclusivePublicationTryClaim(pub unsafe.Pointer, length uintptr, claim unsafe.Pointer) int64 {
	return aeronExclusivePublicationTryClaim(pub, length, claim)
}

// -- Subscription (async) ---------------------------------------------------

var aeronAsyncAddSubscription func(async *unsafe.Pointer, client unsafe.Pointer, uri string, streamId int32, onAvailableImage uintptr, onAvailableClientd unsafe.Pointer, onUnavailableImage uintptr, onUnavailableClientd unsafe.Pointer) int32
var aeronAsyncAddSubscriptionPoll func(sub *unsafe.Pointer, async unsafe.Pointer) int32
var aeronSubscriptionPoll func(sub unsafe.Pointer, handler uintptr, clientd unsafe.Pointer, fragmentLimit int32) int32
var aeronSubscriptionClose func(sub unsafe.Pointer, onComplete uintptr, clientd unsafe.Pointer) int32
var aeronSubscriptionIsConnected func(sub unsafe.Pointer) bool
var aeronSubscriptionIsClosed func(sub unsafe.Pointer) bool
var aeronSubscriptionChannelStatus func(sub unsafe.Pointer) int64

func AsyncAddSubscription(async *unsafe.Pointer, client unsafe.Pointer, uri string, streamId int32, onAvailableImage uintptr, onAvailableClientd unsafe.Pointer, onUnavailableImage uintptr, onUnavailableClientd unsafe.Pointer) error {
	return CheckResult(aeronAsyncAddSubscription(async, client, uri, streamId, onAvailableImage, onAvailableClientd, onUnavailableImage, onUnavailableClientd))
}

func AsyncAddSubscriptionPoll(sub *unsafe.Pointer, async unsafe.Pointer) int32 {
	return aeronAsyncAddSubscriptionPoll(sub, async)
}

func SubscriptionPoll(sub unsafe.Pointer, handler uintptr, clientd unsafe.Pointer, fragmentLimit int32) int32 {
	return aeronSubscriptionPoll(sub, handler, clientd, fragmentLimit)
}

func SubscriptionClose(sub unsafe.Pointer) error {
	return CheckResult(aeronSubscriptionClose(sub, 0, nil))
}

func SubscriptionIsConnected(sub unsafe.Pointer) bool {
	return aeronSubscriptionIsConnected(sub)
}

func SubscriptionIsClosed(sub unsafe.Pointer) bool {
	return aeronSubscriptionIsClosed(sub)
}

func SubscriptionChannelStatus(sub unsafe.Pointer) int64 {
	return aeronSubscriptionChannelStatus(sub)
}

// -- Fragment assembler -----------------------------------------------------

var aeronFragmentAssemblerCreate func(assembler *unsafe.Pointer, delegate uintptr, clientd unsafe.Pointer) int32
var aeronFragmentAssemblerDelete func(assembler unsafe.Pointer) int32

// FragmentAssemblerHandlerPtr is resolved via Dlsym -- it's the C function
// pointer to aeron_fragment_assembler_handler which is passed directly as
// the handler argument to aeron_subscription_poll.
var FragmentAssemblerHandlerPtr uintptr

func FragmentAssemblerCreate(assembler *unsafe.Pointer, delegate uintptr, clientd unsafe.Pointer) error {
	return CheckResult(aeronFragmentAssemblerCreate(assembler, delegate, clientd))
}

func FragmentAssemblerDelete(assembler unsafe.Pointer) error {
	return CheckResult(aeronFragmentAssemblerDelete(assembler))
}

// -- Buffer claim -----------------------------------------------------------

var aeronBufferClaimCommit func(claim unsafe.Pointer) int32
var aeronBufferClaimAbort func(claim unsafe.Pointer) int32

func BufferClaimCommit(claim unsafe.Pointer) error {
	return CheckResult(aeronBufferClaimCommit(claim))
}

func BufferClaimAbort(claim unsafe.Pointer) error {
	return CheckResult(aeronBufferClaimAbort(claim))
}

// -- CnC (Command and Control) -- media driver heartbeat --------------------

var aeronCncInit func(cnc *unsafe.Pointer, basePath string, timeoutMs int64) int32
var aeronCncClose func(cnc unsafe.Pointer)
var aeronCncToDriverHeartbeat func(cnc unsafe.Pointer) int64
var aeronCncFilename func(cnc unsafe.Pointer) string

func CncInit(cnc *unsafe.Pointer, basePath string, timeoutMs int64) error {
	return CheckResult(aeronCncInit(cnc, basePath, timeoutMs))
}

func CncClose(cnc unsafe.Pointer) {
	aeronCncClose(cnc)
}

func CncToDriverHeartbeat(cnc unsafe.Pointer) int64 {
	return aeronCncToDriverHeartbeat(cnc)
}

func CncFilename(cnc unsafe.Pointer) string {
	return aeronCncFilename(cnc)
}

// -- Counter ----------------------------------------------------------------

var aeronAsyncAddCounter func(async *unsafe.Pointer, client unsafe.Pointer, typeId int32, keyBuffer unsafe.Pointer, keyLength uintptr, labelBuffer unsafe.Pointer, labelLength uintptr) int32
var aeronAsyncAddCounterPoll func(counter *unsafe.Pointer, async unsafe.Pointer) int32
var aeronCounterClose func(counter unsafe.Pointer, onComplete uintptr, clientd unsafe.Pointer) int32

// -- Aeron publication constants (return values from offer/try_claim) --------

const (
	NotConnected    int64 = -1
	BackPressured   int64 = -2
	AdminAction     int64 = -3
	PublicationClosed int64 = -4
	MaxPositionExceeded int64 = -5
)

// -- Channel status constants -----------------------------------------------

const (
	ChannelStatusErrored  int64 = -1
	ChannelStatusActive   int64 = 1
	ChannelStatusClosing  int64 = 2
)

// ---------------------------------------------------------------------------
// registerSymbols binds all C function pointers via purego.RegisterLibFunc.
// Called once from Open().
// ---------------------------------------------------------------------------

func registerSymbols(handle uintptr) {
	// Utility
	purego.RegisterLibFunc(&aeronVersionFull, handle, "aeron_version_full")
	purego.RegisterLibFunc(&aeronErrmsg, handle, "aeron_errmsg")
	purego.RegisterLibFunc(&aeronErrcode, handle, "aeron_errcode")

	// Context
	purego.RegisterLibFunc(&aeronContextInit, handle, "aeron_context_init")
	purego.RegisterLibFunc(&aeronContextSetDir, handle, "aeron_context_set_dir")
	purego.RegisterLibFunc(&aeronContextSetDriverTimeoutMs, handle, "aeron_context_set_driver_timeout_ms")
	purego.RegisterLibFunc(&aeronContextSetErrorHandlerFn, handle, "aeron_context_set_error_handler")
	purego.RegisterLibFunc(&aeronContextSetOnNewPublicationFn, handle, "aeron_context_set_on_new_publication")
	purego.RegisterLibFunc(&aeronContextSetOnNewSubscriptionFn, handle, "aeron_context_set_on_new_subscription")
	purego.RegisterLibFunc(&aeronContextSetOnAvailableCounterFn, handle, "aeron_context_set_on_available_counter")
	purego.RegisterLibFunc(&aeronContextSetOnUnavailableCounterFn, handle, "aeron_context_set_on_unavailable_counter")
	purego.RegisterLibFunc(&aeronContextClose, handle, "aeron_context_close")

	// Client lifecycle
	purego.RegisterLibFunc(&aeronInit, handle, "aeron_init")
	purego.RegisterLibFunc(&aeronStart, handle, "aeron_start")
	purego.RegisterLibFunc(&aeronClose, handle, "aeron_close")
	purego.RegisterLibFunc(&aeronMainDoWork, handle, "aeron_main_do_work")
	purego.RegisterLibFunc(&aeronMainIdleStrategy, handle, "aeron_main_idle_strategy")
	purego.RegisterLibFunc(&aeronNextCorrelationId, handle, "aeron_next_correlation_id")
	purego.RegisterLibFunc(&aeronClientId, handle, "aeron_client_id")

	// Publication (async)
	purego.RegisterLibFunc(&aeronAsyncAddPublication, handle, "aeron_async_add_publication")
	purego.RegisterLibFunc(&aeronAsyncAddPublicationPoll, handle, "aeron_async_add_publication_poll")
	purego.RegisterLibFunc(&aeronPublicationOffer, handle, "aeron_publication_offer")
	purego.RegisterLibFunc(&aeronPublicationTryClaim, handle, "aeron_publication_try_claim")
	purego.RegisterLibFunc(&aeronPublicationClose, handle, "aeron_publication_close")
	purego.RegisterLibFunc(&aeronPublicationIsConnected, handle, "aeron_publication_is_connected")
	purego.RegisterLibFunc(&aeronPublicationIsClosed, handle, "aeron_publication_is_closed")
	purego.RegisterLibFunc(&aeronPublicationChannelStatus, handle, "aeron_publication_channel_status")
	purego.RegisterLibFunc(&aeronPublicationStreamId, handle, "aeron_publication_stream_id")
	purego.RegisterLibFunc(&aeronPublicationSessionId, handle, "aeron_publication_session_id")

	// Exclusive publication
	purego.RegisterLibFunc(&aeronAsyncAddExclusivePublication, handle, "aeron_async_add_exclusive_publication")
	purego.RegisterLibFunc(&aeronAsyncAddExclusivePublicationPoll, handle, "aeron_async_add_exclusive_publication_poll")
	purego.RegisterLibFunc(&aeronExclusivePublicationOffer, handle, "aeron_exclusive_publication_offer")
	purego.RegisterLibFunc(&aeronExclusivePublicationTryClaim, handle, "aeron_exclusive_publication_try_claim")
	purego.RegisterLibFunc(&aeronExclusivePublicationClose, handle, "aeron_exclusive_publication_close")
	purego.RegisterLibFunc(&aeronExclusivePublicationIsConnected, handle, "aeron_exclusive_publication_is_connected")
	purego.RegisterLibFunc(&aeronExclusivePublicationIsClosed, handle, "aeron_exclusive_publication_is_closed")

	// Subscription (async)
	purego.RegisterLibFunc(&aeronAsyncAddSubscription, handle, "aeron_async_add_subscription")
	purego.RegisterLibFunc(&aeronAsyncAddSubscriptionPoll, handle, "aeron_async_add_subscription_poll")
	purego.RegisterLibFunc(&aeronSubscriptionPoll, handle, "aeron_subscription_poll")
	purego.RegisterLibFunc(&aeronSubscriptionClose, handle, "aeron_subscription_close")
	purego.RegisterLibFunc(&aeronSubscriptionIsConnected, handle, "aeron_subscription_is_connected")
	purego.RegisterLibFunc(&aeronSubscriptionIsClosed, handle, "aeron_subscription_is_closed")
	purego.RegisterLibFunc(&aeronSubscriptionChannelStatus, handle, "aeron_subscription_channel_status")


	// Fragment assembler
	purego.RegisterLibFunc(&aeronFragmentAssemblerCreate, handle, "aeron_fragment_assembler_create")
	purego.RegisterLibFunc(&aeronFragmentAssemblerDelete, handle, "aeron_fragment_assembler_delete")

	// Resolve the fragment assembler handler function pointer via Dlsym.
	// This is a C function pointer passed to aeron_subscription_poll.
	sym, err := purego.Dlsym(handle, "aeron_fragment_assembler_handler")
	if err == nil {
		FragmentAssemblerHandlerPtr = sym
	}

	// Buffer claim
	purego.RegisterLibFunc(&aeronBufferClaimCommit, handle, "aeron_buffer_claim_commit")
	purego.RegisterLibFunc(&aeronBufferClaimAbort, handle, "aeron_buffer_claim_abort")

	// CnC (media driver heartbeat)
	purego.RegisterLibFunc(&aeronCncInit, handle, "aeron_cnc_init")
	purego.RegisterLibFunc(&aeronCncClose, handle, "aeron_cnc_close")
	purego.RegisterLibFunc(&aeronCncToDriverHeartbeat, handle, "aeron_cnc_to_driver_heartbeat")
	purego.RegisterLibFunc(&aeronCncFilename, handle, "aeron_cnc_filename")

	// Counter
	purego.RegisterLibFunc(&aeronAsyncAddCounter, handle, "aeron_async_add_counter")
	purego.RegisterLibFunc(&aeronAsyncAddCounterPoll, handle, "aeron_async_add_counter_poll")
	purego.RegisterLibFunc(&aeronCounterClose, handle, "aeron_counter_close")
}

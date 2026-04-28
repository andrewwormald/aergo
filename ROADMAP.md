# Aergo Roadmap -- Known Tradeoffs & Tech Debt

## Legend
- [x] = Done
- [P] = Prototype OK, must fix for production
- [B] = Blocker for production latency
- [R] = Research / investigation needed

## Allocations (hot path)

- [x] `Publication.Offer(buf []byte)` copies Go slice to C -- `TryClaim` zero-copy path implemented (Publication.TryClaim + BufferClaim)
- [P] `Subscription.Poll` fragment handler receives `[]byte` created via `unsafe.Slice` from C pointer -- creates a slice header per fragment. Use `Bind()`/`PollBound()` to avoid per-poll handler registration allocs
- [P] SBE `SessionMessageHeader.Encode()` returns `int` byte count on pre-allocated buffer -- OK, but caller must manage buffer lifecycle. BufferPool implemented
- [x] String allocations in `SessionConnectRequest.ResponseChannel` and error paths -- one-time at connect, not hot path
- [P] `EgressListener.OnMessage` passes `[]byte` slice -- production should use flyweight view over underlying buffer
- [x] Map lookup in callback dispatch (`clientd -> Go handler`) -- replaced with fixed-index array (callbackSlot[256])

## Hot Path Blockers

- [P] `purego.SyscallN` overhead per C call (~100-200ns) -- unavoidable with purego. Measured ~9.6us offer+poll RTT on UDP loopback (M4 Pro). If sub-microsecond matters, evaluate CGO or pure Go
- [x] Go GC pause during poll loop -- `TuningProfile.DisableGC` implemented, `Poller` has `DisableGC` option
- [x] `runtime.LockOSThread()` in poll loop -- implemented in `Poller.Start()` and `AeronCluster.Poll()` (configurable via `Config.LockOSThread`)
- [P] No CPU affinity / NUMA awareness -- `PrintAffinityHint()` added with Linux taskset instructions. Must be set externally
- [x] Fragment assembler -- Go-native `FragmentAssembler` implemented, eliminates C-side allocations for reassembly

## purego Constraints

- [x] Callback lifetime limit (~2000, never freed) -- fixed-index array with 256 slots, reusable. `Bind()`/`Unbind()` for long-lived handlers
- [P] No automatic C struct alignment -- `aeron_buffer_claim_t` (24 bytes: ptr, ptr, size_t) manually verified against C source. Must re-verify on each Aeron version upgrade
- [R] purego is beta (v0.10.0) -- API may break between releases. Pinned in go.mod

## Protocol & Compatibility

- [x] Aeron open-source C client v1.46.7 wire-compatible with Aeron 1.50.4 server. Cluster handshake verified
- [P] Cluster protocol version negotiation -- hardcodes SchemaVersion=8. Must handle version mismatch
- [P] Egress channel hardcodes `endpoint=localhost:19876` -- should allocate an ephemeral port dynamically or use IPC egress
- [P] SBE decode uses wire `blockLength` for forward compat -- handles newer schemas with additional fields
- [P] No authentication/challenge-response -- Challenge (template 7) decoded but not responded to. Implement if cluster requires it

## Reliability & Error Handling

- [x] Reconnection logic -- implemented with exponential backoff (1s initial, 30s max), configurable `MaxReconnectAttempts`
- [x] Graceful shutdown -- `GracefulClose()` sends `SessionCloseRequest`, polls for ack, then closes
- [x] Media driver heartbeat detection -- `HeartbeatMonitor` via CnC file (`aeron_cnc_to_driver_heartbeat`)
- [x] Backpressure handling on `Offer` -- `OfferWithBackpressure()` with `BackpressureSpin`, `BackpressureYield`, `BackpressureNone` strategies
- [x] Buffer pooling -- `BufferPool` (fixed ring of pre-allocated buffers, no sync.Pool)

## Agrona Rewrite (long-term)

- [R] Ring buffer (ManyToOneRingBuffer, OneToOneRingBuffer) -- needed to replace C client's shared memory access
- [R] Broadcast receiver -- for CnC file communication with media driver
- [R] Atomic buffer operations -- Go's sync/atomic covers most, but Aeron's ordered/lazy semantics need careful mapping
- [R] Error log reader -- for reading media driver error log from shared memory
- [R] Once Agrona is in Go, can eliminate libaeron.so dependency entirely -- pure Go Aeron client

## Benchmarks (M4 Pro, macOS arm64, Aeron 1.46.7, UDP loopback)

| Path | RTT (ns/op) | Allocs/op |
|------|-------------|-----------|
| Offer + Poll | ~10,100 | ~171 |
| Offer + PollBound | ~10,100 | ~172 |
| TryClaim + Poll | ~9,700 | ~164 |

Remaining allocs are from purego internals and unsafe.Slice slice headers.
IPC channels expected to be 3-5x faster than UDP loopback.

# Aergo

Go Aeron cluster client using purego (no CGO) for low-latency communication with Aeron clusters.

## Build

```bash
# Build Aeron C library (requires cmake)
./scripts/build-aeron.sh

# Build Go
go build ./...
```

## Run

```bash
# Start media driver
./build/aeron/bin/aeronmd

# Pub/sub smoke test
go run ./cmd/aergo -lib ./build/aeron/lib/libaeron.dylib -mode pubsub

# TryClaim zero-copy test
go run ./cmd/aergo -lib ./build/aeron/lib/libaeron.dylib -mode tryclaim

# Cluster connect
go run ./cmd/aergo -lib ./build/aeron/lib/libaeron.dylib -mode cluster -endpoint localhost:10003 -dir <aeron-media-driver-dir>
```

## Test

```bash
# Unit tests (no external deps)
go test ./...

# Integration tests (requires running aeronmd)
go test -tags integration -v ./pkg/aeron/client/ -aeron-lib=$(pwd)/build/aeron/lib/libaeron.dylib

# Benchmarks
go test -tags integration -bench=. -benchmem ./pkg/aeron/client/ -aeron-lib=$(pwd)/build/aeron/lib/libaeron.dylib
```

## Architecture

```
purego.Dlopen(libaeron.dylib)
    |
pkg/aeron/driver    -- raw C function bindings (RegisterLibFunc)
    |
pkg/aeron/client    -- Go-idiomatic Client, Publication, Subscription
    |                   BufferClaim (TryClaim), FragmentAssembler,
    |                   Poller (LockOSThread), HeartbeatMonitor
    |
pkg/codec/sbe       -- zero-alloc SBE encoding primitives
pkg/codec/cluster   -- Aeron cluster protocol messages (templates 1-8)
    |
pkg/cluster         -- Cluster interface + AeronCluster state machine (10 states)
                       auto-reconnect, graceful shutdown, challenge-response
```

## Key conventions

- `pkg/cluster.Cluster` interface decouples consumers from the concrete `AeronCluster` type -- implement this to support different cluster backends or custom framing
- All C types are opaque `unsafe.Pointer` -- never dereference struct internals except `aeron_buffer_claim_t` (24 bytes) and `aeron_header_t` (40 bytes) which are manually laid out
- Struct alignments verified via `scripts/check-structs.c` against Aeron 1.46.7 on arm64
- Callback budget: ~256 slots in fixed-index array. Use `Bind()`/`PollBound()` for long-lived handlers
- `ROADMAP.md` tracks all tradeoffs and tech debt
- Integration tests require `-tags integration` and a running `aeronmd`

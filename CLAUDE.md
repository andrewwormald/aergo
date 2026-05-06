# Aergo

Pure Go Aeron cluster client using shared memory (no C library, no CGO).

## Build

```bash
# Build Aeron C media driver (requires cmake)
./scripts/build-aeron.sh

# Build Go
go build ./...
```

## Run

```bash
# Start media driver
./build/aeron/bin/aeronmd

# Cluster connect
go run ./cmd/aergo -dir /tmp/aeron-<user> -endpoint localhost:10003
```

## Test

```bash
go test ./...
```

## Architecture

```
syscall.Mmap(cnc.dat)
    |
pkg/aeron/native    -- pure Go shared memory protocol
    |                   AtomicBuffer, ManyToOneRingBuffer,
    |                   BroadcastReceiver, Conductor,
    |                   Publication, Subscription
    |
pkg/codec/sbe       -- zero-alloc SBE encoding primitives
pkg/codec/cluster   -- Aeron cluster protocol messages (templates 1-8)
    |
pkg/cluster         -- Cluster interface + AeronCluster state machine
                       auto-reconnect, graceful shutdown, challenge-response
```

## Key conventions

- `pkg/cluster.Cluster` interface decouples consumers from the concrete `AeronCluster` type
- Aeron-idiomatic naming: `Aeron`, `Connect`, `Context`, `Publication`, `Subscription`, `Image`, `FragmentHandler`, `Conductor`
- Zero external dependencies -- pure Go standard library only
- All shared memory access via `sync/atomic` and `unsafe.Pointer` on mmap'd files
- `ROADMAP.md` tracks tradeoffs and tech debt

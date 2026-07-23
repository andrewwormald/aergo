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
go run ./cmd/aergo -dir /tmp/aeron-<user> -endpoint localhost:10000
```

## Test

```bash
go test ./...
```

## Architecture

```
syscall.Mmap(cnc.dat)
    |
pkg                 -- pure Go shared memory protocol (package aeron)
    |                   AtomicBuffer, ManyToOneRingBuffer,
    |                   BroadcastReceiver, Conductor,
    |                   Publication, Subscription
    |
pkg/cluster         -- Cluster interface + AeronCluster state machine
                       SBE codecs, auto-reconnect, graceful shutdown
```

## Key conventions

- `pkg/cluster.Cluster` interface decouples consumers from the concrete `AeronCluster` type
- Aeron-idiomatic naming: `Aeron`, `Connect`, `Context`, `Publication`, `Subscription`, `Image`, `FragmentHandler`, `Conductor`
- Zero external dependencies -- pure Go standard library only
- All shared memory access via `sync/atomic` and `unsafe.Pointer` on mmap'd files

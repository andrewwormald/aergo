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
package aergo (repo root, single package)
    |-- AtomicBuffer, ManyToOneRingBuffer,   -- pure Go shared memory protocol
    |   BroadcastReceiver, Conductor,
    |   Publication, Subscription
    |
    `-- Cluster, AeronCluster, ClusterConfig -- Aeron cluster protocol built on
        SBE codecs, EgressListener              the above (session connect,
                                                 leader tracking, reconnection)
```

## Key conventions

- Single package (`aergo`) at repo root -- no `pkg/` or `cluster/` subpackages. Cluster-specific exports (`ClusterConfig`, `ClusterState`, `NewCluster`, ...) are prefixed to stay unambiguous once merged into the flat namespace; types that are already unambiguous in context (`SessionEvent`, `Challenge`, `EgressListener`) are not.
- `Cluster` interface decouples consumers from the concrete `AeronCluster` type
- Aeron-idiomatic naming: `Aeron`, `Connect`, `Context`, `Publication`, `Subscription`, `Image`, `FragmentHandler`, `Conductor`
- Zero external dependencies -- pure Go standard library only
- All shared memory access via `sync/atomic` and `unsafe.Pointer` on mmap'd files
- No internal logging: the library never writes to a process-global logger. Synchronous failures are returned as errors; the poll-driven cluster state machine (which has no synchronous caller to return to) reports failures via `EgressListener.OnError` instead. Callers decide whether/where to log.

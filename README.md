# Aergo

Pure Go [Aeron](https://github.com/aeron-io/aeron) cluster client. Communicates with the Aeron media driver via shared memory -- no C library, no CGO, zero external dependencies.

## Features

- **Pure Go** -- talks directly to the media driver via mmap'd shared memory (`syscall.Mmap` + `sync/atomic`)
- **Zero dependencies** -- only Go standard library
- **Cluster client** -- full Aeron cluster protocol: session connect, leader tracking, reconnection, graceful shutdown
- **Aeron-idiomatic API** -- `Aeron`, `Publication`, `Subscription`, `Image`, `FragmentHandler`, `Conductor`

## Prerequisites

- Go 1.22+
- A running Aeron media driver (`aeronmd`)

## Quick Start

```go
import (
    aeron "github.com/andrewwormald/aergo/pkg/aeron/native"
    "github.com/andrewwormald/aergo/pkg/cluster"
)

// Connect to the media driver
client, err := aeron.Connect(aeron.WithDir("/dev/shm/aeron-user"))

// Create a publication
pub, err := client.AddPublication("aeron:udp?endpoint=localhost:40123", 1001)
pub.Offer([]byte("hello"))

// Create a subscription
sub, err := client.AddSubscription("aeron:udp?endpoint=localhost:40123", 1001)
sub.Poll(func(buffer []byte, header *aeron.Header) {
    fmt.Println("received:", string(buffer))
}, 10)
```

## Building the Media Driver

The media driver (`aeronmd`) must be running before connecting. Build it from the Aeron C source:

```bash
./scripts/build-aeron.sh
./build/aeron/bin/aeronmd
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

### How it works

The Aeron media driver manages shared memory regions for inter-process communication:

```
CnC File (cnc.dat)
├── To-Driver Buffer    → ManyToOneRingBuffer (send commands)
├── To-Clients Buffer   → BroadcastReceiver (receive responses)
├── Counter Values      → AtomicBuffer (heartbeat, positions)
└── Counter Metadata    → counter definitions

Log Buffer Files (per publication/subscription)
├── Term 0, 1, 2       → TermAppender (write) / TermReader (read)
└── Metadata            → tail positions, connection status
```

All operations use lock-free atomic operations (`sync/atomic`) on memory-mapped files. No locks on the hot path.

## Tests

```bash
go test ./...
```

## License

MIT

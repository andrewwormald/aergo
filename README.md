<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="assets/logo-light.svg">
    <img alt="aergo" src="assets/logo-light.svg" width="320">
  </picture>
</p>

<p align="center"><em>Zero-dependency, pure Go <a href="https://github.com/aeron-io/aeron">Aeron</a> cluster client. No CGO. No C library.</em></p>

Built for low-latency Go services that need Aeron's shared-memory transport without inheriting a C toolchain. Talks to the media driver directly over mmap'd shared memory using only the Go standard library -- no third-party Go modules, no `import "C"`, no linker flags.

## Features

- **Pure Go** -- talks directly to the media driver via mmap'd shared memory (`syscall.Mmap` + `sync/atomic`)
- **Zero dependencies** -- only Go standard library
- **Cluster client** -- full Aeron cluster protocol: session connect, leader tracking, reconnection, graceful shutdown
- **Aeron-idiomatic API** -- `Aeron`, `Publication`, `Subscription`, `Image`, `FragmentHandler`, `Conductor`

## Prerequisites

- Go 1.26+
- A running Aeron media driver (`aeronmd`)

## Quick Start

A single import gets you both the raw pub/sub client and the cluster client:

```go
import "github.com/andrewwormald/aergo"

// Connect to the media driver
client, err := aergo.Connect(aergo.WithDir("/dev/shm/aeron-user"))

// Create a publication
pub, err := client.AddPublication("aeron:udp?endpoint=localhost:40123", 1001)
pos, err := pub.Offer([]byte("hello")) // pos < 0: NotConnected, BackPressured, ... (retry/react)

// Create a subscription
sub, err := client.AddSubscription("aeron:udp?endpoint=localhost:40123", 1001)
sub.Poll(func(buffer []byte, header *aergo.Header) {
    fmt.Println("received:", string(buffer))
}, 10)
```

Most consumers want the cluster client rather than raw pub/sub:

```go
cfg := aergo.DefaultClusterConfig()
cfg.AeronDir = "/dev/shm/aeron-user"
cfg.Members = []aergo.ClusterMember{{MemberId: 0, Endpoint: "localhost:10000"}}

cc, err := aergo.NewCluster(cfg)
cc.Connect()
for cc.State() != aergo.StateConnected {
    cc.Poll()
}
pos, err := cc.Offer([]byte("hello cluster"))
```

### Shared and exclusive publications

`AddPublication` gives a publication any number of goroutines may offer to at once. `AddExclusivePublication` gives one that only a single goroutine may offer to at a time (as Java's `ExclusivePublication`): it is a little faster, because it reserves space in the log without an atomic add, but two goroutines offering together would corrupt the log. The cluster client uses exclusive publications for ingress, so drive an `AeronCluster` from one goroutine.

### Message size

`Offer` follows the Java client. A message longer than one frame (`pub.MaxPayloadLength()`, the MTU minus the 32-byte header) is split into fragments, so wrap your handler in a `FragmentAssembler` to receive it whole. One longer than `pub.MaxMessageLength()` (an eighth of the term length, at most 16 MiB) can never be sent: `Offer` returns a `*MessageTooLongError` (`errors.Is(err, aergo.ErrMessageTooLong)`) and writes nothing.

The returned position is separate from the error. Back pressure and similar conditions are negative positions with a nil error, as in Java, so a retry loop stays cheap; the error is only for a message that cannot be sent however often you retry.

### Errors with no caller to return to

The library never logs. Errors found while the client works in the background (a driver rejection, a log that cannot be mapped, a corrupt frame, a driver timeout) go to a handler you supply, as with Java's `Aeron.Context.errorHandler`. For the cluster client they go to `EgressListener.OnError`.

```go
client, err := aergo.Connect(
    aergo.WithDir("/dev/shm/aeron-user"),
    aergo.WithErrorHandler(func(err error) { log.Printf("aeron: %v", err) }),
)
```

## Supported platforms

Linux on **amd64** and **arm64**, both tested natively in CI. The client reads and writes the driver's memory-mapped files directly, so it relies on the log's little-endian layout; big-endian targets fail to build. On amd64 the release store that commits a frame is a two-instruction assembly routine; on arm64 it is Go's own atomic store, which compiles to the same single `STLR` instruction Java uses. `-tags aergo_noasm` forces the pure-Go store on amd64.

## Building the Media Driver

`aergo` is a client -- it talks to a running `aeronmd` process via shared memory. The media driver itself is the upstream Aeron C/C++ build; this repo does not bundle a build script. Follow the upstream build instructions:

- <https://github.com/aeron-io/aeron> (`cppbuild/cppbuild` or `cmake` directly)

Once built, launch the driver and point `aergo` at the same directory:

```bash
aeronmd                                       # defaults to /dev/shm/aeron-<user>
go run ./cmd/aergo -dir /dev/shm/aeron-<user> # use the same path
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

### How it works

The Aeron media driver manages shared memory regions for inter-process communication:

```
CnC File (cnc.dat)
├── To-Driver Buffer    → ManyToOneRingBuffer (send commands)
├── To-Clients Buffer   → BroadcastReceiver (receive responses)
├── Counter Values      → AtomicBuffer (heartbeat, positions)
├── Counter Metadata    → counter definitions
└── Error Log Buffer    → AtomicBuffer (driver error reports)

Log Buffer Files (per publication/subscription)
├── Term 0, 1, 2       → TermAppender (write) / TermReader (read)
└── Metadata            → tail positions, connection status
```

Data-plane reads and writes use `sync/atomic` directly on the mmap'd shared memory (the one exception is the release store on amd64, a plain `MOV` in `atomic_store_amd64.s`). `Publication.Offer` and `Subscription.Poll` are both lock-free: the conductor publishes an immutable snapshot of a subscription's images, which `Poll` reads without taking a lock, and hands the handler a view straight into the term buffer.

## Tests

```bash
go test ./...
```

## License

MIT

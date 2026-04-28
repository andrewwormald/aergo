# Aergo

Go [Aeron](https://github.com/aeron-io/aeron) cluster client using [purego](https://github.com/ebitengine/purego) (no CGO) for low-latency communication with Aeron clusters.

Aergo loads the Aeron C client library at runtime via `purego.Dlopen`, giving you native Aeron performance from pure Go without CGO complications in your build pipeline.

## Features

- **No CGO** -- links to `libaeron.so`/`libaeron.dylib` at runtime via purego
- **Cluster client** -- full Aeron cluster protocol: session connect, leader tracking, reconnection, graceful shutdown
- **Zero-copy publish** -- `TryClaim` / `BufferClaim` path writes directly into the Aeron log buffer
- **Fragment assembler** -- reassembles multi-fragment messages in pure Go
- **Backpressure** -- configurable strategies (spin, yield, none) on `Offer`
- **Heartbeat monitor** -- detects media driver liveness via CnC file
- **`Cluster` interface** -- decouple consumers from `AeronCluster` for custom framing, testing, or alternative backends

## Prerequisites

- Go 1.22+
- CMake 3.10+ (to build the Aeron C library)
- A C compiler (gcc, clang, or MSVC)

## Building the Aeron C library

Aergo requires the Aeron C client library (`libaeron`) and optionally the standalone media driver (`aeronmd`). The included script builds both from source.

```bash
./scripts/build-aeron.sh
```

This clones [aeron-io/aeron](https://github.com/aeron-io/aeron) v1.46.7, builds the C client and media driver, and places outputs in `./build/aeron/`:

```
build/aeron/
  lib/
    libaeron.dylib     # macOS
    libaeron.so        # Linux
    libaeron_static.a
  bin/
    aeronmd            # standalone media driver
```

To build a specific version:

```bash
AERON_VERSION=1.46.8 ./scripts/build-aeron.sh
```

## Running locally

### 1. Start the media driver

The Aeron media driver manages shared memory for IPC and network transport. It must be running before any Aeron client can connect.

```bash
# Standalone media driver (default dir: /dev/shm/aeron-<user> on Linux, /tmp/aeron-<user> on macOS)
./build/aeron/bin/aeronmd
```

Or with a custom directory:

```bash
AERON_DIR=/tmp/my-aeron ./build/aeron/bin/aeronmd -Daeron.dir=/tmp/my-aeron
```

### 2. Pub/sub smoke test

Tests basic publication and subscription over UDP loopback. Uses the standalone media driver started above.

```bash
go run ./cmd/aergo -lib ./build/aeron/lib/libaeron.dylib -mode pubsub
```

### 3. Zero-copy test

Tests the `TryClaim` / `BufferClaim` path that writes directly into the log buffer (no copy).

```bash
go run ./cmd/aergo -lib ./build/aeron/lib/libaeron.dylib -mode tryclaim
```

### 4. Cluster connect

Connects to an Aeron cluster. Requires a running cluster node that shares the same media driver.

```bash
go run ./cmd/aergo \
  -lib ./build/aeron/lib/libaeron.dylib \
  -mode cluster \
  -endpoint localhost:10003 \
  -dir /path/to/cluster/aeron-driver
```

The `-dir` flag **must** point to the same Aeron media driver directory used by the cluster node. A separate standalone `aeronmd` will not work -- the client and cluster must share the same shared memory region.

## Aeron media driver in Kubernetes

In Kubernetes, the Aeron media driver runs as a sidecar or init container alongside your application pod. The key requirement is **shared memory** between the media driver and your application.

### Option 1: Sidecar container (recommended)

Build a container image with `aeronmd`:

```dockerfile
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y cmake gcc g++ git make

ARG AERON_VERSION=1.46.7
RUN git clone --depth 1 --branch ${AERON_VERSION} \
    https://github.com/aeron-io/aeron.git /tmp/aeron-src && \
    mkdir /tmp/aeron-build && cd /tmp/aeron-build && \
    cmake /tmp/aeron-src \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_AERON_DRIVER=ON \
        -DBUILD_AERON_ARCHIVE_API=OFF \
        -DAERON_TESTS=OFF \
        -DAERON_BUILD_SAMPLES=OFF \
        -DAERON_BUILD_DOCUMENTATION=OFF && \
    cmake --build . --parallel $(nproc) --target aeronmd aeron_driver

FROM debian:bookworm-slim
COPY --from=builder /tmp/aeron-build/binaries/aeronmd /usr/local/bin/aeronmd
COPY --from=builder /tmp/aeron-build/lib/libaeron_driver.so /usr/local/lib/
RUN ldconfig
ENTRYPOINT ["aeronmd"]
```

Pod spec with shared memory:

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
    - name: aeronmd
      image: your-registry/aeronmd:1.46.7
      args: ["-Daeron.dir=/dev/shm/aeron"]
      volumeMounts:
        - name: aeron-shm
          mountPath: /dev/shm
      resources:
        requests:
          memory: "256Mi"
        limits:
          memory: "512Mi"

    - name: app
      image: your-registry/your-app:latest
      env:
        - name: AERON_DIR
          value: /dev/shm/aeron
        - name: AERON_LIB
          value: /usr/local/lib/libaeron.so
      volumeMounts:
        - name: aeron-shm
          mountPath: /dev/shm
        - name: aeron-lib
          mountPath: /usr/local/lib

  volumes:
    - name: aeron-shm
      emptyDir:
        medium: Memory          # tmpfs -- backed by RAM, not disk
        sizeLimit: 256Mi
    - name: aeron-lib
      emptyDir: {}

  # Init container to copy libaeron.so for the app container
  initContainers:
    - name: copy-aeron-lib
      image: your-registry/aeronmd:1.46.7
      command: ["cp", "/usr/local/lib/libaeron_driver.so", "/aeron-lib/"]
      volumeMounts:
        - name: aeron-lib
          mountPath: /aeron-lib
```

### Option 2: Embedded media driver

If your cluster framework (e.g. Aeron Archive, Aeron Cluster) starts an embedded media driver, your Go application just needs `libaeron.so` and the path to the driver's shared memory directory.

```yaml
# In your app container
env:
  - name: AERON_DIR
    value: /dev/shm/aeron-cluster  # must match the embedded driver's directory
```

### Key considerations for Kubernetes

| Concern | Solution |
|---------|----------|
| Shared memory | Use `emptyDir` with `medium: Memory` (tmpfs). Both containers mount the same volume at `/dev/shm`. |
| Memory sizing | Aeron uses memory-mapped files. Default term buffer is 64KB per stream. Size the `emptyDir` for your stream count. |
| CPU pinning | For lowest latency, use `resources.limits.cpu` to get guaranteed QoS, and set `isolcpus` on the node. The media driver's conductor and sender/receiver threads benefit from dedicated cores. |
| Liveness | `aeronmd` writes heartbeats to the CnC file. Aergo's `HeartbeatMonitor` detects driver death and can trigger reconnection. Add a liveness probe that checks the CnC file. |
| Graceful shutdown | Set `terminationGracePeriodSeconds` high enough for `GracefulClose()` to complete. The app should close its cluster session before the media driver shuts down. |
| Network | For UDP transport, the pod needs network access to cluster endpoints. For IPC-only (same pod), no network config needed. |

### Multi-arch build

The Aeron C library must match the container's architecture. For multi-arch images:

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t your-registry/aeronmd:1.46.7 .
```

## Architecture

```
purego.Dlopen(libaeron)
    |
pkg/aeron/driver     raw C function bindings via purego
    |
pkg/aeron/client     Go-idiomatic Client, Publication, Subscription
    |                 BufferClaim (TryClaim), FragmentAssembler,
    |                 Poller (LockOSThread), HeartbeatMonitor
    |
pkg/codec/sbe        zero-alloc SBE encoding primitives
pkg/codec/cluster    Aeron cluster protocol messages (templates 1-8)
    |
pkg/cluster          Cluster interface + AeronCluster state machine
                     auto-reconnect, graceful shutdown, challenge-response
```

### The `Cluster` interface

Consumers depend on the `Cluster` interface rather than the concrete `AeronCluster` type:

```go
type Cluster interface {
    Connect()
    Poll() int
    Offer(buf []byte) int64
    State() State
    GracefulClose()
    Close() error
    // ...
}
```

This decouples your application logic from the Aeron transport. Use it to:

- Add custom message framing (service headers, protobuf wrapping) in an adapter
- Mock the cluster in tests
- Swap transport backends

### Message flow

```
Your App                         Aeron Cluster
   |                                  |
   |  Offer(payload)                  |
   |  +--[SessionMessageHeader]------>|  (ingress, stream 101)
   |                                  |
   |           OnMessage(buf,offset)  |
   |<--[SessionMessageHeader]------+  |  (egress, stream 102)
   |                                  |
```

The `SessionMessageHeader` (24 bytes: LeadershipTermId, ClusterSessionId, Timestamp) is automatically prepended on send and stripped on receive. Your `EgressListener.OnMessage` callback receives the raw application payload.

## Tests

```bash
# Unit tests (no external dependencies)
go test ./...

# Integration tests (requires a running aeronmd)
go test -tags integration -v ./pkg/aeron/client/ \
  -aeron-lib=$(pwd)/build/aeron/lib/libaeron.dylib

# Benchmarks
go test -tags integration -bench=. -benchmem ./pkg/aeron/client/ \
  -aeron-lib=$(pwd)/build/aeron/lib/libaeron.dylib
```

## Benchmarks

Measured on M4 Pro, macOS arm64, Aeron 1.46.7, UDP loopback:

| Path | RTT (ns/op) | Allocs/op |
|------|-------------|-----------|
| Offer + Poll | ~10,100 | ~171 |
| TryClaim + Poll | ~9,700 | ~164 |

IPC channels are expected to be 3-5x faster than UDP loopback.

## License

MIT

# Hotpath benchmarks

Aergo ships benchmarks for every step on the per-message send/receive hotpath,
plus one composite end-to-end benchmark. They run entirely in-process against
heap-allocated buffers — no `aeronmd` driver is required.

## Run

```sh
make bench-quick        # ~60s smoke run
make bench              # full baseline (10 counts × 2s) → bench/baseline.txt
```

To target one benchmark:

```sh
go test -bench=BenchmarkPublicationOffer -benchmem ./pkg/aeron/
```

## Coverage

Send path:

- `BenchmarkMessageHeaderEncode`, `BenchmarkSessionMessageHeaderEncode` — SBE encode (pkg/cluster)
- `BenchmarkAtomicBuffer*` — plain, ordered, CAS, fetch-add primitives
- `BenchmarkRingBufferWrite` — to-driver MPSC ring (sub-bench by payload size)
- `BenchmarkTermAppenderAppend`, `BenchmarkPublicationOffer` — log buffer write (sub-bench by payload size)

Receive path:

- `BenchmarkBroadcastReceiveNext`, `BenchmarkCopyBroadcastReceive` — to-clients broadcast buffer
- `BenchmarkReadTerm`, `BenchmarkSubscriptionPoll` — log buffer read + fragment dispatch
- `BenchmarkMessageHeaderDecode`, `BenchmarkSessionMessageHeaderDecode` — SBE decode

Composite:

- `BenchmarkEndToEndSendReceive` — encode → Offer → Poll → decode in one process (sub-bench by payload size)

## Comparing runs

Install `benchstat` once:

```sh
go install golang.org/x/perf/cmd/benchstat@latest
```

Then compare a working copy against the committed baseline:

```sh
make bench > bench/new.txt          # or redirect manually; this also overwrites baseline.txt
benchstat bench/baseline.txt bench/new.txt
```

Convention: any PR that touches code on the hotpath should include a
`benchstat` diff in the description.

## Known cost: per-fragment allocation in Poll

`BenchmarkSubscriptionPoll` and `BenchmarkEndToEndSendReceive` will both
report non-zero `allocs/op`. That comes from
`pkg/aeron/subscription.go`'s fragment handler doing
`payload := make([]byte, length)` per fragment. It is real and intentional to
surface — the baseline exists so this allocation (and any others) can be
measured before and after future optimisations.

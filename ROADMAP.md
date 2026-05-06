# Aergo Roadmap -- Known Tradeoffs & Tech Debt

## Legend
- [x] = Done
- [P] = Prototype OK, must fix for production
- [R] = Research / investigation needed

## Pure Go Implementation

- [x] AtomicBuffer with volatile/CAS/GetAndAdd operations via sync/atomic
- [x] ManyToOneRingBuffer (MPSC) for sending commands to media driver
- [x] BroadcastReceiver for reading responses from media driver
- [x] CnC file mmap and buffer slicing
- [x] DriverProxy for all command types (AddPublication, AddSubscription, etc.)
- [x] Conductor for lifecycle management (publications, subscriptions, images)
- [x] LogBuffers mmap with TermAppender and TermReader
- [x] Publication.Offer() and Subscription.Poll()
- [x] Zero external dependencies (pure Go standard library)
- [P] Subscription.Poll() position tracking needs per-image state
- [P] BroadcastReceiver lapped message recovery needs testing
- [P] TermAppender does not handle term rotation (wrap to next partition)
- [R] Fragment assembler for multi-fragment messages (pure Go, no C)
- [R] TryClaim / BufferClaim zero-copy path

## Cluster Protocol

- [x] Session connect, leader tracking, reconnection, graceful shutdown
- [x] SBE message codec for all 8 cluster protocol templates
- [x] Cluster interface for testability
- [P] Egress channel hardcodes `endpoint=localhost:19876` -- should allocate dynamically
- [P] SBE decode uses wire blockLength for forward compatibility
- [P] No authentication/challenge-response implementation

## Benchmarks

Pending -- need to benchmark the pure Go implementation against the C library path.

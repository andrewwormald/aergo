# The Room With No Doors

*A short story about how `aergo` works.*

---

Maya Okonkwo had a rule she'd stolen from an old trader: *when everyone agrees a thing is impossible, find out who benefits from you believing it.*

In her case the impossible thing was small and specific. She wrote low-latency
services in Go. The fastest messaging transport she knew of was
[Aeron](https://github.com/aeron-io/aeron) — the thing the electronic trading
world used when microseconds were money. And the received wisdom, repeated in
every thread she read, was that to speak Aeron from Go you had to swallow C.
You linked against `libaeron`, you turned on CGO, you inherited a C toolchain
and its cross-compilation nightmares and its garbage-collector-hostile call
boundary, and you told yourself this was the price of speed.

Maya didn't want to pay it. So she went looking for who benefited from the
bill.

What she found, when she finally sat down and read how Aeron actually worked,
was almost funny. The fast part of Aeron — the part everyone was so afraid of
— wasn't a library at all. It was a *separate process*. A daemon called the
**media driver**, `aeronmd`, sitting off to the side, doing all the real work
of moving bytes between machines. The C library everyone linked against was
just a *client*. A translator. And the media driver and its clients didn't
talk to each other through function calls or sockets.

They talked through a room.

---

## The room

The room was a file. Specifically a file the driver created in a shared-memory
directory — on Linux, `/dev/shm/aeron-<user>` — called `cnc.dat`. CnC for
*Command-and-Control*. When you memory-mapped that file into your process with
`MAP_SHARED`, the operating system did something that felt like a magic trick:
the exact same physical pages of RAM appeared inside your address space *and*
inside the driver's, at the same time. Write a byte to your copy and the
driver saw it instantly, because there was no "copy." There was one room, and
both of you were standing in it.

This was the whole secret. This was the thing you didn't need C for. You needed
`syscall.Mmap` and the nerve to do pointer arithmetic on the result.

Maya opened a file called `cnc.go` — the first file she'd write in what became
`aergo` — and mapped the room:

```go
data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()),
    syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
```

The first 128 bytes were a header — two CPU cache lines of it, padded on
purpose. It told her the version of the protocol (she was building for version
26, the layout the 1.46-era drivers used) and, crucially, the *lengths* of the
five regions that followed. Because the room wasn't one open space. It was
partitioned, like a suite:

- a **to-driver** buffer, where clients wrote commands;
- a **to-clients** buffer, where the driver broadcast its replies;
- two **counter** regions — one holding little named gauges, the other their
  metadata;
- and an **error log**, where the driver left notes when something a client
  did was wrong.

She sliced her single mmap'd byte array into those five regions and wrapped
each one in the same small, dangerous, beautiful type.

---

## The dangerous, beautiful type

She called it `AtomicBuffer`, and it was the foundation everything else stood
on. It held two things: an `unsafe.Pointer` into shared memory, and a length.
That was all. Every read and write in the entire system — every command, every
message, every heartbeat — was ultimately this type reaching into the shared
room and touching bytes at an offset.

The trick was that two processes were touching those bytes *at the same time*,
with no lock between them, because a lock in one process means nothing to
another. So `AtomicBuffer` offered two vocabularies. There were the plain reads
and writes — `GetInt32`, `PutInt64` — for when you already owned a region and
nobody was racing you. And there were the *volatile* and *ordered* ones —
`GetInt64Volatile`, `PutInt64Ordered`, `CompareAndSetInt64` — built on Go's
`sync/atomic`, for the contested bytes. A volatile load guaranteed you saw the
freshest value the other process had published. An ordered store guaranteed
that when the other process finally saw your write, everything you'd written
*before* it was already visible too.

That ordering guarantee turned out to be the entire art of the thing. You
wrote a message body into the room, and *then*, with a single ordered store,
you wrote the length that said "this message is real now." A reader spinning on
that length field would see zero, zero, zero — and then, in one indivisible
instant, the length and everything behind it. There was no moment where a
reader could see a half-written message. The publish was the last byte, and it
was atomic.

Maya thought of it as writing a letter, sealing it, and only *then* raising the
flag on the mailbox. The flag was the ordered store. Everyone downstream
watched the flag.

---

## Speaking to the driver

Now she needed to actually say something to the driver: *please give me a
publication.* The to-driver region was shared by every client on the box —
many writers, one reader — so she couldn't just scribble into it. She wrapped
it as a `ManyToOneRingBuffer`.

A ring buffer is a circle of memory with a *tail* (where writers claim space)
and a *head* (where the reader has consumed up to). To write, a client did an
atomic compare-and-swap to bump the tail forward by the size of its record,
which reserved that slice for itself alone. If the record would run off the end
of the circle, the writer laid down a **padding** record to fill the gap and
wrapped around. Then it wrote its bytes, and — the flag again — committed the
record with an ordered store of the record length.

Every command got a unique **correlation ID**, drawn by atomically
incrementing a counter that lived in the ring buffer's trailer. This ID was the
thread you'd follow through the whole conversation: you send command #457, and
somewhere in the future the driver's reply says "regarding #457…". The very
first correlation ID a client ever drew became its permanent **client ID** —
its name, as far as the driver was concerned.

She wrote a thin `DriverProxy` to format the commands. Each was a little packed
struct: your client ID, a fresh correlation ID, a stream ID, the channel string
(`"aeron:udp?endpoint=localhost:40123"`). `AddPublication` was command `0x01`.
`AddSubscription`, `0x04`. Keepalive, `0x06`. Close, `0x0B`. She was, at this
point, a Go program impersonating the C client so precisely that the driver
never knew the difference. That was the con, and the driver was in on it
without knowing.

---

## Listening for the reply

The driver answered on the to-clients region, and here the shape was inverted:
*one* writer (the driver), *many* readers (every client). So this buffer was a
`BroadcastReceiver`, and it had a property that would have horrified Maya if she
hadn't understood it: the driver never waited for you. It broadcast into the
ring and moved on. If a slow client fell behind, the driver would *lap* it —
circle all the way around and overwrite messages the client hadn't read yet.

The receiver handled this honestly. After copying a message out of the room and
into a private scratch buffer, it re-checked a counter the driver bumped
*before* each overwrite. If that counter had lapped past the record it just
read, the copy was poisoned — the receiver threw it away and resynchronized to
the latest safe point, counting the loss. You copied first and validated
second, because in a room with no locks, the only proof your read was clean was
that nobody had started writing over it while you looked.

Maya wrapped all of this — the room, the outbound ring, the inbound broadcast —
in a single coordinator she named, in keeping with Aeron's own vocabulary, the
**`Conductor`**. The Conductor was the beating heart of the client. It had one
method that mattered, `DoWork()`, and the entire system was built on the
assumption that *something* would call it, over and over, forever.

Each turn of `DoWork` did three things. It drained up to ten replies from the
broadcast buffer and dispatched them. It checked whether the driver was still
alive. And, on an interval, it sent its own heartbeat.

---

## The two heartbeats, and the two ways to die

This was where Maya learned to respect the protocol, because it was a protocol
built entirely around the assumption that *the other side might die at any
moment.*

The driver published its own pulse into counter zero: a timestamp it refreshed
constantly. The Conductor read that timestamp on every keepalive cycle, and if
it was older than the driver timeout — ten seconds — it declared the driver
dead. This mattered enormously, because a dead driver would never reply to
anything, and without this check, every `AddPublication` would simply *hang*
until its own fifteen-second deadline. Maya made the client fail fast instead:
the moment the driver's pulse went stale, the Conductor marked itself
*terminated*, and every pending operation was released with a
`DriverTimeoutError` rather than left to rot.

The client had a pulse too, and it kept it in two places. It sent an explicit
keepalive *command* to the driver every half-second. And it found its own
**heartbeat counter** — the driver allocated one per client, tagged with the
client's ID — and stamped the current time into it on the same cadence. If the
client ever stopped, the driver would notice the frozen counter, time the
client out, reclaim its resources, and broadcast a `RespOnClientTimeout`. When
the Conductor saw that message carrying *its own* client ID, it knew it had
been evicted from the room, and it terminated itself the same way.

Two heartbeats, watching each other across shared memory. Either could be the
one that stopped. The whole design was two paranoid parties, each holding a
finger to the other's wrist.

---

## Where the data actually lives

Getting a publication back from the driver was, itself, just the beginning. The
reply — `RespOnPublication` — didn't contain a channel or a socket. It
contained a *filename*: the path to another shared-memory file, the **log
buffer**, where messages for this stream would physically live.

Maya mapped it the same way she'd mapped the room. A log buffer file was four
regions: three equal **term** buffers and a metadata trailer. Three, because
Aeron never stops the world to recycle memory. When a term filled up, the
writer rotated to the next — 0 to 1 to 2 and back to 0 — while readers were
still draining the one behind it. The metadata trailer, a page of it, held the
coordination state: which term was active, whether a subscriber was connected,
the initial term ID, and — packed cleverly into single 64-bit values so they
could be updated atomically — the *tail* of each term, with the term ID in the
high 32 bits and the write offset in the low 32.

To send a message, `Publication.Offer` performed a sequence Maya came to know
by heart, because she'd benchmarked every step of it:

1. Check you're connected and not closed.
2. Read the **publication-limit counter** — a value the driver advanced as
   subscribers consumed. This was flow control. You could only write *up to*
   that position; past it, the offer came back `BackPressured` and you were
   expected to try again later.
3. Atomically add your message's aligned length to the active term's tail,
   which claimed your slice of the term the same way the ring buffer claimed
   space — by moving a number, not by taking a lock.
4. Write a 32-byte frame header and your payload into the claimed slice.
5. Raise the flag: an ordered store of the frame length.

The whole path was lock-free. No mutex was taken anywhere in a successful
`Offer`. And when a claim ran off the end of a term, the first writer to trip
the boundary laid down a padding frame over the remainder and *rotated the
log* — a careful compare-and-swap dance that prepared the next term and
advanced the active-term counter, safe even when several publishers hit the
wall at once. Only one won the rotation; the losers simply saw it was already
done and retried.

The return value was never a boolean. It was a *position* — the absolute
byte-offset the stream had now reached — or one of five negative codes Maya
copied verbatim from the reference client so nobody would be surprised:
`NotConnected` (-1), `BackPressured` (-2), `AdminAction` (-3, "we rotated, just
retry"), `Closed` (-4), and `MaxPositionExceeded` (-5, "this stream is full,
forever, start a new one").

---

## Reading it back

A `Subscription` was the mirror. When a publisher somewhere connected to a
stream you'd subscribed to, the driver sent `RespOnAvailableImage` — and an
**Image** was born: your view onto one specific publisher's log buffer, mapped
into your process.

`Poll` took a brief mutex, but only to snapshot the current set of images —
publishers came and went, and it refused to iterate a list that might change
under it. Having copied the slice, it let go of the lock and read each term
buffer *without* any lock at all, spinning on that same frame-length field. A
length of zero meant "nothing committed here yet." A positive length was the
flag raised — a complete message, safe to read, which it handed to your
`FragmentHandler`.

But there was one more atomic write, and forgetting it was the subtlest bug in
the system. As a subscriber consumed, it had to publish its new read position
back into a **subscriber-position counter** the driver watched. That counter
was how the driver's flow control knew the reader was keeping up — how it
decided to advance the publication limit and let the writer keep writing. Skip
the update, and the publication would sail along happily until it hit exactly
*join-position plus half a term*, and then stall dead, back-pressured against a
reader the driver thought had stopped listening. The reader was fine. It just
hadn't told anyone. In a room with no doors, if you don't announce where you're
standing, everyone assumes you left.

For messages too big for one frame, a `FragmentAssembler` sat in front of the
handler, stitching begin-flagged and end-flagged fragments back together per
session. And for the busy-wait loops that ran through all of this, Maya
borrowed Aeron's own `IdleStrategy`: spin hard for the first ten empty polls,
then start yielding the thread, then park in doublings from a microsecond up to
a millisecond — aggressive when there's work, merciful to the CPU when there
isn't.

---

## The upstairs: a cluster is a conversation with rules

Everything so far lived in one package, `pkg/aeron` — the pure-Go
reimplementation of the shared-memory protocol, the thing that let a Go program
stand in the room. But Maya's actual goal was to talk to an **Aeron cluster**:
a Raft-replicated group of servers where one member was the leader and the
others stood ready to take over. That was the second package, `pkg/cluster`,
and it was a different kind of code entirely — not memory tricks, but a
*conversation with a strict etiquette.*

The etiquette had a wire format: **SBE**, Simple Binary Encoding. Every cluster
message opened with an eight-byte header — block length, template ID, schema
ID, schema version — and the cluster only trusted schema `111`, version `8`.
Get the numbers wrong and the cluster ignored you. Maya hand-wrote a codec for
each of the eight message templates: the `SessionConnectRequest` that opened a
session, the `SessionEvent` the cluster replied with, the `SessionMessageHeader`
that wrapped every application message with a leadership term and session ID,
the `SessionKeepAlive`, the `NewLeaderEvent`, and the challenge/response pair
for authentication.

And she built the conversation itself as a **state machine** — `AeronCluster`,
which advanced one step per `Poll`:

- It created an **egress** subscription first — the channel the cluster would
  reply *down*, stream 102.
- Then **ingress** publications — one to each cluster member, the channels it
  would speak *up*, stream 101.
- It waited for a publication to connect, then sent its
  `SessionConnectRequest`, telling the cluster which egress channel to answer
  on.
- It waited for the reply. An `EventCodeOK` carried the assigned cluster
  session ID, the current leadership term, and *which member was the leader.*
- And then it was `Connected`, and it stayed connected by sending a session
  keepalive every second and listening.

Two details captured the whole point of a cluster. The first was leadership.
Every outbound message went to `leaderPublication()` — indexed by the leader
member ID the cluster had told it about. When a `NewLeaderEvent` arrived
mid-stream, the machine simply updated the index and kept talking, now
addressing a different server, without the application above it noticing a
thing.

The second was resilience. If the connection dropped — a rejection, a timeout,
a session the cluster closed — the machine didn't die. It tore down its
publications and subscriptions and, if auto-reconnect was on, backed off and
started the whole dance again, doubling its wait from one second toward a
thirty-second ceiling, until the cluster came back. A leader could fail. A
whole connection could fail. The state machine's job was to make that look,
from above, like a brief pause.

---

## The interface at the top

The thing Maya handed to the rest of her company was deliberately small. Not
`AeronCluster`, the concrete machine, but `Cluster`, an interface:
`Connect`, `Poll`, `Offer`, `State`, `Close`, and a few accessors for the
session and leader IDs. You wrote an `EgressListener` with four callbacks —
`OnMessage`, `OnSessionEvent`, `OnNewLeader`, `OnChallenge` — and you drove the
whole apparatus with a loop that did nothing but call `Poll()` and, when
`Connected`, `Offer()` your bytes.

The command-line program, `cmd/aergo`, was exactly that loop and nothing more:
point it at the driver's directory and a cluster endpoint, and it connected,
sent "hello cluster from aergo" once a second, logged what came back, and shut
down cleanly on Ctrl-C by sending a proper close request and waiting for the
acknowledgment.

Underneath that thirty-line loop was everything: a state machine, a
hand-written binary protocol, a lock-free publication path, a lapping broadcast
receiver, three rotating term buffers per stream, two heartbeats holding each
other's wrist, and a room in shared memory that a Go program had learned to
enter by reading a file.

---

## What she actually built

Maya's `aergo` was, in the end, a refusal. It refused CGO. It refused
`libaeron`. It refused every third-party Go module — the whole thing leaned on
nothing but the standard library, `go 1.26` and not a single dependency in the
`go.mod`. What it did *not* refuse was the media driver itself: `aeronmd`, the
upstream C/C++ daemon, still ran off to the side doing the actual network I/O.
`aergo` never reimplemented the driver. It reimplemented the *client* — the
translator — and it did so well enough that the driver, standing in the shared
room, could not tell it apart from the C original.

That was the benefit someone else had wanted her not to see: the belief that
the C library *was* Aeron. It wasn't. Aeron was a protocol written in the
layout of a file and the discipline of atomic writes. The library was just the
first program that happened to speak it.

Maya wrote the second one in Go.

---

### A reader's map to the code

- **`cmd/aergo/main.go`** — the driving loop; `Poll` then `Offer`, graceful
  shutdown on signal.
- **`pkg/aeron/`** — the pure-Go shared-memory protocol:
  - `atomic.go` — `AtomicBuffer`, the volatile/ordered/CAS foundation.
  - `cnc.go` — mmap of `cnc.dat`, sliced into the five control regions.
  - `ringbuffer.go` — `ManyToOneRingBuffer`, client→driver commands.
  - `broadcast.go` — `BroadcastReceiver` / `CopyBroadcastReceiver`, driver→clients replies.
  - `command.go` — `DriverProxy`, the command/response type IDs.
  - `conductor.go` — `DoWork`, driver-message dispatch, liveness, the `Image` type.
  - `logbuffer.go` — mmap of term buffers, tail packing, rotation, term read/append.
  - `publication.go` — lock-free `Offer`, flow control, end-of-log handling, return codes.
  - `subscription.go` — `Poll`, the `FragmentHandler`, subscriber-position updates.
  - `assembler.go` — fragment reassembly. `idlestrategy.go` — backoff. `counters.go` — heartbeats. `errors.go` — the offer codes and typed errors.
- **`pkg/cluster/`** — the cluster client:
  - `cluster.go` — the `Cluster` interface and the `AeronCluster` state machine.
  - `messages.go` — the eight SBE message templates (schema 111, version 8).
  - `sbe.go` — little-endian SBE primitives. `listener.go` — the `EgressListener` callbacks.
- **`docs/BENCHMARKS.md`** — a benchmark for every step on the hotpath, driver not required.

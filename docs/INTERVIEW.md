# Interview drill sheet

The questions this project actually invites, with answers you should be able
to give without notes. If you can't answer one from memory, that section of
the code isn't yours yet — go read it and the matching ADR.

**The single most important skill here:** being able to draw the write path
on a whiteboard and name what breaks at each step. Everything else follows.

---

## 1. "Walk me through what happens when I write 1 GB."

1. Client splits it into **16 chunks of 64 MB** (`CHUNK_SIZE`).
2. For each chunk, client asks the master to `AllocateChunk`. Master picks
   **3 chunk servers** ordered by free space, creates chunk metadata, and —
   critically — **appends an operation-log record and fsyncs it before
   replying**. Returns the handle and the three locations.
3. Client asks for a **lease**. Master picks one replica as **primary**,
   bumps the chunk's **version number**, and grants for 60 s. Only one
   primary at a time: that's what orders concurrent writes.
4. Client **pushes the data along a chain** — client → nearest replica →
   next → next — not to all three in parallel. Data flow is decoupled from
   control flow so each machine's outbound bandwidth is used once.
   (`PushData`, GFS §3.1.)
5. Once all three have the bytes buffered, client sends a **write request to
   the primary only**. Primary assigns a serial number, applies locally,
   forwards the order to secondaries. All apply in the same order.
6. Primary acks; client tells the master the new size, which is another
   fsync'd log record.

**Where it breaks:** a secondary that fails step 6 leaves the chunk
*inconsistent* — the client retries. The master finds out at the next
heartbeat and queues re-replication.

---

## 2. "The leader dies mid-write. What does the client see?"

- Writes **already acknowledged** survive — the oplog was fsync'd before the
  reply ([ADR-001](adr/001-operation-log.md)). Verified by
  `test_acknowledged_creates_survive_leader_sigkill`.
- The in-flight write fails. The client's `_execute_with_leader_failover`
  discovers the new leader and retries.
- Measured failover: **1.0 s**. Floor is the election timeout (800–1600 ms).
- **Honest caveat:** a write acknowledged by the old leader alone, that no
  surviving peer ever received, *is lost*. See Q7.

---

## 3. "Why isn't your multi-master actually Raft?"

Say this plainly, don't let them find it:

> It isn't. There's leader election with terms, randomized timeouts and
> majority voting — that part is real Raft. But there's **no replicated log
> and no majority ack before replying to a client**. Metadata is synced to
> peers best-effort. So failover can lose acknowledged writes.

Then: *"I quantified it rather than guessing. My simulator loses writes on
7.6% of fault schedules; catch-up on promotion cuts that to 1.6%, and only
a real replicated log closes it. I implemented full Raft separately in my
`raftkv` project — porting it here is the obvious next step."*

This answer is strictly better than claiming consensus and getting caught.

---

## 4. "Explain Reed–Solomon like I don't know it."

> Split the data into **k shards**, compute **m parity shards**, and any
> **k of the k+m** reconstruct the original. I use RS(6,3): 9 shards, any 6
> rebuild it. That's **1.5× storage instead of 3×**, and it survives **3**
> failures where 3× replication dies on its third.

If they push on mechanics:

> Each byte position is independent. The k data bytes are a vector; multiply
> by a **(k+m)×k encoding matrix** to get k+m output bytes. Lose some — cross
> out those rows, leaving a k×k matrix, invert it, multiply. It works because
> the matrix is built so **every k×k submatrix is invertible** (Vandermonde).
> Ordinary arithmetic overflows a byte, so it's all in **GF(2⁸)**: addition is
> XOR, multiplication is done with log/antilog tables.

**"Why systematic form?"** The first k shards are the input verbatim, so a
read that lost nothing is a concatenation with no decode. That's the
difference between **115 MB/s and 8–22 GB/s** on the common path.

**"Why not just use a library?"** For a product I would — `pyeclib`/Jerasure
are faster. I wrote it because the field arithmetic is the interesting part.

---

## 5. "Why EC for warm data and not everything?"

Encoding costs CPU and a degraded read costs **6 network fetches plus a
matrix inversion** instead of one read from one replica. Hot data is read and
written constantly; paying that on every access is a bad trade. So:
replication for hot, background conversion once a chunk has been untouched
for N seconds. **That's SeaweedFS's design** — writes never pay encode cost.

**The ordering rule that matters:** replicas are dropped **only after every
shard is confirmed placed**. Between those points the chunk is stored twice.
Wasteful for a few seconds, and the only ordering where a crashed conversion
can't lose data.

---

## 6. "What's a bug you found, and how?"

Lead with the simulator one — it's the best story.

> A partitioned leader can't know it's been deposed, so it kept granting
> write leases while the majority side elected a replacement doing the same.
> Two primaries on one chunk means two independent mutation orders and
> replicas silently diverge.
>
> I found it with deterministic simulation testing — whole cluster in one
> thread over virtual time, every source of randomness behind one seed, so
> any failure replays exactly. It appeared on seed 165; my shrinker reduced
> 12 injected faults to **2**, and the trace shows 240 ms of overlap.
>
> Fixed with **leader leases**: a leader may only grant if a majority
> confirmed it within 400 ms — comfortably under the 800 ms minimum election
> timeout, so a partitioned leader stops granting *before* a replacement can
> exist. Violations went from 2 per 500 schedules to 0.

Others, if asked for more: `move_path` crashed on any nested directory
move; `master_lock` was commented out while three call sites used it; the
reconnect path built a protobuf from fields that don't exist; a
single-master cluster could never elect itself.

---

## 7. "What's the failure mode you're least confident about?"

Have a real answer. This question separates people who built something from
people who assembled something.

> Metadata durability across failover. The oplog makes a *single* master
> crash-safe, but the multi-master layer isn't consensus, so a write
> acknowledged by a leader that then dies — with no peer having received it —
> is gone. My simulator still hits that on about **1.2% of fault schedules**
> after both fixes. I know the shape of the fix (majority ack before reply,
> i.e. a real replicated log); I haven't done it.

Runner-up: **the EC conversion window**. If a chunk server crashes
mid-encode, partial shards are left behind. Harmless — replicas are retained
and the next attempt overwrites — but it's the code path I'd want fuzzed
harder before trusting in production.

---

## 8. "Your group commit made it slower. Explain."

> Standard WAL optimization: batch concurrent mutations into one fsync. I
> implemented it, measured it, and it was **3× slower** — 124 ops/s versus
> 377.
>
> The reason is specific to my master: `commit()` runs while holding
> `metadata_lock`, so mutations are already serialized. Only one thread is
> ever in the commit path, no second writer arrives during the batching
> window, so no batch ever forms — the wait is pure added latency, capping
> throughput at 1/window.
>
> I turned it off by default and documented why. The real prerequisite is
> narrowing the lock, which is a genuine concurrency change I didn't want to
> make on a hunch.

**Why this answer lands:** it shows measurement beating intuition, and the
discipline to ship the slower-sounding-but-faster configuration.

---

## 9. Numbers to know cold

| | |
|---|---|
| 1 GiB write / read | 76.5 / 230 MB/s |
| 32 concurrent clients | 32/32, 272 MB/s aggregate |
| Failover | 1.0 s |
| Node loss → RF=3 restored | 10.1 s |
| Storage overhead, EC | 3.0× → 1.5×, 3 failures tolerated vs 2 |
| RS encode / degraded decode / intact read | 115 / 115 MB/s / 8–22 GB/s |
| Client cache | 80% fewer master RPCs |
| Namespace ops | 377/s durable (vs 320/s non-durable before) |
| Simulation | 1000 seeds × 20 virtual s in 3.1 s (~6500×) |
| Safety violations / 500 schedules | 40 → 6 |
| Tests | 188 |

**All measured on a laptop over loopback.** Say that unprompted — every
"network" hop is a memory copy, so replication looks cheaper than it is. The
Ada multi-node scripts exist to get honest numbers.

---

## 10. Questions you should ask back

- "How do you handle metadata scale — have you had to shard the namespace?"
  (Shows you know the single-master ceiling and what Colossus/Tectonic did.)
- "Do you run deterministic simulation or Jepsen-style testing in CI?"
- "Where do you sit on the replication-vs-erasure-coding line, and what
  drives the threshold?"

---

## Things NOT to say

- ❌ "It implements Raft consensus." It doesn't. See Q3.
- ❌ "It's production-ready." It's a 10K-line study implementation in Python.
- ❌ Quoting throughput without saying it's single-machine loopback.
- ❌ "It handles any number of failures." RS(6,3) handles 3. Be precise.
- ❌ Claiming the simulator proves correctness. It tests the protocol *as
  modelled*; it says nothing about threading, gRPC, or real disks.

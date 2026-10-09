# ADR-002 — Group commit: batch concurrent mutations into one fsync

**Status:** implemented · **Code:** `oplog.py` (`OperationLog._await_durable`) · **Tests:** `tests/test_oplog.py::test_group_commit_batches_concurrent_appends`

## The problem

[ADR-001](001-operation-log.md) puts an fsync on the request path. An fsync on
this machine costs on the order of a millisecond, which caps the master at
roughly **1000 namespace ops/sec no matter how many threads are calling** —
the bottleneck is the disk barrier, not CPU, so adding workers does nothing.

That ceiling is low enough to matter: a job creating 100k output files would
spend 100 seconds purely in fsync.

## What the paper does

§2.6.3, same passage:

> "...we batch several log records together before flushing, thereby reducing
> the impact of flushing and replication on overall system throughput."

The paper does not elaborate, because the technique long predates it — group
commit goes back to database write-ahead logs in the 1980s, and every serious
WAL implementation (PostgreSQL's `commit_delay`, InnoDB's group commit,
etcd's batched WAL) has some form of it.

## Decision

When a writer needs an fsync and nobody else is already doing one, it becomes
the **batch leader**: it waits up to `OPLOG_GROUP_COMMIT_MS` (default 5 ms)
*holding the log lock*, so every writer that arrives in that window queues up
behind it, and then issues a single fsync covering all of them.

Holding the lock during the wait looks wrong at first glance and is the entire
mechanism — the point is to let writers accumulate. The window is single-digit
milliseconds and bounded.

Each writer then checks whether the flush covered its own sequence number, so
a writer that arrived just after the batch closed waits for the next one
rather than returning early on someone else's fsync. Getting that check wrong
would report durability that never happened, which is worse than the original
bug.

### Rejected alternatives

- **A dedicated flusher thread** with writers posting to a queue. Cleaner
  separation, and what a production WAL usually does. Rejected for one more
  thread and one more handoff on the hot path, for no measurable gain at this
  scale.
- **`fdatasync` instead of `fsync`.** Skips the metadata flush and is
  meaningfully faster on Linux. Not portable to macOS, which is where this
  gets developed and demoed, and on macOS even `fsync` does not force the
  drive's own write cache (`F_FULLFSYNC` does, at a large cost). Worth saying
  out loud: **on a Mac, "durable" here means durable against process crash,
  not against sudden power loss.**
- **No group commit, just a bigger thread pool.** Does not help — the disk
  barrier serializes regardless of thread count.

## Consequences

- Up to 5 ms of added latency on a mutation to an otherwise-idle master. For
  a single `CreateFile` with nothing else happening, this design is *slower*
  than one fsync per op. It only wins under concurrency. That trade is
  deliberate: the idle case is already fast enough to not matter.
- `GFS_OPLOG_GROUP_COMMIT_MS=0` disables batching entirely, which is how
  Phase 3.5 measures what it is worth.

## Measured — and the decision was wrong

`bench/bench_metadata.py`, 16 concurrent writer processes:

| Configuration | Namespace ops/sec | Durable per-op |
|---|---|---|
| snapshot (original 30 s JSON dump) | 320.5 | no |
| oplog, fsync per op | **376.8** | yes |
| oplog, 5 ms group commit | **124.2** | yes |

**Group commit made it three times slower.** The reasoning above is sound in
general and does not apply to this master, because `commit()` is called
*while holding `metadata_lock`*. Mutations are therefore already fully
serialized: only one thread is ever inside `_await_durable`, no second writer
can arrive during the window, and no batch ever forms. The 5 ms wait becomes
pure per-operation latency, capping throughput at 1/window ≈ 200 ops/s — and
124 is what that looks like with the rest of the work included.

`test_group_commit_batches_concurrent_appends` passes and is not wrong; it
drives `OperationLog` directly with 24 real threads, which is a situation the
master never actually creates.

**Default changed to `GFS_OPLOG_GROUP_COMMIT_MS=0`.** The machinery stays,
because it is correct and becomes valuable the moment the lock is narrowed —
but shipping it on, having measured it, would be indefensible.

The real prerequisite is moving `commit()` outside `metadata_lock`: take the
lock to mutate in-memory state, release it, then make the record durable
before replying. That is a genuine concurrency change (two mutations could
then be made durable out of order relative to their in-memory application)
and is deliberately not being done on a hunch. It is the clearest next
optimisation for the master, and it is listed as such.

### The second-order finding

The operation log is **faster than the snapshot it replaced** (377 vs 320
ops/s) as well as being durable. That is not a coincidence: the snapshot
rewrote the entire namespace on every mutation, so its cost grew with the
number of files, while a log append is O(1). The durability improvement was
free.

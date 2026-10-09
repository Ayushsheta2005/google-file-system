# ADR-010 — Thread pool 10 → 64 on both servers

**Status:** implemented · **Code:** `gfs_config.py` (`DEFAULT_MAX_WORKERS`)

## The problem

Both servers were built as:

```python
grpc.server(futures.ThreadPoolExecutor(max_workers=10))
```

Ten threads is the cap on **concurrently executing RPCs** for the whole
process. The 11th client blocks in the queue. The project's README advertised
support for many concurrent clients; the transport could not deliver more than
ten at a time, and a chunk server servicing a slow 64 MB read holds one of
those ten slots for the duration.

## Decision

Default to **64**, exposed as `--max-workers` and `GFS_MAX_WORKERS`.

Chosen rather than something larger because these are threads doing blocking
file I/O, not CPU work. Under Python's GIL, more threads buy concurrency only
while they are blocked in syscalls — which, for a storage server, is most of
the time, so 64 is useful where 64 CPU-bound threads would not be. Past a few
hundred, context-switch overhead and memory start to dominate.

### Rejected

- **asyncio / `grpc.aio`.** The right answer for an I/O-bound server and it
  would sidestep the GIL question entirely. Rejected as a rewrite of all three
  modules for a benefit this project can get more cheaply; noted as future
  work.
- **A thread per connection, unbounded.** No backpressure; a client storm
  turns into memory exhaustion instead of queueing.
- **Leaving it at 10 and claiming 10 concurrent clients.** Technically true
  and exactly the kind of claim that collapses under one follow-up question.

## Consequences

- Lock contention inside the master becomes the next bottleneck, not the
  thread pool. `metadata_lock` is held across whole handler bodies in several
  places; with 64 workers that is where the queue forms. Deliberately **not**
  optimized yet — Phase 3.2 profiles it at N=32 first, because narrowing lock
  scope without a measurement is how correctness bugs get introduced.
- Idle memory cost of 64 stacks per process. Immaterial here.

## Measured

Phase 3.2 sweeps N = 1…32 client processes and reports the scaling curve
before and after.

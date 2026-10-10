# Build progress

Working log for the Tier-1 rebuild. Updated as each task lands so work resumes
cleanly after an interruption. Design rationale lives in `docs/adr/`.

**Legend:** `[x]` done · `[~]` in progress · `[ ]` not started

---

## Phase 0 — Foundation

- [x] **0.0 Environment**
  - `.venv` (Python 3.9.6, grpcio 1.80.0, numpy 2.0.2, pytest)
  - protobuf stubs regenerated
  - `gfs_config.py`: every tunable in one place, env-overridable so the
    benchmark suite can A/B a change without a rebuild
  - `--data-dir` / `--storage-dir` / `--max-workers` / `--log-level` added to
    both entrypoints; data dirs no longer collide in cwd
  - shared gRPC options applied to all 10 channel sites + both servers
  - `cluster.py`: no-Docker local cluster (spawn, await leader, kill, pause,
    restart, diagnostics) — used by dev, tests, benchmarks and DST
  - smoke test green: 3 masters + 3 chunkservers, pipelined write, read back
    verified byte-for-byte
- [x] **0.1 Operation log** — `oplog.py`: CRC32-framed records, fsync before reply, group commit
- [x] **0.2 Checkpoints** — atomic rename + dir fsync, log truncation, torn-tail recovery, fallback to older checkpoint
- [x] **0.3 Wire mutating handlers** — all 13 persistence sites now log; 30s JSON dump retired (kept behind `GFS_OPLOG_MODE=snapshot` for A/B)
- [x] **0.4 Re-replication** — `replication.py`: priority queue, heartbeat piggyback, target pulls, throttled, stall timeout
- [x] **0.5 Chunk GC** — `chunks_to_delete` issued from a diff of what each server reports
- [x] **0.6 Client metadata cache** — `metacache.py`: LRU+TTL, invalidate on stale/mutation/subtree, counters
- [x] **0.9 `GetClusterStatus` RPC** — replication health readable directly (added so tests and benchmarks stop inferring it from files on disk)
- [x] **0.7 Concurrency** — `max_workers` 10 → 64, message cap 4 MB → 128 MB, keepalive, channel reuse
- [x] **0.8 Timing** — heartbeat 30s → 5s, election 5–8s → 0.8–1.6s, initial 10s → 3s

## Phase 1 — Erasure coding

- [x] **1.1** Reed–Solomon over GF(2⁸), numpy-vectorized — 42 tests, exhaustive erasure patterns, 115 MB/s encode, 1.50× overhead
- [x] **1.2** EC storage mode — `ec_storage.py` + `ec_manager.py`, shards on distinct hosts, client degraded read
- [x] **1.3** Hot→warm converter — replicas retired only after all k+m shards confirmed
- [x] **1.4** Shard repair — rebuild one shard from k survivors, via the heartbeat queue

## Phase 2 — Deterministic simulation testing

- [x] **2.1** Virtual clock + seeded PRNG + message bus — `simulation/core.py`
- [x] **2.2** Fault injection — delay, drop, reorder, partition, crash, pause (SIGSTOP-equivalent)
- [x] **2.3** Four invariants, checked after every event
- [x] **2.4** Shrinking — 40 faults reduced to 3–10

## Phase 3 — Benchmarks (last, by design)

- [x] **3.1** Harness + 1 GiB sequential R/W — `bench/bench_throughput.py`
- [x] **3.2** Concurrency sweep N = 1…32 processes — `bench/bench_concurrent.py`
- [x] **3.4** Failover, chunkserver-loss recovery, EC conversion — measured in the e2e tests
- [x] **3.5** A/B: cache on/off, oplog vs snapshot, group commit on/off
- [ ] **3.3** 16-writer record-append contention — *not done*; the lease path
      is exercised by the simulator but never benchmarked under contention
- [ ] **3.6** Results table in the README — results are in `bench/results/*.json`
      and summarised here, but the README still describes the old system
- [ ] **3.6** Results → JSON + markdown → README

## Throughout

- [x] ADRs in `docs/adr/` — 11 records, one per decision
- [x] `docs/INTERVIEW.md` — drill sheet
- [x] `ada/` — setup, 4 SLURM jobs, multi-node runner (**not yet run**)
- [ ] Optional, not done: FUSE mount, Prometheus/Grafana, rack-aware placement

## Known gaps

Worth being able to name these; an interviewer who reads the code will find
them anyway.

1. **The metadata path is not consensus.** No replicated log, no majority ack
   before reply. The simulator still loses acknowledged writes on ~1.2% of
   fault schedules after both fixes. Porting the Raft implementation from the
   sibling `raftkv` project is the real fix.
2. **Group commit is implemented but disabled**, because `commit()` runs
   under `metadata_lock` so no batch ever forms. Narrowing that lock is the
   clearest next optimisation for the master.
3. **Record-append is untested under contention.** 16 concurrent appenders to
   one file was planned (3.3) and never written.
4. **The README still describes the pre-rebuild system**, including the
   "Raft-like consensus" claim that ADR-007 contradicts.
5. **Every number is single-machine over loopback.** The `ada/` scripts exist
   to get honest multi-node figures and have not been run.

---

## Numbers so far

| Measure | Before | After |
|---|---|---|
| Failover: leader SIGKILL → first successful write | ~5 s floor (election timeout) | **1.01 s** |
| Node loss → replicas back at RF=3 | never (warn-only) | **10.1 s** |
| Deleted file reclaimed from chunk server disk | never | within a heartbeat round |
| Acknowledged metadata ops lost on master crash | up to 30 s worth | **0** |
| Cold boot, 3 masters + 4 chunk servers | 20.5 s | **5.5 s** |
| 1 GiB file, verified byte-for-byte | not measured | **76.5 MB/s** write, **230 MB/s** read |
| Concurrent clients | not measured | **32/32**, **272 MB/s** aggregate |
| Client cache | none | **80%** fewer master RPCs |
| Namespace ops, durable | 320/s (not durable) | **377/s** |
| Storage overhead after EC | 3.00× | **1.50×**, tolerating 3 losses vs 2 |
| Cold chunk → erasure-coded | n/a | **7.1 s** |
| Protocol safety violations / 500 fault schedules | 40 | **6** |
| Tests | 1 ad-hoc script | **188 passing** |

## Protocol bugs found by the simulator (Phase 2)

Both confirmed and fixed in `master.py`, not just in the model. Full detail
in [ADR-007](docs/adr/007-deterministic-simulation.md).

**A. Two primaries on one chunk.** A partitioned leader cannot learn it has
been deposed, and kept granting write leases while the majority side elected
a replacement doing the same. Reproduced on seed 165, shrunk to two faults,
240 ms of overlap in the trace. This is issue (1) from `textfiles/issues.txt`
— previously a hunch with no reproducer. Fixed with leader leases: a leader
may grant only while a majority confirmed it within 400 ms, comfortably below
the 800 ms minimum election timeout.

**B. Acknowledged writes lost across failover.** A master that was down when
writes were acknowledged could win the next election and serve state missing
them. Not really a bug so much as the absence of consensus — no replicated
log, no majority ack before reply. Partially fixed with catch-up on
promotion; the residual case is measured, not hidden.

| Configuration | Violations / 500 seeds | durability | lease safety |
|---|---|---|---|
| baseline | **40** | 38 | 2 |
| + catch-up on promotion | 10 | 8 | 2 |
| + leader lease only | 32 | 32 | **0** |
| + both | **6** | 6 | **0** |

1000 seeds × 20 virtual seconds runs in **3.1 s** (~6500× speedup).

## Bugs found and fixed in the existing code

These were all pre-existing; each was found by a test written for the work
above, not by reading. Worth keeping for the interview doc.

1. **`move_path` crashed on any directory containing a subdirectory.**
   `src_dir` satisfies its own `startswith(src_dir)` prefix test, so it was
   both moved explicitly and queued in `items_to_move`; the second pass hit
   `KeyError` on a key it had just deleted.
2. **`master_lock` was commented out while three call sites still used it.**
   Every one of those paths raised `AttributeError`. They only run when no
   leader is known, which is why it survived.
3. **The reconnect path built a `HeartbeatRequest` with fields that do not
   exist** (`chunk_server_id`, `chunk_server_address`, `available_chunks` —
   they belong to a commented-out message in the .proto), so it raised
   `ValueError` every time it ran.
4. **Two unreconciled connection schemes in the chunk server.**
   `connect_to_master()` set `master_stub`; `send_heartbeat()` read
   `leader_stub`. On the fallback path a chunk server would reconnect
   "successfully" and then heartbeat into a `None` stub forever.
5. **`chunks_to_delete` was typed `repeated ChunkLocation`** when it names
   chunks, not servers. Nothing caught it because nothing used it.
6. **Fixing 2–4 cut cold-boot time from 20.5 s to 5.5 s** — the slow path had
   been failing silently and falling back to a 10 s retry timer.

## Notes / gotchas hit so far

- Python 3.9 is the only interpreter on this machine; no `match`, no `X | Y`
  type syntax. `tuple[...]` annotations are fine.
- `timeout(1)` is not installed on macOS — use Python-side deadlines in scripts.
- Cold boot of a 3+3 cluster is ~15 s, dominated by the initial election
  timeout plus the first chunkserver heartbeat round.
- `await_leader()` deliberately waits for chunk servers to *register*, not just
  for a leader to exist — a leader with no chunk servers cannot allocate, and
  returning early only pushes the failure into the caller.

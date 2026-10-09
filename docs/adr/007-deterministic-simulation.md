# ADR-007 — Deterministic simulation testing

**Status:** implemented · **Code:** `simulation/` · **Tests:** `tests/test_simulation.py` (21)

## The problem

The concurrency bugs in a distributed system live in orderings that happen
rarely and never twice. `tests/test_crash_recovery.py` kills a real master
and hopes to catch one; it usually does not, and when it does the run is not
reproducible, so the bug cannot be studied.

The project's own `textfiles/issues.txt` records two such suspicions:

> 1) lease timeout is only 60 seconds but chunk server cannot extend lease
>    leading to race conditions
> 2) a new server (master) can become leader if election timeout exceeds the
>    time it takes to connect.

Both were written as hunches. Neither had a reproducer.

## What production systems do

[FoundationDB pioneered this around 2010](https://antithesis.com/docs/resources/deterministic_simulation_testing/)
and [TigerBeetle made it protocol-aware](https://tigerbeetle.com/blog/2026-08-20-protocol-aware-dst/).
The shape is the same in both: run the system's logic in **one thread over
virtual time**, and route *every* source of nondeterminism — the clock,
message latency, loss, reordering, which node crashes when — through a single
seeded PRNG. A whole run becomes a pure function of one 64-bit seed.

Adopted since by Convex, RisingWave and others. It is the single highest-value
testing technique available for this kind of system.

## Decision

A `simulation/` package with a virtual clock, a seeded event loop, and an
in-process message bus supporting delay, drop, reorder, partition, crash and
**pause** (SIGSTOP-equivalent — a node that is alive but not running, whose
timers resume with stale state, which is what actually breaks lease
protocols and is distinct from a crash).

Four invariants, checked after **every event** rather than at the end — a
safety violation that exists for three virtual milliseconds and then resolves
is still a violation, and end-state checking misses exactly the transient
bugs this exists to find:

| Invariant | What it protects |
|---|---|
| `at_most_one_leader_per_term` | election safety; two leaders in one term means someone voted twice |
| `no_two_leases_on_one_chunk` | mutation ordering; two primaries means replicas silently diverge |
| `committed_writes_are_never_lost` | the durability claim in [ADR-001](001-operation-log.md) |
| `term_never_decreases` | monotonicity; a node whose term went backwards re-grants votes it already cast |

Plus **shrinking**: on failure, remove faults one at a time and keep any
subset that still fails. A failure with 40 faults says almost nothing; the
same failure reduced to three usually names the bug outright.

### What is modelled, and what is not

The simulator models the *protocol* in `master.py` — the election state
machine, lease granting, heartbeat placement — rather than importing the
real classes, which are built around threads, gRPC stubs and `time.time()`.

**That is a real limitation and worth stating plainly**: a bug the simulator
finds is a bug in the protocol *as modelled*, so the next step is always to
confirm it against the real implementation and fix it there. Both bugs below
were confirmed and fixed in `master.py`, not only in the model.

The replication and erasure-coding planners are **not** modelled — the real
`ReplicationManager` and `ECManager` are driven directly. They hold no gRPC
objects and no master reference specifically so this would be possible.

### Rejected alternatives

- **Jepsen.** The reference tool, and it tests a real cluster through real
  faults — strictly more convincing. Rejected because it is Clojure, it
  cannot replay a failure exactly, and the setup exceeds this project's
  scope. Worth saying: Jepsen and DST answer different questions, and a
  serious system runs both.
- **Porcupine-style linearizability checking.** Powerful, and the wrong
  model: GFS deliberately is not linearizable (§2.7 defines
  consistent-but-undefined regions). A checker built on the wrong
  consistency model reports noise.
- **More random `SIGKILL`s against the real cluster.** Cheap to write, and
  it is what the existing slow tests already do. It finds shallow bugs and
  cannot reproduce anything.

## Consequences

- Two descriptions of the protocol now exist and can drift. The model is a
  liability if nobody maintains it. Mitigated by importing the timing
  constants from `gfs_config`, so at least the numbers cannot diverge.
- A green simulator proves nothing about threading bugs, gRPC behaviour, or
  disk failures. It covers protocol logic only.
- Determinism is easy to break by accident. Several tests exist purely to
  pin it down: same seed → identical trace, ties break on sequence number,
  a clean run must still show delivered and dropped messages.

## Bugs found

### 1. Two primaries on one chunk (safety)

Reproduced on seed 165, shrunk from 12 faults to **two**: `pause m0`,
`restart m0`. The trace:

```
9.8423  m0 grants lease on chunk2 to cs0 until 10.342
9.9781  PAUSE m0
10.0994 m2 grants lease on chunk2 to cs3 until 10.599
```

m0 is partitioned from the others but **cannot know it has been deposed** —
nothing reaches it to say so — and keeps granting leases while the majority
side elects m2, which grants leases on the same chunks. 240 ms of two
primaries, which in the real system means two independent mutation orders
and replicas that diverge.

This is issue (1) from `issues.txt`, with a reproducer.

**Fix:** leader leases. A leader may grant a write lease only if a majority
confirmed its leadership within `LEADER_LEASE_WINDOW` (400 ms), comfortably
below the 800 ms minimum election timeout — so a partitioned leader stops
granting strictly before a replacement can be elected. Implemented in
`master.has_leader_lease()` and gated in `grant_lease()`.

### 2. Acknowledged writes lost across failover (durability)

Reproduced on 38 of 500 fault schedules, routinely shrinking to two faults
(`crash m1`, `restart m1`). A master that was down or partitioned while
writes were acknowledged can win the next election holding stale state and
serve a namespace missing them.

**This is not a bug so much as the absence of consensus**, which the README
obscured by calling the design "Raft-like". There is no replicated log and no
majority ack before replying, so this outcome is a direct consequence of the
architecture.

**Partial fix:** catch-up on promotion. A new leader probes every reachable
peer's `metadata_version` and pulls the newest before serving
(`catch_up_from_peers()`, new `FetchMetadata` RPC).

It narrows the window; it does **not** close it. A write acknowledged by the
old leader alone, which no surviving peer ever received, is still gone. Only
majority-ack-before-reply fixes that, and that means a genuine replicated
log. The residual rate is measured below rather than hidden.

## Measured

1000 seeds × 20 virtual seconds in **3.1 s of real time** — a ~6500×
speedup, which is what makes exploring thousands of schedules practical.

Violations per 500 seeds, by configuration:

| Configuration | Total | durability | lease safety |
|---|---|---|---|
| baseline (as originally written) | **40** | 38 | 2 |
| + catch-up on promotion | 10 | 8 | 2 |
| + leader lease only | 32 | 32 | **0** |
| + both fixes | **6** | 6 | **0** |

So: the leader lease **eliminates** the lease-safety bug, catch-up cuts
durability violations by ~79%, and together they take total violations down
**85%**. The remaining 6 are the irreducible no-consensus case, and they
stay on the board until the metadata path becomes a real replicated log.

`at_most_one_leader_per_term` and `term_never_decreases` never fired, before
or after. The election logic was already sound; it was everything built on
top of it that was not.

Shrinking reduces 40-fault schedules to 3–10 faults.

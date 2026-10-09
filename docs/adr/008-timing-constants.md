# ADR-008 — Heartbeat and election timeouts cut by roughly 6×

**Status:** implemented · **Code:** `gfs_config.py`

## The problem

| Constant | Was | Effect |
|---|---|---|
| `HEARTBEAT_INTERVAL` | 30 s | liveness checks used `HEARTBEAT_INTERVAL * 2`, so **a dead chunk server took 60 s to notice** |
| `ELECTION_TIMEOUT_MIN/MAX` | 5000–8000 ms | failover could not beat ~5 s however fast the code was |
| `INITIAL_ELECTION_TIMEOUT` | 10 s | cold boot of a cluster idled for 10 s before electing anyone |
| `MASTER_HEARTBEAT_INTERVAL` | 200 ms | fine |

The comment in the original source says these were "increased for network
delays" — a reasonable reaction to flaky elections, but it treats the symptom.
The project's own `textfiles/issues.txt` records the underlying race:

> "a new server (master) can become leader if election timeout exceeds the
> time it takes to connect."

Padding the timeout hides that race rather than fixing it, and the cost is
that every failure takes a minute to detect.

## What the paper and Raft say

GFS §2.7: the master "polls chunkservers for their chunk information" and uses
regular HeartBeat messages; the paper does not pin numbers, but the design
assumes detection on the order of seconds.

Raft §5.2 recommends **broadcast time ≪ election timeout ≪ MTBF**, with
broadcast time typically 0.5–20 ms and election timeouts of 150–300 ms for a
LAN. Our masters are on loopback, so even 800 ms is conservative by an order
of magnitude.

## Decision

| Constant | Now | Reasoning |
|---|---|---|
| `HEARTBEAT_INTERVAL` | 5 s | detection in ≤15 s with a 3-miss limit |
| `HEARTBEAT_MISS_LIMIT` | 3 | explicit, instead of the implicit `* 2` |
| `ELECTION_TIMEOUT_MIN/MAX` | 800–1600 ms | ~5× the master heartbeat; the spread exists to break split votes |
| `INITIAL_ELECTION_TIMEOUT` | 3 s | long enough to find an existing leader before campaigning |
| `MASTER_HEARTBEAT_INTERVAL` | 150 ms | unchanged in spirit |

All of them are environment-overridable, so the simulation tests in Phase 2
can compress or stretch time without editing code.

### Rejected

- **Raft's 150–300 ms directly.** Correct for a tuned LAN cluster; too tight
  for Python processes competing for a laptop's CPU during a benchmark, where
  a GC pause or scheduler hiccup would cause spurious elections. 800 ms keeps
  margin without being slow enough to notice.
- **Leaving the padding and documenting it.** Rejected: a 60-second failure
  detector makes re-replication ([ADR-003](003-rereplication.md)) impossible
  to demo and makes the failover benchmark meaningless.

## Consequences

- More election traffic at idle. Negligible at this scale.
- Spurious elections become possible if a master is paused longer than 1.6 s.
  That is a real exposure, and it is exactly what the deterministic simulation
  tests in Phase 2 are built to hunt — `Proc.pause()` exists to inject it.
- The underlying startup race from `issues.txt` is **not fixed by this ADR**,
  only made more likely to surface. That is intentional: it should be found by
  the simulator and fixed properly, not papered over with a longer timeout.

## Measured

`test_cluster_keeps_serving_while_a_master_is_down` reports **1.01 s** from
SIGKILL of the leader to the first successful client write. The old floor was
~5 s before any code ran.

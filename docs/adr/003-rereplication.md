# ADR-003 — Re-replication driven by heartbeat piggyback, target pulls

**Status:** implemented · **Code:** `replication.py`, `master.py` (`Heartbeat`), `chunk_server.py` (`_clone_chunk`) · **Tests:** `tests/test_replication.py`, `tests/test_rereplication_e2e.py`

## The problem in our code

The master detected under-replication and then did nothing:

```python
# master.py, before
if len(alive_locations) < REPLICATION_FACTOR:
    self.logger.warning(f"Chunk {handle} is under-replicated: ...")
```

`ReplicationTask` and `replication_tasks` were defined in `gfs.proto` and
referenced by **zero lines of Python**. So "fault tolerance with replication
factor 3" was true exactly until the first node died, after which the cluster
stayed permanently degraded, silently, with a warning in a log nobody reads.
A second failure then loses data.

The scan also ran on the same 30-second loop as the old metadata dump, and
liveness used `HEARTBEAT_INTERVAL * 2` = 60 s, so detection alone took a
minute before any repair could start.

## What the GFS paper does

§4.3. The master re-replicates "as soon as the number of available replicas
falls below a user-specified goal", prioritising chunks by how far below the
goal they are, how recently the chunk was written, and whether the chunk is
blocking client progress. It explicitly throttles: clone traffic is limited
per-server and cluster-wide so recovery does not starve foreground work.

Instructions reach chunk servers **by piggybacking on the HeartBeat reply**
(§4.4 describes the same mechanism for garbage collection). The master never
initiates a connection to a chunk server.

## What production systems did

- **HDFS** does the same thing with the same vocabulary — under-replicated
  blocks queued by priority, replication commands returned in the DataNode
  heartbeat response, with `dfs.namenode.replication.max-streams` throttling.
  The convergence is not a coincidence; HDFS is a GFS reimplementation.
- **Ceph** is the interesting contrast: CRUSH computes placement
  deterministically from the cluster map, so OSDs work out for themselves
  what they should hold and peer directly. No central queue at all. That
  scales better but gives up the master's ability to prioritise and throttle
  globally.

## Decision

A `ReplicationManager` that holds **no gRPC objects and no master reference**
— it is fed plain dicts and returns plain decisions, which is what lets the
Phase 2 simulator drive it without a cluster.

Three sub-decisions worth defending:

**1. Piggyback on heartbeats, don't push.** A chunk server that is down or
overloaded simply does not ask for work; nothing has to time out to discover
that. The message was being sent anyway, so instructions cost no extra RPC.

**2. The target pulls; the master never moves bytes.** The target is by
definition the node with spare capacity (it is the one being given more
data), and a pull means a dead target wastes nobody else's bandwidth. The
clone reuses the ordinary `ReadChunk` path rather than a dedicated RPC — one
less code path that can diverge from the one clients exercise constantly.

**3. Completion is learned from the next heartbeat, not acknowledged.** The
target reports the chunk; the master marks the clone done. Consistent with
[ADR-001](001-operation-log.md) never persisting locations: the chunk
server's disk is the authority, and an ack only says it *believed* the write
succeeded.

Priority is replica count ascending — a chunk on its last copy is one failure
from being gone forever and must be copied before one that merely dropped
from 3 to 2. That ordering is the difference between losing data and not.

### Rejected alternatives

- **Master pushes clone commands directly.** Needs connection management to
  every chunk server and a timeout policy to notice dead ones — both of which
  the heartbeat already provides for free.
- **Source pushes to target.** Puts the load on a node that is already
  serving reads for an under-replicated chunk, which is the one node you most
  want to leave alone.
- **A dedicated `CloneChunk` RPC.** Marginally cleaner for version checking.
  Rejected to avoid a second, rarely-exercised read path; the task already
  carries the expected version.
- **No throttling.** Losing one node would point every missing replica at
  whichever server has the most free space and saturate it, turning a
  degraded cluster into an unavailable one. Capped at 2 clones per server and
  8 cluster-wide.
- **Erasure-coded repair instead of cloning.** Deferred to
  [ADR-005](005-erasure-coding.md) — replication repair has to work first,
  since EC only applies to warm data.

## Consequences

- A clone slot held by a target that dies mid-copy would block that chunk
  forever, so assignments expire after `clone_timeout` and requeue. Without
  that the queue silently stops making progress while still looking busy —
  `test_a_stalled_clone_is_requeued_after_its_timeout` pins it.
- The scan interval (3 s) and heartbeat interval (5 s) now set the floor on
  recovery time. See [ADR-008](008-timing-constants.md).
- A chunk whose every replica is on a down server is logged as unavailable
  rather than queued; there is nothing to copy from. Surfaced separately as
  `unavailable_chunks` in `GetClusterStatus`, because that is the number that
  means "data loss is imminent", as distinct from "durability is degraded".

## Measured

`test_replicas_are_restored_after_a_chunkserver_dies` kills a chunk server
holding a replica and waits for the master's own `under_replicated` counter
to return to zero:

**detect + re-replicate after node loss: 10.1 s** (3 s scan + 5 s heartbeat
interval + clone). Previously: never.

# ADR-004 — Lazy garbage collection, driven by what servers report

**Status:** implemented · **Code:** `replication.py` (`plan_deletions`), `chunk_server.py` (`_handle_deletions`) · **Tests:** `tests/test_replication.py`, `tests/test_rereplication_e2e.py`

## The problem in our code

`HeartbeatResponse.chunks_to_delete` existed in the proto and was referenced
by zero lines of Python. Deleting a file removed it from the master's map and
left every replica on disk **forever**. A workload that creates and deletes
files — which is most workloads — fills the cluster until it stops.

The field was also mistyped as `repeated ChunkLocation` when it is naming
chunks, not servers. Nothing caught it because nothing used it.

## What the GFS paper does

§4.4, and it is one of the paper's better arguments. Deletion is **lazy**:
the file is renamed to a hidden name, actually removed after a few days, and
the orphaned chunks are reclaimed in the background. The reasoning:

> "...it is simple and reliable in a large-scale distributed system where
> component failures are common... storage reclamation is done in the
> background during regular HeartBeat messages"

The key insight is that the chunk server's report drives it. The master
answers "do you still need this?" rather than tracking "who do I still need
to tell?". A server that was down during a delete gets the instruction the
first time it heartbeats again, with **no bookkeeping kept for it while it
was away** — which is exactly the state that eager deletion gets wrong.

## What production systems did

- **HDFS** sends `DNA_INVALIDATE` commands in the DataNode heartbeat reply,
  same mechanism, same reasoning.
- **Colossus / Tectonic** keep deletion lazy for a different additional
  reason: at exabyte scale, an eager delete storm is itself a denial of
  service against the metadata layer.

## Decision

Reuse the heartbeat round trip. Each heartbeat reports the chunks the server
holds; the master diffs that against its own map and queues anything it does
not recognise, which covers two cases at once:

- the chunk belonged to a deleted file (orphan)
- the chunk exists but this server is not a recognised location for it
  (a stale replica, e.g. left behind after a partition)

Batches are capped at 64 handles per reply, because a server returning from a
long outage can be holding thousands of orphans and one reply should not
become a huge message.

### Rejected alternatives

- **Eager deletion: master pushes deletes at `DeleteFile` time.** Reclaims
  space immediately, and is wrong in the way the paper describes — a server
  that is down when the delete happens never gets told, so the master must
  keep a durable per-server retry list. The report-driven version needs no
  such state.
- **The paper's hidden-rename + multi-day grace period.** Gives an "undelete"
  window, which is genuinely valuable in production. Skipped here because it
  would make every test wait out a timer or need a clock hook, for a feature
  nothing in this project exercises. Noted as future work.
- **Reference-counting chunks.** More immediate, and introduces a class of
  bug (leaked or double-decremented counts) that the diff-based approach
  cannot have, because it recomputes from ground truth every heartbeat.

## Consequences

- Space is reclaimed within a heartbeat interval or two, not instantly. Fine;
  the alternative is worse.
- There is no undelete. A `DeleteFile` is final once the heartbeat lands.
- The master does the diff on every heartbeat from every server, which is
  O(chunks reported) per heartbeat. At this scale irrelevant; at Colossus
  scale this is precisely why metadata got sharded.

## Measured

`test_deleting_a_file_reclaims_its_chunks_from_disk` writes a file, confirms
3 replica files exist on disk, deletes it, and waits for the count to reach
zero. `test_a_returning_server_is_told_to_drop_orphaned_chunks` kills a
server, deletes the file while it is down, restarts it, and asserts it drops
the orphans it came back holding.

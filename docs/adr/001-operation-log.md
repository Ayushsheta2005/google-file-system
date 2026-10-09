# ADR-001 — Operation log and checkpoints, not a periodic snapshot

**Status:** implemented · **Code:** `oplog.py`, `master.py` (`commit`, `apply_record`, `recover`) · **Tests:** `tests/test_oplog.py`, `tests/test_recovery.py`, `tests/test_crash_recovery.py`

## The problem in our code

The master persisted by serializing *all* of its metadata to pretty-printed
JSON, called from a background loop on a 30-second timer:

```python
# master.py, before
def background_tasks(self):
    while self.running:
        ...
        if self.is_leader():
            self.save_metadata()        # json.dump(everything)
            self.sync_metadata_to_peers()
        time.sleep(30)
```

Two separate problems:

1. **Up to 30 seconds of acknowledged operations are lost on a crash.** A
   client that got `success=True` from `CreateFile` has no reason to think the
   file might not exist. That is a correctness bug, not a performance one.
2. **Cost per mutation is O(whole namespace).** Creating one file in a
   namespace of 100k files rewrites all 100k entries. Durability gets *more*
   expensive as the system gets more useful.

## What the GFS paper does

§2.6.3. The master keeps an **operation log** of namespace mutations, and:

> "...we flush the corresponding log record to disk before responding to the
> client operation"

Replay cost is bounded by **checkpoints** — a compact on-disk form of the
whole state, after which the log prefix it covers can be thrown away. Recovery
is "load the latest checkpoint, replay the log tail."

Crucially, the paper does **not** log chunk replica locations. The master asks
the chunk servers at startup and keeps the map current from heartbeats,
because the chunk servers are the only authority on what is actually on their
disks. A logged location is a lie the moment a disk dies.

## What production systems did

- **HDFS** uses exactly this shape and the names make it obvious: `EditLog`
  (the operation log) plus `FsImage` (the checkpoint), with a secondary
  NameNode whose job is to compact one into the other.
- **Colossus** moved master metadata into BigTable, so durability became
  BigTable's problem (which is itself a log-structured store). Same idea, one
  level down.
- **Tectonic** keeps metadata in a sharded key-value store (ZippyDB) for the
  same reason.

The common thread: nobody snapshots synchronously on the request path, and
nobody relies on a periodic timer for durability.

## Decision

An append-only log with records framed as:

```
u32 payload_len | u32 crc32(payload) | payload (JSON)
```

`commit()` appends and fsyncs **before** the handler returns. Checkpoints are
written every `CHECKPOINT_EVERY_OPS` mutations, atomically (temp file → fsync
→ `os.rename` → fsync the directory), after which the log is truncated.

**Records store the effect, not the request.** `DeleteDirectory` logs the
actual list of paths removed rather than `{"path": "/a", "recursive": true}`.
Replay therefore never re-runs path validation or placement logic, so recovery
cannot diverge from the original execution because a config value changed in
between.

### Rejected alternatives

- **Logging the request and re-running the handler on replay.** Compact, and
  how a state machine replication system normally does it — but it makes every
  handler a replay-correctness hazard: any nondeterminism (a timestamp, a
  random chunk handle, a placement decision that reads live free space)
  silently produces different state on recovery.
- **Logging replica locations.** Would avoid a cold window after restart where
  the master knows files exist but not where their chunks live. Rejected
  because the information is stale by definition; the paper's argument stands.
  Cost: locations are empty until the first heartbeat round, now ~5 s.
- **SQLite for metadata.** Gets durability, atomicity and crash recovery for
  free and would genuinely be the right call in a product. Rejected because
  the fsync-ordering and torn-tail handling *is* the thing worth understanding
  here, and delegating it to a library removes the part that is interesting.
- **Keeping the 30 s snapshot as well, as a belt-and-braces backup.** Rejected
  as confusing: two persistence paths that can disagree is worse than one.
  Snapshot mode survives only as `GFS_OPLOG_MODE=snapshot`, so the benchmark
  suite can measure what the log actually bought.

## Consequences

- Every namespace mutation now costs an fsync on the request path. Mitigated
  by group commit — see [ADR-002](002-group-commit.md).
- Two code paths now construct master state: the live handler and
  `apply_record`. **They can drift**, and if they do the master silently
  resurrects or loses files after a restart. This is the real risk the design
  introduces, and it is why `test_replay_reproduces_live_state_exactly` runs a
  mixed workload live, snapshots the state, rebuilds from disk alone, and
  asserts the two are identical.
- A follower cannot write log records — it never saw the individual mutations,
  only the leader's snapshot — so `persist_synced_state()` makes it checkpoint
  instead. Without this, a follower that wins an election after restarting
  would roll the namespace back.
- Replica locations are empty for one heartbeat interval after recovery.

## Measured

`tests/test_crash_recovery.py` SIGKILLs the leader with 40 acknowledged
creates outstanding and asserts all 40 return. Under the old snapshot timer
this loses everything created since the last tick.

Throughput effect of group commit vs one-fsync-per-op is benchmarked in
Phase 3.5 (`GFS_OPLOG_GROUP_COMMIT_MS=0` vs `5`).

## Bug found while doing this

`move_path()` crashed with `KeyError` when moving any directory that contained
a subdirectory: `src_dir` satisfies its own `startswith(src_dir)` prefix test,
so it landed in `items_to_move` *and* was moved explicitly, and the second
pass looked up a key it had already deleted. Fixed by excluding `src_dir` from
the prefix scan. It had never been caught because nothing tested a nested
directory move.

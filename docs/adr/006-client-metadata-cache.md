# ADR-006 — Client-side metadata cache: LRU + TTL + invalidate on contradiction

**Status:** implemented · **Code:** `metacache.py`, `client.py` · **Tests:** `tests/test_metacache.py`, `tests/test_cache_e2e.py`

## The problem in our code

Every read made at least two master round trips before touching data:
`GetFileInfo` for the chunk list, then `GetChunkLocations` per chunk. A 1 GB
file is 16 chunks, so **17 master RPCs to read data the client may already
know the location of** — and it paid the same cost again on the next read of
the same file.

That makes the master a throughput bottleneck for *reads*, which is precisely
what the GFS design is built to avoid.

## What the GFS paper does

§2.4, and it is the load-bearing argument for why a single master is viable
at all:

> "Clients never read and write file data through the master. Instead, a
> client asks the master which chunkservers it should contact. It caches this
> information for a limited time and interacts with the chunkservers directly
> for many subsequent operations."

"For a limited time" is doing real work in that sentence. The paper also
notes clients typically read several chunks in sequence, so the cache earns
its keep immediately.

## What production systems did

- **HDFS** clients cache block locations per open stream and re-fetch on
  read failure — the same two rules.
- **Tectonic** goes further: the Client Library is where tenant-specific
  behaviour lives, and clients drive operations rather than the metadata
  layer. More caching, not less.
- Neither trusts a TTL on its own; both invalidate on error.

## Decision

Two bounded LRU caches with per-entry TTL — filename → file info, chunk
handle → locations — behind one `MetadataCache` that owns the invalidation
rules, so call sites do not have to remember that deleting a file must also
drop its chunks' locations.

Three invalidation triggers, and all three are necessary:

**1. TTL (60 s).** Bounds how long a client can hold a wrong answer when
nothing goes visibly wrong.

**2. Contradiction from a chunk server.** If every cached replica for a chunk
fails, the client drops the entry and asks the master **once** before giving
up. This is the rule that makes caching safe rather than merely fast: without
it, a client keeps hammering replicas of a dead server until the TTL happens
to run out. `invalidate_chunk()` returns whether it actually dropped
something, so "the cache was wrong, retry" is distinguishable from "we never
had it cached, so this answer is already fresh" — otherwise the retry
recurses forever.

**3. The client's own mutations.** Deleting, creating, resizing, or writing
invalidates. A recursive directory delete invalidates the whole subtree by
prefix, because the client cannot enumerate what the master considered to be
under that path.

**Mutating methods invalidate on exit, not inline.** This was a real bug
found by `test_a_clients_own_write_is_visible_to_its_next_read`: `write_file`
calls `get_file_info` again on its way out, which re-cached the size the
master had *before* the write committed, so the next read truncated to the
stale length — silently, and only for the client that did the write. The
implementations are now wrapped so the entry is dropped after they return,
which holds regardless of what they do internally.

### Rejected alternatives

- **Caching with TTL alone, no invalidation.** Simpler, and wrong in exactly
  the way that matters: a stale location is not a slow read, it is a failed
  one.
- **Leases / master-driven invalidation** (NFSv4 delegations, Ceph
  capabilities). Strictly better consistency, and it requires the master to
  track every client holding every entry and push revocations — which
  reintroduces the per-client master state the paper was avoiding. Wrong
  trade for GFS's workload.
- **Caching chunk *data*, not just metadata.** The paper explicitly declines
  this (§2.4): chunks are too large and most reads stream through. Agreed.
- **An unbounded dict.** A client walking a large namespace grows it without
  limit. Bounded at 4096 file entries with LRU eviction.

## Consequences

- A client can read a file deleted by *another* client for up to one TTL. In
  GFS's consistency model that is acceptable; it would not be in a POSIX
  filesystem, and is worth saying out loud before mounting this over FUSE.
- Correctness now depends on every mutating path invalidating. One missed
  path is a silent stale-read bug, which is why `test_cache_e2e.py` tests
  write-then-read, delete-then-read, recreate-with-new-chunks, and
  recursive-delete separately rather than assuming the rule is applied
  uniformly.
- `GFS_CLIENT_CACHE=off` swaps in a `NullCache` object rather than branching
  at call sites, so the A/B benchmark differs only in caching and in nothing
  else.

## Measured

`test_a_second_read_is_served_without_touching_the_master` asserts 10 repeat
reads of a hot file cost zero master RPCs. The throughput and
RPC-reduction figures land in Phase 3.5 (`GFS_CLIENT_CACHE=off` vs `lru`).

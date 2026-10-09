# Architecture Decision Records

One file per decision. Each answers the same five questions, because those are
the questions that get asked about this system:

1. **What was wrong** — the concrete problem in *this* code, with file and line.
2. **What the GFS paper does** — the 2003 design, with a section reference.
3. **What production systems did** — Colossus, Tectonic, SeaweedFS, HDFS, and
   where they diverged from the paper and why.
4. **What we chose, and what we rejected** — alternatives are the interesting
   part; a decision with no rejected alternative was not a decision.
5. **What it cost** — every choice gives something up. If an ADR lists no cost,
   it is not finished.

| ADR | Decision |
|-----|----------|
| [001](001-operation-log.md) | Operation log + checkpoints instead of a periodic JSON snapshot |
| [002](002-group-commit.md) | Group commit: batch concurrent mutations into one fsync |
| [003](003-rereplication.md) | Re-replication by heartbeat piggyback; the target pulls |
| [004](004-garbage-collection.md) | Lazy GC driven by what chunk servers report |
| [005](005-erasure-coding.md) | Reed–Solomon RS(6,3), hand-rolled; replication stays for hot data |
| [006](006-client-metadata-cache.md) | Client metadata cache: LRU + TTL + invalidate on contradiction |
| [007](007-deterministic-simulation.md) | Deterministic simulation testing; two protocol bugs found and fixed |
| [008](008-timing-constants.md) | Heartbeat and election timeouts cut by ~6× |
| [009](009-grpc-transport.md) | Raise the gRPC message cap and share channel options |
| [010](010-thread-pool.md) | Thread pool 10 → 64 on both servers |

## Sources

- Ghemawat, Gobioff, Leung. *The Google File System*, SOSP 2003.
- [Colossus: successor to GFS](https://www.systutorials.com/colossus-successor-to-google-file-system-gfs/)
- Pan et al. [*Facebook's Tectonic Filesystem: Efficiency from Exascale*](https://www.usenix.org/system/files/fast21-pan.pdf), FAST 2021.
- [SeaweedFS erasure coding for warm storage](https://github.com/seaweedfs/seaweedfs/wiki/Tiered-Storage)
- [gRPC performance best practices](https://grpc.io/docs/guides/performance/)
- [TigerBeetle: protocol-aware deterministic simulation testing](https://tigerbeetle.com/blog/2026-08-20-protocol-aware-dst/)

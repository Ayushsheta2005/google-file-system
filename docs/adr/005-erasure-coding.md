# ADR-005 — Reed–Solomon over GF(2⁸), hand-rolled; hot stays replicated

**Status:** codec implemented (1.1); storage integration in progress (1.2–1.4) · **Code:** `reedsolomon.py` · **Tests:** `tests/test_reedsolomon.py` (42)

## The problem

RF=3 costs 3× storage, forever, for every byte. That is the single largest
inefficiency in the GFS design, and it is why Google replaced GFS.

## What the GFS paper does

Nothing — it is 3× replication throughout. §2.5 mentions erasure codes only
as an acknowledged gap. This ADR is therefore a deliberate *departure* from
the paper, following what the paper's successors actually did.

## What production systems did

- **Colossus**, GFS's replacement, moved to Reed–Solomon. Same durability at
  roughly 1.5× storage instead of 3×.
- **SeaweedFS** uses RS(10,4) — 1.4× overhead, tolerates 4 losses — and,
  crucially, applies it **in the background to warm data only**: *"If the
  volume is replicated, only one copy will be erasure encoded. All the
  original copies will be purged after a successful erasure encoding."*
  Writes never pay the encoding cost.
- **Facebook f4 / Tectonic** do the same hot-vs-warm split per tenant.

The shared lesson is not "EC is better than replication" — it is **EC is
better for cold data, replication is better for hot data**, and a real system
runs both and migrates between them.

## Decision

**RS(6,3), hot data replicated, background conversion to EC once warm.**

### Why hand-roll the codec

`pyeclib` (bindings to Jerasure/ISA-L) is faster and is what a product
should use. Rejected here because the finite-field arithmetic is the part
worth being able to explain, and a library call explains nothing. It is also
only ~200 lines once the structure is clear, and the correctness is
exhaustively testable.

The implementation uses log/antilog tables so that multiplication becomes
`antilog[log[a] + log[b]]`, which numpy applies to a whole buffer at once.
The encoding matrix is Vandermonde (every square submatrix invertible — the
property the whole scheme rests on) transformed into **systematic** form, so
the first k shards are the input verbatim.

Systematic form is the single most important performance decision here: when
nothing is lost, reading is a concatenation and the decoder is never touched.
That is the difference between 115 MB/s and ~10 GB/s on the common path.

### Why RS(6,3) and not RS(10,4)

RS(10,4) has lower overhead (1.4× vs 1.5×) and is what SeaweedFS ships. But
14 shards across a 4–6 node demo cluster puts multiple shards on the same
host, which destroys the independence the durability argument assumes — you
would be claiming 4-failure tolerance while a single host loss takes out
three shards. 9 shards is the largest that spreads meaningfully at this
scale. Configurable via `GFS_EC_DATA_SHARDS` / `GFS_EC_PARITY_SHARDS`.

### Rejected alternatives

- **`pyeclib` / Jerasure.** Faster, needs a C library, and removes the
  interesting part.
- **EC on the write path.** Every write would pay encode latency, and small
  files pay it worst. SeaweedFS's background approach avoids this entirely.
- **Replacing replication outright.** Wrong for hot data: a degraded read
  under EC needs k shards from k different hosts plus a matrix inversion,
  versus one read from one replica. Keep both.
- **XOR-only parity (RAID-5 style).** Much faster and tolerates exactly one
  loss, which is worse than 3× replication. Not a real option.

## Consequences

- **Degraded reads get much more expensive**: 6 network fetches plus a matrix
  inversion instead of a single read from one replica. Measured below.
- Repairing one lost shard still requires reading k shards — the
  well-known Reed–Solomon repair-bandwidth problem, and why research codes
  (LRC, regenerating codes) exist. Out of scope here; worth naming as the
  known limitation.
- Two storage modes mean two code paths for read, repair and placement. Real
  added complexity, and the reason this is Phase 1 rather than Phase 0.

## Measured (codec, `RS(6,3)`, this machine)

| Operation | Throughput |
|---|---|
| Encode | **115 MB/s** |
| Degraded read (3 shards lost, worst case) | **115 MB/s** |
| Intact read (systematic fast path) | **8–22 GB/s** |
| Storage overhead | **1.50×** vs 3.00× for RF=3 |
| Simultaneous losses tolerated | **3** vs 2 for RF=3 |

Note the last row: RS(6,3) survives *three* losses; 3× replication dies on
its third. So this is strictly more durable at half the storage.

## Bug found while building this

The first version paired `PRIMITIVE_POLY = 0x11D` with `GENERATOR = 3`. Under
that polynomial 3 has multiplicative order **51**, not 255 — so the log table
covered 51 of 256 values and every multiplication through it was wrong, while
still producing perfectly plausible-looking bytes. (3 is primitive under
0x11B, the AES polynomial; 2 is primitive under 0x11D. The two are easy to
conflate.)

The guard added for it was *also* initially wrong: checking "do the powers
return to 1 after 255 steps?" passes, because an element's order always
divides 255 and 51 does. The correct check is that the 255 powers are
distinct. Both the bug and the flawed guard have regression tests.

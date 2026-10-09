#!/usr/bin/env python3
"""Invariants, fault injection, the seed loop, and schedule shrinking.

An invariant is checked after *every* event, not at the end of the run. The
difference matters: a safety violation that exists for three virtual
milliseconds and then resolves is still a safety violation, and checking only
the final state misses exactly the transient bugs this exists to find.

Shrinking is the other half. A seed that fails with 400 injected faults tells
you almost nothing; the same failure reduced to two faults usually tells you
the bug outright. `shrink()` removes faults one at a time and keeps whatever
still fails.

Usage:
    python -m simulation.runner --seeds 1000
    python -m simulation.runner --seed 48213 --verbose     # replay one

See ADR-007.
"""

import argparse
import json
import random
import os
import sys
import time
from typing import Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulation.model import LEADER, SimCluster  # noqa: E402


class InvariantViolation(Exception):
    def __init__(self, name: str, detail: str, when: float):
        super().__init__(f'[t={when:.4f}] {name}: {detail}')
        self.name = name
        self.detail = detail
        self.when = when


# ==========================================================================
# Invariants
# ==========================================================================
def at_most_one_leader_per_term(cluster) -> Optional[str]:
    """Election safety.

    Two leaders in the *same* term is the classic consensus bug and means
    some node voted twice. Two leaders in *different* terms is normal and
    transient -- a deposed leader has not noticed yet -- so the check is
    deliberately per-term rather than global.
    """
    by_term: Dict[int, List[str]] = {}
    for m in cluster.masters.values():
        if m.state == LEADER:
            by_term.setdefault(m.term, []).append(m.id)
    for term, leaders in by_term.items():
        if len(leaders) > 1:
            return f'term {term} has leaders {sorted(leaders)}'
    return None


def no_two_leases_on_one_chunk(cluster) -> Optional[str]:
    """Lease exclusivity.

    Leases are what order concurrent mutations in GFS. Two live leases on one
    chunk means two primaries sequencing writes independently, and replicas
    silently diverge.
    """
    now = cluster.sim.clock.now
    holders: Dict[str, List[Tuple[str, str]]] = {}
    for m in cluster.masters.values():
        if m.state != LEADER:
            continue
        for chunk, lease in m.leases.items():
            if lease['expires'] > now:
                holders.setdefault(chunk, []).append((m.id, lease['holder']))
    for chunk, hs in holders.items():
        distinct = {h for _, h in hs}
        if len(distinct) > 1:
            return f'chunk {chunk} leased to {sorted(distinct)} simultaneously'
    return None


def committed_writes_are_never_lost(cluster) -> Optional[str]:
    """Durability.

    Mirrors the claim ADR-001 makes: once acknowledged, an operation survives
    anything. The model keeps `committed` across a crash precisely because
    the real master fsyncs before replying.
    """
    for m in cluster.masters.values():
        lost = cluster.expected_committed - m.committed
        if m.state == LEADER and m.running and lost:
            return (f'leader {m.id} is missing {len(lost)} acknowledged '
                    f'write(s), e.g. {sorted(lost)[:3]}')
    return None


def term_never_decreases(cluster) -> Optional[str]:
    """Monotonicity. A node whose term went backwards would accept stale
    leaders and re-grant votes it already cast."""
    for m in cluster.masters.values():
        prev = cluster.max_term_seen.get(m.id, 0)
        if m.term < prev:
            return f'{m.id} term went {prev} -> {m.term}'
        cluster.max_term_seen[m.id] = max(prev, m.term)
    return None


INVARIANTS: List[Tuple[str, Callable]] = [
    ('at_most_one_leader_per_term', at_most_one_leader_per_term),
    ('no_two_leases_on_one_chunk', no_two_leases_on_one_chunk),
    ('committed_writes_are_never_lost', committed_writes_are_never_lost),
    ('term_never_decreases', term_never_decreases),
]


# ==========================================================================
# Nemesis
# ==========================================================================
class Fault:
    __slots__ = ('at', 'kind', 'target', 'extra')

    def __init__(self, at, kind, target=None, extra=None):
        self.at = at
        self.kind = kind
        self.target = target
        self.extra = extra or {}

    def describe(self) -> str:
        return f't={self.at:.3f} {self.kind}' + (f' {self.target}' if self.target else '')

    def as_dict(self):
        return {'at': round(self.at, 4), 'kind': self.kind,
                'target': self.target, **self.extra}


def generate_faults(cluster, rng, duration: float, n: int) -> List[Fault]:
    """Build a random but reproducible fault schedule."""
    kinds = ['crash', 'restart', 'pause', 'resume', 'partition', 'heal']
    master_ids = sorted(cluster.masters)          # sorted: set order is not
    cs_ids = sorted(cluster.chunkservers)         # stable across runs
    faults = []
    for _ in range(n):
        at = rng.uniform(0.1, duration * 0.9)
        kind = rng.choice(kinds)
        if kind in ('crash', 'restart', 'pause', 'resume'):
            target = rng.choice(master_ids + cs_ids)
            faults.append(Fault(at, kind, target))
        elif kind == 'partition':
            # Never partition a majority away from itself -- that is a
            # liveness scenario, not a safety one, and the system is
            # *supposed* to stop making progress. Keeping the split to a
            # minority means safety violations stay meaningful.
            size = rng.randint(1, max(1, len(master_ids) // 2))
            group = rng.sample(master_ids, size)
            rest = [m for m in master_ids if m not in group]
            if rest:
                faults.append(Fault(at, 'partition', ','.join(sorted(group))))
        else:
            faults.append(Fault(at, 'heal'))
    faults.sort(key=lambda f: f.at)
    return faults


def apply_faults(cluster, faults: List[Fault]):
    for fault in faults:
        def make(f):
            def run():
                net = cluster.net
                if f.kind == 'crash':
                    net.crash(f.target)
                    if f.target in cluster.masters:
                        cluster.masters[f.target].crash()
                elif f.kind == 'restart':
                    net.restart(f.target)
                elif f.kind == 'pause':
                    net.pause(f.target)
                elif f.kind == 'resume':
                    net.resume(f.target)
                elif f.kind == 'partition':
                    group = f.target.split(',')
                    rest = [m for m in cluster.masters if m not in group]
                    if rest:
                        net.partition(group, rest)
                elif f.kind == 'heal':
                    net.heal()
            return run
        cluster.sim.at(fault.at, make(fault), fault.kind)


# ==========================================================================
# Workload
# ==========================================================================
def drive_workload(cluster, rng, duration: float, n_ops: int):
    """Clients asking the leader for leases and acknowledging writes."""
    chunks = [f'chunk{i}' for i in range(5)]
    cs_ids = sorted(cluster.chunkservers)

    for i in range(n_ops):
        at = rng.uniform(0.05, duration * 0.95)
        chunk = rng.choice(chunks)
        holder = rng.choice(cs_ids)
        op_id = f'op{i}'

        def make(c, h, oid):
            def run():
                leaders = cluster.leaders()
                if not leaders:
                    return
                leader = leaders[0]
                if leader.grant_lease(c, h, duration=0.5):
                    # An acknowledged write. From here on, losing it is a
                    # durability violation.
                    leader.committed.add(oid)
                    cluster.expected_committed.add(oid)
                    for m in cluster.masters.values():
                        if m.running:
                            m.committed.add(oid)
            return run
        cluster.sim.at(at, make(chunk, holder, op_id), 'client')


# ==========================================================================
# One run
# ==========================================================================
def run_seed(seed: int, duration: float = 30.0, n_faults: int = 12,
             n_ops: int = 40, drop_rate: float = 0.02,
             faults: Optional[List[Fault]] = None,
             verbose: bool = False) -> Optional[dict]:
    """Run one simulation. Returns None if clean, or a failure report."""
    cluster = SimCluster(seed=seed, n_masters=3, n_chunkservers=4,
                         drop_rate=drop_rate)
    cluster.expected_committed = set()
    cluster.max_term_seen = {}

    # Three independent PRNG streams, all derived from the one seed.
    #
    # Sharing a single stream looks simpler and quietly breaks shrinking:
    # passing an explicit fault list skips generate_faults, so the stream is
    # not advanced, so drive_workload draws different values and the run
    # being "reduced" is not the run that failed. Separate streams make a
    # fault subset reproduce the same workload.
    fault_rng = random.Random(seed * 2 + 1)
    workload_rng = random.Random(seed * 2 + 2)

    if faults is None:
        faults = generate_faults(cluster, fault_rng, duration, n_faults)
    apply_faults(cluster, faults)
    drive_workload(cluster, workload_rng, duration, n_ops)

    violation = {}

    def check():
        if violation:
            return
        for name, fn in INVARIANTS:
            detail = fn(cluster)
            if detail:
                violation['name'] = name
                violation['detail'] = detail
                violation['when'] = cluster.sim.clock.now
                cluster.sim.stop()
                return

    try:
        cluster.run(duration, on_step=check)
    except Exception as e:
        return {'seed': seed, 'name': 'crash', 'detail': f'{type(e).__name__}: {e}',
                'when': cluster.sim.clock.now,
                'faults': [f.as_dict() for f in faults],
                'trace': cluster.sim.dump_trace()}

    if violation:
        return {'seed': seed, **violation,
                'faults': [f.as_dict() for f in faults],
                'n_faults': len(faults),
                'trace': cluster.sim.dump_trace(80)}

    if verbose:
        print(f'seed {seed}: clean. {cluster.net.stats}')
    return None


# ==========================================================================
# Shrinking
# ==========================================================================
def shrink(seed: int, faults: List[Fault], duration: float, n_ops: int,
           drop_rate: float, max_rounds: int = 200) -> List[Fault]:
    """Remove faults one at a time, keeping any subset that still fails.

    A failure with 400 faults says almost nothing. The same failure with two
    usually names the bug. Greedy and linear, which is enough here -- the
    delta-debugging algorithms are better on much larger schedules.
    """
    current = list(faults)
    rounds = 0
    i = 0
    while i < len(current) and rounds < max_rounds:
        rounds += 1
        candidate = current[:i] + current[i + 1:]
        if run_seed(seed, duration=duration, n_ops=n_ops,
                    drop_rate=drop_rate, faults=candidate) is not None:
            current = candidate      # still fails without it; drop it
        else:
            i += 1                   # that fault was necessary; keep it
    return current


# ==========================================================================
# CLI
# ==========================================================================
def main():
    ap = argparse.ArgumentParser(description='Deterministic simulation testing')
    ap.add_argument('--seeds', type=int, default=200, help='how many seeds')
    ap.add_argument('--seed', type=int, default=None, help='replay one seed')
    ap.add_argument('--start', type=int, default=0)
    ap.add_argument('--duration', type=float, default=30.0, help='virtual seconds')
    ap.add_argument('--faults', type=int, default=12)
    ap.add_argument('--ops', type=int, default=40)
    ap.add_argument('--drop-rate', type=float, default=0.02)
    ap.add_argument('--no-shrink', action='store_true')
    ap.add_argument('--verbose', action='store_true')
    ap.add_argument('--out', default=None)
    args = ap.parse_args()

    if args.seed is not None:
        report = run_seed(args.seed, duration=args.duration,
                          n_faults=args.faults, n_ops=args.ops,
                          drop_rate=args.drop_rate, verbose=True)
        if report is None:
            print(f'seed {args.seed}: no violation')
            return 0
        print(f'\nVIOLATION on seed {args.seed}')
        print(f'  invariant: {report["name"]}')
        print(f'  detail:    {report["detail"]}')
        print(f'  at:        t={report["when"]:.4f}')
        print(f'\nfault schedule ({report.get("n_faults", 0)}):')
        for f in report['faults']:
            print(f'  {f}')
        print(f'\ntrace:\n{report["trace"]}')
        return 1

    print(f'running {args.seeds} seeds x {args.duration}s virtual, '
          f'{args.faults} faults, {args.ops} ops, drop={args.drop_rate}')
    t0 = time.time()
    failures = []
    for i in range(args.start, args.start + args.seeds):
        report = run_seed(i, duration=args.duration, n_faults=args.faults,
                          n_ops=args.ops, drop_rate=args.drop_rate)
        if report:
            print(f'\n  seed {i}: {report["name"]} -- {report["detail"]}')
            if not args.no_shrink:
                original = [Fault(f['at'], f['kind'], f.get('target'))
                            for f in report['faults']]
                minimal = shrink(i, original, args.duration, args.ops,
                                 args.drop_rate)
                report['minimal_faults'] = [f.as_dict() for f in minimal]
                print(f'    shrunk {len(original)} faults -> {len(minimal)}:')
                for f in minimal:
                    print(f'      {f.describe()}')
            failures.append(report)
        elif (i - args.start + 1) % 50 == 0:
            print(f'  {i - args.start + 1}/{args.seeds} seeds clean', flush=True)

    elapsed = time.time() - t0
    virtual = args.seeds * args.duration
    print(f'\n{args.seeds} seeds in {elapsed:.1f}s real '
          f'({virtual:.0f}s virtual, {virtual / max(elapsed, 0.001):.0f}x speedup)')
    print(f'violations: {len(failures)}')

    if args.out:
        with open(args.out, 'w') as f:
            json.dump({'seeds': args.seeds, 'duration': args.duration,
                       'failures': failures}, f, indent=2)
        print(f'-> {args.out}')
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())

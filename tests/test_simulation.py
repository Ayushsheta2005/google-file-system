#!/usr/bin/env python3
"""Deterministic simulation tests.

Fast by construction -- no processes, no sockets, no sleeping -- so a few
hundred fault schedules run in about a second and belong in the ordinary
test suite rather than behind the `slow` marker.

Two things are being tested here, and they are different:

  * that the *simulator* is sound -- reproducible, honest about time, and
    actually capable of detecting a violation it is shown;
  * that the *protocol* holds under fault injection.

The first matters because a simulator that silently stops injecting faults
reports a green run forever. Several tests below exist only to pin that down.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulation import model  # noqa: E402
from simulation.core import Network, Simulator  # noqa: E402
from simulation.model import LEADER, SimCluster  # noqa: E402
from simulation.runner import (  # noqa: E402
    INVARIANTS,
    Fault,
    at_most_one_leader_per_term,
    no_two_leases_on_one_chunk,
    run_seed,
    shrink,
)


@pytest.fixture(autouse=True)
def fixes_enabled():
    """Both protocol fixes on by default; individual tests flip them off."""
    model.SimMaster.CATCH_UP_ON_PROMOTION = True
    model.SimMaster.ENFORCE_LEADER_LEASE = True
    yield
    model.SimMaster.CATCH_UP_ON_PROMOTION = True
    model.SimMaster.ENFORCE_LEADER_LEASE = True


# ==================================================================
# Is the simulator itself trustworthy?
# ==================================================================
def test_the_same_seed_produces_an_identical_run():
    """The entire premise. If this fails, nothing else here means anything
    and a reported bug cannot be replayed."""
    def fingerprint(seed):
        c = SimCluster(seed=seed, n_masters=3, n_chunkservers=3)
        c.expected_committed = set()
        c.max_term_seen = {}
        c.run(10.0)
        return [(round(t, 6), m) for t, m in c.sim.trace]

    assert fingerprint(12345) == fingerprint(12345)


def test_different_seeds_produce_different_runs():
    """Otherwise the seed loop explores one schedule a thousand times."""
    def fingerprint(seed):
        c = SimCluster(seed=seed, n_masters=3, n_chunkservers=3)
        c.expected_committed = set()
        c.max_term_seen = {}
        c.run(10.0)
        return [m for _, m in c.sim.trace]

    assert fingerprint(1) != fingerprint(2)


def test_virtual_time_does_not_consume_real_time():
    """An hour of cluster life has to cost milliseconds, or exploring
    thousands of schedules is impossible."""
    import time as real_time
    t0 = real_time.time()
    c = SimCluster(seed=7, n_masters=3, n_chunkservers=3)
    c.expected_committed = set()
    c.max_term_seen = {}
    c.run(3600.0)                     # one virtual hour
    elapsed = real_time.time() - t0
    assert c.sim.clock.now >= 3600.0
    assert elapsed < 20.0, f'one virtual hour took {elapsed:.1f}s of real time'


def test_time_never_runs_backwards():
    sim = Simulator(seed=1)
    seen = []
    for t in (5.0, 1.0, 3.0, 2.0):
        sim.at(t, lambda tt=t: seen.append(sim.clock.now))
    sim.run_until(10.0)
    assert seen == sorted(seen)


def test_events_at_the_same_instant_fire_in_scheduling_order():
    """Ties must break on a sequence number. Breaking on object identity
    would vary between runs and quietly destroy determinism."""
    sim = Simulator(seed=1)
    order = []
    for i in range(10):
        sim.at(1.0, lambda i=i: order.append(i))
    sim.run_until(2.0)
    assert order == list(range(10))


def test_a_partition_actually_blocks_messages():
    sim = Simulator(seed=1)
    net = Network(sim)
    got = []
    net.register('a', lambda s, m: got.append(('a', m)))
    net.register('b', lambda s, m: got.append(('b', m)))

    net.send('a', 'b', 'before')
    sim.run_until(1.0)
    net.partition(['a'], ['b'])
    net.send('a', 'b', 'during')
    sim.run_until(2.0)
    net.heal()
    net.send('a', 'b', 'after')
    sim.run_until(3.0)

    assert [m for _, m in got] == ['before', 'after']


def test_a_message_in_flight_to_a_node_that_crashes_is_not_delivered():
    """Checking reachability only at send time is a classic simulator bug
    that hides real partition behaviour."""
    sim = Simulator(seed=1)
    net = Network(sim, min_latency=0.5, max_latency=0.5)
    got = []
    net.register('b', lambda s, m: got.append(m))

    net.send('a', 'b', 'doomed')
    sim.run_until(0.1)
    net.crash('b')
    sim.run_until(2.0)
    assert got == []


def test_crashed_nodes_send_nothing():
    sim = Simulator(seed=1)
    net = Network(sim)
    got = []
    net.register('b', lambda s, m: got.append(m))
    net.crash('a')
    net.send('a', 'b', 'should not arrive')
    sim.run_until(1.0)
    assert got == []


def test_drop_rate_is_honoured():
    sim = Simulator(seed=42)
    net = Network(sim, drop_rate=0.5)
    net.register('b', lambda s, m: None)
    for _ in range(400):
        net.send('a', 'b', 'x')
    sim.run_until(10.0)
    assert 120 < net.stats['dropped'] < 280, net.stats


# ==================================================================
# Can the invariants detect a violation they are shown?
# ==================================================================
def test_the_leader_invariant_catches_two_leaders_in_one_term():
    """An invariant that cannot fail is not a test."""
    c = SimCluster(seed=1, n_masters=3, n_chunkservers=1)
    for m in list(c.masters.values())[:2]:
        m.state = LEADER
        m.term = 5
    assert at_most_one_leader_per_term(c) is not None


def test_the_leader_invariant_permits_leaders_in_different_terms():
    """A deposed leader that has not noticed yet is normal and transient."""
    c = SimCluster(seed=1, n_masters=3, n_chunkservers=1)
    ms = list(c.masters.values())
    ms[0].state, ms[0].term = LEADER, 4
    ms[1].state, ms[1].term = LEADER, 5
    assert at_most_one_leader_per_term(c) is None


def test_the_lease_invariant_catches_two_primaries():
    c = SimCluster(seed=1, n_masters=2, n_chunkservers=2)
    ms = list(c.masters.values())
    for i, m in enumerate(ms):
        m.state = LEADER
        m.leases['chunkX'] = {'holder': f'cs{i}', 'expires': 1e9, 'term': 1}
    assert no_two_leases_on_one_chunk(c) is not None


def test_the_lease_invariant_ignores_expired_leases():
    c = SimCluster(seed=1, n_masters=2, n_chunkservers=2)
    ms = list(c.masters.values())
    ms[0].state = LEADER
    ms[0].leases['chunkX'] = {'holder': 'cs0', 'expires': -1, 'term': 1}
    ms[1].state = LEADER
    ms[1].leases['chunkX'] = {'holder': 'cs1', 'expires': 1e9, 'term': 2}
    assert no_two_leases_on_one_chunk(c) is None


# ==================================================================
# The protocol under fault injection
# ==================================================================
SEED_CORPUS = list(range(120))


def test_election_safety_holds_across_the_seed_corpus():
    """No two leaders in one term, over 120 fault schedules.

    This one has never failed, before or after the fixes -- the election
    logic was already sound. Worth keeping precisely so a future change that
    breaks it is caught.
    """
    for seed in SEED_CORPUS:
        report = run_seed(seed, duration=20.0, n_faults=12, n_ops=40,
                          drop_rate=0.02)
        if report and report['name'] == 'at_most_one_leader_per_term':
            pytest.fail(f'seed {seed}: {report["detail"]}\n{report["trace"]}')


def test_lease_exclusivity_holds_with_the_leader_lease_guard():
    """Found by the simulator: a partitioned leader kept granting leases
    while the majority side elected a replacement doing the same, giving two
    primaries on one chunk. See ADR-007."""
    for seed in SEED_CORPUS:
        report = run_seed(seed, duration=20.0, n_faults=12, n_ops=40,
                          drop_rate=0.02)
        if report and report['name'] == 'no_two_leases_on_one_chunk':
            pytest.fail(f'seed {seed}: {report["detail"]}\n{report["trace"]}')


def test_removing_the_leader_lease_guard_reintroduces_the_bug():
    """Guards the fix. If this ever stops failing, either the bug came back
    by another route or the simulator stopped injecting partitions -- both
    worth knowing, and neither visible from a green suite."""
    model.SimMaster.ENFORCE_LEADER_LEASE = False
    found = None
    for seed in range(400):
        report = run_seed(seed, duration=20.0, n_faults=12, n_ops=40,
                          drop_rate=0.02)
        if report and report['name'] == 'no_two_leases_on_one_chunk':
            found = (seed, report['detail'])
            break
    assert found, 'expected the unguarded protocol to violate lease exclusivity'


def test_catch_up_on_promotion_reduces_durability_violations():
    """Quantifies the fix rather than asserting it is perfect, because it is
    not: catch-up narrows the window, and only majority-ack-before-reply
    closes it. See ADR-007."""
    def count(catch_up):
        model.SimMaster.CATCH_UP_ON_PROMOTION = catch_up
        return sum(
            1 for s in range(200)
            if (r := run_seed(s, duration=20.0, n_faults=12, n_ops=40,
                              drop_rate=0.02))
            and r['name'] == 'committed_writes_are_never_lost')

    without = count(False)
    with_fix = count(True)
    assert without > 0, 'baseline should lose writes; simulator may be inert'
    assert with_fix < without / 2, f'{without} -> {with_fix}, expected a big drop'


def test_shrinking_finds_a_smaller_reproducer():
    """A failure with 40 faults says little; the same failure with two
    usually names the bug."""
    model.SimMaster.CATCH_UP_ON_PROMOTION = False
    seed = next(
        s for s in range(200)
        if (r := run_seed(s, duration=20.0, n_faults=40, n_ops=40,
                          drop_rate=0.02)) and r['name'].startswith('committed'))
    report = run_seed(seed, duration=20.0, n_faults=40, n_ops=40,
                      drop_rate=0.02)
    original = [Fault(f['at'], f['kind'], f.get('target'))
                for f in report['faults']]
    minimal = shrink(seed, original, 20.0, 40, 0.02)

    assert len(minimal) < len(original)
    assert run_seed(seed, duration=20.0, n_ops=40, drop_rate=0.02,
                    faults=minimal) is not None, 'shrunk schedule stopped failing'


def test_a_clean_run_really_exercised_the_network():
    """A green result from a simulator that delivered nothing is worthless."""
    c = SimCluster(seed=3, n_masters=3, n_chunkservers=4, drop_rate=0.02)
    c.expected_committed = set()
    c.max_term_seen = {}
    c.run(20.0)
    assert c.net.stats['delivered'] > 500, c.net.stats
    assert c.net.stats['dropped'] > 0, c.net.stats


def test_a_leader_is_elected_without_faults():
    c = SimCluster(seed=11, n_masters=3, n_chunkservers=3, drop_rate=0.0)
    c.expected_committed = set()
    c.max_term_seen = {}
    c.run(10.0)
    assert len(c.leaders()) == 1


def test_a_single_master_cluster_elects_itself():
    """Regression for a real bug: the majority check ran only when a peer's
    vote came back, so a lone master campaigned forever."""
    c = SimCluster(seed=5, n_masters=1, n_chunkservers=2, drop_rate=0.0)
    c.expected_committed = set()
    c.max_term_seen = {}
    c.run(10.0)
    assert len(c.leaders()) == 1

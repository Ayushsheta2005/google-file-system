#!/usr/bin/env python3
"""Unit tests for the erasure-coding planner.

The dangerous decisions in EC are not the maths -- that is covered in
test_reedsolomon.py -- but the *ordering*: when replicas may be dropped,
whether shards land on distinct hosts, and what happens when a conversion
dies halfway. Those are what this file is about.
"""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ec_manager import ECChunkState, ECManager  # noqa: E402


class FakeChunk:
    def __init__(self, handle, locations, size=1000, version=1, last_modified=0.0):
        self.handle = handle
        self.locations = set(locations)
        self.size = size
        self.version = version
        self.last_modified = last_modified


class FakeServer:
    def __init__(self, sid, space=1000, port=9000):
        self.server_id = sid
        self.available_space = space
        self.address = '127.0.0.1'
        self.port = port
        self.last_heartbeat = time.time()


def servers(n, spaces=None):
    return {f's{i}': FakeServer(f's{i}', (spaces or {}).get(f's{i}', 1000 + i),
                                port=9000 + i) for i in range(n)}


def mgr(**kw):
    kw.setdefault('data_shards', 6)
    kw.setdefault('parity_shards', 3)
    kw.setdefault('warm_after', 100.0)
    return ECManager(**kw)


# ------------------------------------------------------------- eligibility
def test_a_hot_chunk_is_not_converted():
    """Writes must never pay encoding cost -- that is the whole point of the
    hot/warm split."""
    m = mgr()
    now = time.time()
    chunks = {'c1': FakeChunk('c1', {'s0', 's1', 's2'}, last_modified=now)}
    assert m.eligible_for_encoding(chunks, {'s0', 's1', 's2'}, now=now) == []


def test_a_cold_chunk_is_eligible():
    m = mgr()
    now = time.time()
    chunks = {'c1': FakeChunk('c1', {'s0', 's1', 's2'}, last_modified=now - 500)}
    assert m.eligible_for_encoding(chunks, {'s0', 's1', 's2'}, now=now) == ['c1']


def test_an_under_replicated_chunk_is_not_converted():
    """Repair it the cheap way first -- copying one replica beats reading
    the whole chunk and writing k+m pieces."""
    m = mgr()
    now = time.time()
    chunks = {'c1': FakeChunk('c1', {'s0', 's1'}, last_modified=now - 500)}
    assert m.eligible_for_encoding(chunks, {'s0', 's1'}, now=now) == []


def test_an_empty_chunk_is_not_converted():
    m = mgr()
    now = time.time()
    chunks = {'c1': FakeChunk('c1', {'s0', 's1', 's2'}, size=0,
                              last_modified=now - 500)}
    assert m.eligible_for_encoding(chunks, {'s0', 's1', 's2'}, now=now) == []


def test_an_already_encoded_chunk_is_not_converted_again():
    m = mgr()
    now = time.time()
    chunks = {'c1': FakeChunk('c1', {'s0', 's1', 's2'}, last_modified=now - 500)}
    for i in range(9):
        m.note_shard_present('c1', i, f's{i}', 6, 3, 1000)
    assert m.eligible_for_encoding(chunks, {'s0', 's1', 's2'}, now=now) == []


# --------------------------------------------------------------- placement
def test_shards_go_to_distinct_servers():
    """Two shards on one host would mean advertising 3-failure tolerance
    while a single host loss takes out two shards."""
    m = mgr()
    srv = servers(9)
    targets = m.choose_shard_targets(srv, set(srv))
    assert targets is not None
    assert len(targets) == len(set(targets)) == 9


def test_conversion_is_refused_without_enough_distinct_servers():
    """Better to stay replicated than to claim durability we do not have."""
    m = mgr()
    srv = servers(5)
    assert m.choose_shard_targets(srv, set(srv)) is None


def test_targets_prefer_the_emptiest_servers():
    m = ECManager(data_shards=2, parity_shards=1, warm_after=0)
    srv = servers(5, spaces={'s0': 10, 's1': 20, 's2': 900, 's3': 800, 's4': 700})
    assert set(m.choose_shard_targets(srv, set(srv))) == {'s2', 's3', 's4'}


def test_encode_is_throttled():
    m = mgr(max_encodes_in_flight=2)
    srv = servers(9)
    now = time.time()
    chunks = {f'c{i}': FakeChunk(f'c{i}', {'s0', 's1', 's2'}, last_modified=now - 500)
              for i in range(10)}
    assert m.plan_encodes(chunks, srv, set(srv)) == 2


# ----------------------------------------------------- the ordering rule
def test_replicas_are_not_droppable_until_every_shard_is_confirmed():
    """THE safety rule. Dropping replicas on a partial shard set is exactly
    how an EC migration loses data."""
    m = mgr()
    srv = servers(9)
    now = time.time()
    chunks = {'c1': FakeChunk('c1', {'s0', 's1', 's2'}, last_modified=now - 500)}
    m.plan_encodes(chunks, srv, set(srv))

    for i in range(8):                       # one short of the full set
        m.note_shard_present('c1', i, f's{i}', 6, 3, 1000)
        assert m.take_replicas_to_drop() == [], f'droppable after only {i+1} shards'

    m.note_shard_present('c1', 8, 's8', 6, 3, 1000)
    assert m.take_replicas_to_drop() == ['c1']


def test_replicas_to_drop_is_consumed_once():
    m = mgr()
    for i in range(9):
        m.note_shard_present('c1', i, f's{i}', 6, 3, 1000)
    m._replicas_to_drop.add('c1')
    assert m.take_replicas_to_drop() == ['c1']
    assert m.take_replicas_to_drop() == []


def test_a_timed_out_encode_leaves_replicas_intact():
    """A conversion that dies halfway must degrade to wasted work, never to
    data loss."""
    m = mgr(encode_timeout=0.05)
    srv = servers(9)
    now = time.time()
    chunks = {'c1': FakeChunk('c1', {'s0', 's1', 's2'}, last_modified=now - 500)}
    assert m.plan_encodes(chunks, srv, set(srv)) == 1

    m.note_shard_present('c1', 0, 's0', 6, 3, 1000)   # partial
    time.sleep(0.1)
    m._expire()

    assert m.take_replicas_to_drop() == []
    assert m.stats['encodes_timed_out'] == 1


# -------------------------------------------------------------- readability
def test_an_ec_chunk_is_readable_with_exactly_k_shards():
    st = ECChunkState('c1', 6, 3, 1000)
    for i in range(9):
        st.shards[i] = f's{i}'
    assert st.readable({f's{i}' for i in range(6)})
    assert not st.readable({f's{i}' for i in range(5)})


def test_ec_tolerates_three_losses_where_replication_tolerates_two():
    """The headline durability claim, at the planner level."""
    st = ECChunkState('c1', 6, 3, 1000)
    for i in range(9):
        st.shards[i] = f's{i}'
    live = {f's{i}' for i in range(9)} - {'s0', 's4', 's8'}
    assert st.readable(live)


# ------------------------------------------------------------------ repair
def test_a_missing_shard_is_detected():
    m = mgr()
    for i in range(9):
        m.note_shard_present('c1', i, f's{i}', 6, 3, 1000)
    live = {f's{i}' for i in range(9)} - {'s3'}
    degraded = m.degraded(live)
    assert degraded and degraded[0][0] == 'c1'
    assert 3 in degraded[0][1]


def test_repair_targets_a_server_not_already_holding_a_shard():
    m = mgr()
    srv = servers(10)
    for i in range(9):
        m.note_shard_present('c1', i, f's{i}', 6, 3, 1000)
    live = set(srv) - {'s3'}
    assert m.plan_repairs(srv, live) == 1

    tasks = m.repair_tasks_for('s9', srv)
    assert len(tasks) == 1
    assert tasks[0]['shard_index'] == 3
    assert len(tasks[0]['sources']) >= 6


def test_unreadable_chunk_is_not_queued_for_repair():
    """Fewer than k shards means there is nothing to reconstruct from;
    queueing it would occupy a slot forever."""
    m = mgr()
    for i in range(9):
        m.note_shard_present('c1', i, f's{i}', 6, 3, 1000)
    live = {'s0', 's1'}                       # only 2 of 6 needed
    srv = servers(9)
    assert m.plan_repairs(srv, live) == 0


def test_repair_completion_frees_the_slot():
    m = mgr()
    srv = servers(10)
    for i in range(9):
        m.note_shard_present('c1', i, f's{i}', 6, 3, 1000)
    m.plan_repairs(srv, set(srv) - {'s3'})
    m.note_repair_done('c1', 3, 's9')
    assert m.stats['repairs_completed'] == 1


# ------------------------------------------------------------- the headline
def test_overhead_falls_from_three_to_one_point_five():
    """The number the whole feature exists to move."""
    m = mgr()
    chunks = {f'c{i}': FakeChunk(f'c{i}', {'s0'}, size=1_000_000) for i in range(4)}

    logical, physical = m.overhead_for(chunks)
    assert physical / logical == pytest.approx(3.0)

    for handle in chunks:
        for i in range(9):
            m.note_shard_present(handle, i, f's{i}', 6, 3, 1_000_000)

    logical, physical = m.overhead_for(chunks)
    assert physical / logical == pytest.approx(1.5)


def test_mixed_hot_and_warm_gives_a_blended_overhead():
    m = mgr()
    chunks = {'hot': FakeChunk('hot', {'s0'}, size=1_000_000),
              'warm': FakeChunk('warm', {'s0'}, size=1_000_000)}
    for i in range(9):
        m.note_shard_present('warm', i, f's{i}', 6, 3, 1_000_000)
    logical, physical = m.overhead_for(chunks)
    assert physical / logical == pytest.approx(2.25)      # (3.0 + 1.5) / 2


def test_snapshot_reports_degraded_chunks():
    m = mgr()
    for i in range(9):
        m.note_shard_present('c1', i, f's{i}', 6, 3, 1000)
    snap = m.snapshot(live_servers={f's{i}' for i in range(9)})
    assert snap['ec_chunks'] == 1 and snap['degraded'] == 0

    snap = m.snapshot(live_servers={f's{i}' for i in range(8)})
    assert snap['degraded'] == 1

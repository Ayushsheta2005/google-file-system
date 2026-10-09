#!/usr/bin/env python3
"""Unit tests for the replication planner.

ReplicationManager holds no gRPC objects and no master reference on purpose,
so it can be driven with plain fakes here and by the simulator in Phase 2.
"""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from replication import ReplicationManager  # noqa: E402


class FakeChunk:
    def __init__(self, handle, locations, version=1, size=0):
        self.handle = handle
        self.locations = set(locations)
        self.version = version
        self.size = size


class FakeServer:
    def __init__(self, server_id, space=1000, address='127.0.0.1', port=9001):
        self.server_id = server_id
        self.available_space = space
        self.address = address
        self.port = port
        self.last_heartbeat = time.time()


def servers(*specs):
    return {sid: FakeServer(sid, space, port=9000 + i)
            for i, (sid, space) in enumerate(specs)}


# ------------------------------------------------------------- detection
def test_fully_replicated_chunk_is_not_queued():
    m = ReplicationManager(replication_factor=3)
    chunks = {'c1': FakeChunk('c1', {'s1', 's2', 's3'})}
    assert m.under_replicated(chunks, {'s1', 's2', 's3'}) == []


def test_chunk_missing_a_replica_is_detected():
    m = ReplicationManager(replication_factor=3)
    chunks = {'c1': FakeChunk('c1', {'s1', 's2'})}
    out = m.under_replicated(chunks, {'s1', 's2', 's3'})
    assert [h for h, _, _ in out] == ['c1']


def test_replica_on_a_dead_server_does_not_count():
    """The bug this whole module exists to fix: a location in the map is not
    the same as a replica that exists."""
    m = ReplicationManager(replication_factor=3)
    chunks = {'c1': FakeChunk('c1', {'s1', 's2', 's3'})}
    out = m.under_replicated(chunks, {'s1', 's2'})      # s3 is down
    assert [h for h, n, _ in out] == ['c1']
    assert out[0][1] == 2


def test_most_urgent_chunk_is_queued_first():
    """A chunk on its last replica is one failure from gone forever; a chunk
    at 2 of 3 is not. Ordering here is the difference between losing data
    and not."""
    m = ReplicationManager(replication_factor=3)
    chunks = {
        'two_left': FakeChunk('two_left', {'s1', 's2'}),
        'one_left': FakeChunk('one_left', {'s1'}),
    }
    out = m.under_replicated(chunks, {'s1', 's2', 's3'})
    assert [h for h, _, _ in out] == ['one_left', 'two_left']


def test_chunk_with_no_live_replica_is_not_queued():
    """Nothing to copy from. Queueing it would occupy a slot forever."""
    m = ReplicationManager(replication_factor=3)
    chunks = {'lost': FakeChunk('lost', {'s9'})}
    assert m.under_replicated(chunks, {'s1', 's2'}) == []


# -------------------------------------------------------------- placement
def test_target_is_never_a_server_that_already_holds_it():
    m = ReplicationManager(replication_factor=3)
    srv = servers(('s1', 100), ('s2', 100), ('s3', 999))
    target = m.choose_target('c1', {'s1', 's3'}, srv, {'s1', 's2', 's3'})
    assert target == 's2'


def test_target_prefers_the_emptiest_server():
    m = ReplicationManager(replication_factor=3)
    srv = servers(('s1', 10), ('s2', 50), ('s3', 900))
    assert m.choose_target('c1', set(), srv, {'s1', 's2', 's3'}) == 's3'


def test_no_target_when_every_server_already_has_it():
    m = ReplicationManager(replication_factor=3)
    srv = servers(('s1', 10), ('s2', 50))
    assert m.choose_target('c1', {'s1', 's2'}, srv, {'s1', 's2'}) is None


# -------------------------------------------------------------- throttling
def test_per_server_clone_limit_is_respected():
    """Without this, losing one node points every missing replica at
    whichever server has the most free space and saturates it."""
    m = ReplicationManager(replication_factor=3, max_in_flight_per_server=2,
                           max_in_flight_cluster=99)
    srv = servers(('s1', 100), ('s2', 100), ('s3', 500))
    chunks = {f'c{i}': FakeChunk(f'c{i}', {'s1', 's2'}) for i in range(10)}

    m.plan(chunks, srv, {'s1', 's2', 's3'})
    snap = m.snapshot()
    assert sum(1 for t in snap['in_flight'].values() if t == 's3') <= 2


def test_cluster_wide_clone_limit_is_respected():
    m = ReplicationManager(replication_factor=3, max_in_flight_per_server=99,
                           max_in_flight_cluster=4)
    srv = servers(*[(f's{i}', 100) for i in range(8)])
    chunks = {f'c{i}': FakeChunk(f'c{i}', {'s0'}) for i in range(20)}

    m.plan(chunks, srv, set(srv))
    assert len(m.snapshot()['in_flight']) <= 4


def test_planning_twice_does_not_double_queue():
    m = ReplicationManager(replication_factor=3)
    srv = servers(('s1', 100), ('s2', 100), ('s3', 100))
    chunks = {'c1': FakeChunk('c1', {'s1', 's2'})}

    first = m.plan(chunks, srv, {'s1', 's2', 's3'})
    second = m.plan(chunks, srv, {'s1', 's2', 's3'})
    assert (first, second) == (1, 0)


# ------------------------------------------------------------- completion
def test_heartbeat_reporting_the_chunk_completes_the_clone():
    m = ReplicationManager(replication_factor=3)
    srv = servers(('s1', 100), ('s2', 100), ('s3', 100))
    chunks = {'c1': FakeChunk('c1', {'s1', 's2'})}
    m.plan(chunks, srv, {'s1', 's2', 's3'})

    target = list(m.snapshot()['in_flight'].values())[0]
    m.note_replica_present('c1', target)

    assert m.snapshot()['in_flight'] == {}
    assert m.stats['clones_completed'] == 1


def test_completion_from_the_wrong_server_is_ignored():
    m = ReplicationManager(replication_factor=3)
    srv = servers(('s1', 100), ('s2', 100), ('s3', 100))
    chunks = {'c1': FakeChunk('c1', {'s1', 's2'})}
    m.plan(chunks, srv, {'s1', 's2', 's3'})

    m.note_replica_present('c1', 's1')     # already had it; not the target
    assert len(m.snapshot()['in_flight']) == 1


def test_a_stalled_clone_is_requeued_after_its_timeout():
    """A target that dies mid-clone must not hold that chunk's slot forever,
    or the queue silently stops making progress while still looking busy."""
    m = ReplicationManager(replication_factor=3, clone_timeout=0.05)
    srv = servers(('s1', 100), ('s2', 100), ('s3', 100))
    chunks = {'c1': FakeChunk('c1', {'s1', 's2'})}

    assert m.plan(chunks, srv, {'s1', 's2', 's3'}) == 1
    time.sleep(0.1)
    assert m.plan(chunks, srv, {'s1', 's2', 's3'}) == 1
    assert m.stats['clones_timed_out'] == 1


# --------------------------------------------------------------- task I/O
def test_task_carries_sources_and_version_but_not_the_target_itself():
    m = ReplicationManager(replication_factor=3)
    srv = servers(('s1', 100), ('s2', 100), ('s3', 500))
    chunks = {'c1': FakeChunk('c1', {'s1', 's2'}, version=4)}
    m.plan(chunks, srv, {'s1', 's2', 's3'})

    tasks = m.tasks_for('s3', chunks, srv)
    assert len(tasks) == 1
    assert tasks[0]['chunk_version'] == 4
    assert {s['server_id'] for s in tasks[0]['sources']} == {'s1', 's2'}
    assert tasks[0]['target']['server_id'] == 's3'


def test_servers_only_get_their_own_tasks():
    m = ReplicationManager(replication_factor=3)
    srv = servers(('s1', 100), ('s2', 100), ('s3', 500))
    chunks = {'c1': FakeChunk('c1', {'s1', 's2'})}
    m.plan(chunks, srv, {'s1', 's2', 's3'})

    assert m.tasks_for('s1', chunks, srv) == []
    assert len(m.tasks_for('s3', chunks, srv)) == 1


# -------------------------------------------------------------- deletions
def test_chunks_the_master_forgot_are_queued_for_deletion():
    """Deleting a file drops it from the master's map; the bytes stay on
    disk until something tells the chunk server. Nothing did, before."""
    m = ReplicationManager()
    chunks = {'alive': FakeChunk('alive', {'s1'})}
    queued = m.plan_deletions({'s1': {'alive', 'orphan1', 'orphan2'}}, chunks)
    assert queued == 2
    assert set(m.deletions_for('s1')) == {'orphan1', 'orphan2'}


def test_live_chunks_are_never_queued_for_deletion():
    m = ReplicationManager()
    chunks = {'c1': FakeChunk('c1', {'s1'}), 'c2': FakeChunk('c2', {'s1'})}
    assert m.plan_deletions({'s1': {'c1', 'c2'}}, chunks) == 0
    assert m.deletions_for('s1') == []


def test_deletions_are_dequeued_once_handed_out():
    m = ReplicationManager()
    m.plan_deletions({'s1': {'o1', 'o2'}}, {})
    first = m.deletions_for('s1')
    assert len(first) == 2
    assert m.deletions_for('s1') == []


def test_deletion_batches_are_capped():
    """A server returning from a long outage can be holding thousands of
    orphans; sending them all in one heartbeat reply is a huge message."""
    m = ReplicationManager()
    m.plan_deletions({'s1': {f'o{i}' for i in range(200)}}, {})
    assert len(m.deletions_for('s1', limit=64)) == 64


def test_a_server_that_was_down_during_the_delete_still_gets_told():
    """No per-server bookkeeping is kept while a server is away -- the
    instruction is derived from what it reports when it comes back."""
    m = ReplicationManager()
    m.plan_deletions({'s_online': {'orphan'}}, {})
    m.deletions_for('s_online')

    m.plan_deletions({'s_returning': {'orphan'}}, {})
    assert m.deletions_for('s_returning') == ['orphan']

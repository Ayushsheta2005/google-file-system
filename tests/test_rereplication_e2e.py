#!/usr/bin/env python3
"""End-to-end re-replication and garbage collection against a real cluster.

The unit tests prove the planner makes the right decisions. These prove the
decisions reach a chunk server and move bytes -- the part that was entirely
missing before, since `ReplicationTask` and `chunks_to_delete` lived in the
.proto and were referenced by zero lines of Python.

Assertions read the master's own replication counters via GetClusterStatus
rather than counting files on disk, so a failure reports *what* the master
believed, not just that a number was wrong.
"""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cluster import LocalCluster  # noqa: E402

pytestmark = pytest.mark.slow


def chunk_files(proc):
    try:
        return {n for n in os.listdir(proc.data_dir)
                if n.startswith('chunk_') and not n.endswith('.tmp')}
    except OSError:
        return set()


@pytest.fixture
def cluster():
    # Four chunk servers: with RF=3, killing one still leaves a valid target
    # for the lost replica. With exactly three there is nowhere to put it and
    # the test would measure nothing.
    c = LocalCluster(masters=3, chunkservers=4, log_level='WARNING')
    c.start()
    yield c
    c.destroy()


def write_file(cluster, path, payload):
    client = cluster.client()
    assert client.create_file(path), f'create {path} failed'
    assert client.write_file(path, payload, 0), f'write {path} failed'
    return client


def test_a_new_chunk_reaches_full_replication(cluster):
    """Baseline, so the recovery test below measures recovery rather than
    initial placement."""
    write_file(cluster, '/rep/a.bin', b'x' * 5000)

    elapsed = cluster.await_condition(
        lambda s: s['total_chunks'] >= 1 and s['under_replicated'] == 0,
        timeout=60,
        describe=lambda s: f"under_replicated={s.get('under_replicated')}")
    print(f'initial replication to RF=3: {elapsed:.1f}s')


def test_replicas_are_restored_after_a_chunkserver_dies(cluster):
    """The claim that was previously false.

    Before this work the master logged 'chunk is under-replicated' on a timer
    and did nothing else, so a dead node meant permanently degraded
    durability.
    """
    write_file(cluster, '/rep/b.bin', b'y' * 8000)
    cluster.await_condition(lambda s: s['under_replicated'] == 0, timeout=60)

    victim = next(p for p in cluster.chunkservers if chunk_files(p))
    victim.kill()

    # First the loss has to be noticed...
    cluster.await_condition(
        lambda s: s['live_chunkservers'] == 3, timeout=60,
        describe=lambda s: f"live={s.get('live_chunkservers')}")

    # ...then repaired, with no operator involvement.
    elapsed = cluster.await_condition(
        lambda s: s['under_replicated'] == 0 and s['unavailable'] == 0,
        timeout=120,
        describe=lambda s: (f"under_replicated={s.get('under_replicated')} "
                            f"clones_in_flight={s.get('clones_in_flight')}"))
    print(f'detect + re-replicate after node loss: {elapsed:.1f}s')


def test_data_stays_readable_throughout_a_chunkserver_failure(cluster):
    """Re-replication is worthless if reads break while it runs."""
    payload = b'readable-through-failure-' * 400
    write_file(cluster, '/rep/c.bin', payload)
    cluster.await_condition(lambda s: s['under_replicated'] == 0, timeout=60)

    victim = next(p for p in cluster.chunkservers if chunk_files(p))
    victim.kill()

    deadline = time.time() + 45
    while time.time() < deadline:
        if cluster.client().read_file('/rep/c.bin') == payload:
            return
        time.sleep(1.0)
    pytest.fail(f'unreadable after losing one replica; status={cluster.status()}')


def test_no_chunk_is_left_unavailable_after_a_single_failure(cluster):
    """With RF=3 and one node lost, nothing should be down to zero copies.
    If this trips, placement is putting replicas on too few distinct hosts."""
    for i in range(4):
        write_file(cluster, f'/rep/multi{i}.bin', bytes([i]) * 3000)
    cluster.await_condition(lambda s: s['under_replicated'] == 0, timeout=90)

    next(p for p in cluster.chunkservers if chunk_files(p)).kill()
    time.sleep(cluster.status().get('live_chunkservers', 0) and 20 or 20)

    assert cluster.status().get('unavailable', 0) == 0


def test_deleting_a_file_reclaims_its_chunks_from_disk(cluster):
    """Garbage collection. `chunks_to_delete` was dead proto, so deleted
    files occupied chunk server disk permanently."""
    client = write_file(cluster, '/rep/trash.bin', b'z' * 6000)
    cluster.await_condition(lambda s: s['under_replicated'] == 0, timeout=60)

    on_disk_before = sum(len(chunk_files(p)) for p in cluster.chunkservers if p.alive())
    assert on_disk_before >= 3

    assert client.delete_file('/rep/trash.bin')

    deadline = time.time() + 90
    while time.time() < deadline:
        remaining = sum(len(chunk_files(p)) for p in cluster.chunkservers if p.alive())
        if remaining == 0:
            print(f'reclaimed {on_disk_before} replica file(s) after delete')
            return
        time.sleep(1.0)
    pytest.fail(f'{remaining} replica file(s) never reclaimed after delete')


def test_a_returning_server_is_told_to_drop_orphaned_chunks(cluster):
    """A server down during a delete still holds those chunks. No per-server
    bookkeeping is kept while it is away -- the instruction is derived from
    what it reports when it returns."""
    client = write_file(cluster, '/rep/stale.bin', b'w' * 4000)
    cluster.await_condition(lambda s: s['under_replicated'] == 0, timeout=60)

    victim = next(p for p in cluster.chunkservers if chunk_files(p))
    held = chunk_files(victim)
    victim.kill()

    assert client.delete_file('/rep/stale.bin')
    time.sleep(3.0)

    cluster.restart(victim)
    deadline = time.time() + 90
    while time.time() < deadline:
        if not (held & chunk_files(victim)):
            return
        time.sleep(1.0)
    pytest.fail(f'returning server kept orphans: {held & chunk_files(victim)}')

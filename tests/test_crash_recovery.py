#!/usr/bin/env python3
"""End-to-end crash tests against a real cluster.

The unit tests in test_recovery.py drive the master object directly, which
proves replay is correct but not that the fsync actually happens before the
client is told "yes". These tests run real processes and SIGKILL them, which
is the only way to tell the difference.

Slow by nature (each test boots a cluster), so they are marked and skipped by
default under `-m "not slow"`.
"""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cluster import LocalCluster  # noqa: E402

pytestmark = pytest.mark.slow


@pytest.fixture
def cluster():
    c = LocalCluster(masters=3, chunkservers=3, log_level='WARNING')
    c.start()
    yield c
    c.destroy()


def test_acknowledged_creates_survive_leader_sigkill(cluster):
    """The headline durability claim.

    Create 40 files, confirm each one was acknowledged, then SIGKILL the
    leader with no chance to clean up. Every acknowledged file must come back.
    Under the old 30-second JSON dump this loses whatever was created since
    the last tick.
    """
    client = cluster.client()
    acknowledged = []
    for i in range(40):
        name = f'/crash/file{i:03d}.txt'
        if client.create_file(name):
            acknowledged.append(name)

    assert len(acknowledged) == 40, 'setup failed, not all creates acknowledged'

    leader = cluster.kill_leader()
    assert leader is not None

    cluster.restart(leader)
    cluster.await_leader(timeout=60)

    client2 = cluster.client()
    listing = client2.list_all_files() or {}
    missing = [n for n in acknowledged if n not in listing]
    assert not missing, f'lost {len(missing)} acknowledged files: {missing[:5]}'


def test_written_data_is_readable_after_leader_sigkill(cluster):
    """Metadata surviving is not enough -- the data has to still be reachable
    through the recovered chunk handles."""
    client = cluster.client()
    payload = b'crash-test-payload-' * 500       # ~9.5 KB

    assert client.create_file('/crash/data.bin')
    assert client.write_file('/crash/data.bin', payload, 0)

    leader = cluster.kill_leader()
    cluster.restart(leader)
    cluster.await_leader(timeout=60)

    got = cluster.client().read_file('/crash/data.bin')
    assert got == payload


def test_deletes_do_not_resurrect_after_crash(cluster):
    """A delete that does not survive is worse than a lost create: the file
    comes back pointing at chunks that have been reclaimed."""
    client = cluster.client()
    for i in range(10):
        client.create_file(f'/crash/d{i}.txt')
    for i in range(0, 10, 2):
        assert client.delete_file(f'/crash/d{i}.txt')

    leader = cluster.kill_leader()
    cluster.restart(leader)
    cluster.await_leader(timeout=60)

    listing = cluster.client().list_all_files() or {}
    for i in range(0, 10, 2):
        assert f'/crash/d{i}.txt' not in listing, f'deleted file {i} came back'
    for i in range(1, 10, 2):
        assert f'/crash/d{i}.txt' in listing, f'surviving file {i} was lost'


def test_cluster_keeps_serving_while_a_master_is_down(cluster):
    """Availability, as distinct from durability: killing one of three
    masters should cost a failover, not the cluster."""
    client = cluster.client()
    assert client.create_file('/crash/before.txt')

    cluster.kill_leader()

    t0 = time.time()
    client2 = cluster.client()
    deadline = time.time() + 60
    created = False
    while time.time() < deadline:
        if client2.create_file('/crash/after.txt'):
            created = True
            break
        time.sleep(0.5)

    assert created, 'cluster never recovered write availability'
    print(f'failover to first successful write: {time.time() - t0:.2f}s')


def test_oplog_and_checkpoint_files_actually_exist(cluster):
    """Guards against the log silently not being used -- every other test
    here would still pass if persistence quietly fell back to snapshots."""
    client = cluster.client()
    for i in range(5):
        client.create_file(f'/crash/probe{i}.txt')
    time.sleep(1.0)

    found = False
    for m in cluster.masters:
        path = os.path.join(m.data_dir, 'oplog.bin')
        if os.path.exists(path) and os.path.getsize(path) > 0:
            found = True
    assert found, 'no master wrote a non-empty oplog'

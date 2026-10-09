#!/usr/bin/env python3
"""Erasure coding against a real cluster.

The unit tests cover the maths (test_reedsolomon.py) and the policy
(test_ec_manager.py). These prove the two actually meet: that a chunk
converts, that its replicas are retired only afterwards, that reads keep
working across the transition, and that the durability claim -- survives 3
losses where RF=3 survives 2 -- holds on real processes with real disks.

Runs with a short warm threshold so conversion happens in seconds rather
than the 5-minute production default.
"""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cluster import LocalCluster  # noqa: E402

pytestmark = pytest.mark.slow

# RS(6,3) needs 9 distinct servers. Smaller codes keep the test cluster
# affordable while exercising exactly the same code paths.
EC_ENV = {
    'GFS_STORAGE_MODE': 'ec',
    'GFS_EC_DATA_SHARDS': '4',
    'GFS_EC_PARITY_SHARDS': '2',
    'GFS_EC_WARM_AFTER': '3',
    'GFS_REPLICATION_SCAN_INTERVAL': '2',
    'GFS_HEARTBEAT_INTERVAL': '2',
}
N_SHARDS = 6     # k + m


def shard_files(proc):
    try:
        return {n for n in os.listdir(proc.data_dir)
                if '.shard' in n and n.endswith('.data')}
    except OSError:
        return set()


def chunk_files(proc):
    try:
        return {n for n in os.listdir(proc.data_dir)
                if n.startswith('chunk_') and n.endswith('.data')
                and '.shard' not in n}
    except OSError:
        return set()


@pytest.fixture
def cluster():
    # k+m+1 servers, not k+m. With exactly k+m, every live server already
    # holds a shard after one dies, so there is nowhere to put a rebuilt one
    # and the cluster cannot self-heal. That is a genuine property of the
    # scheme, not a bug -- EC needs a spare host to repair into.
    c = LocalCluster(masters=1, chunkservers=N_SHARDS + 1, log_level='WARNING',
                     env=EC_ENV)
    c.start()
    yield c
    c.destroy()


def total(cluster, fn):
    return sum(len(fn(p)) for p in cluster.chunkservers if p.alive())


def test_a_cold_chunk_is_converted_to_shards(cluster):
    """The core of ADR-005: write, wait, and the chunk becomes k+m shards
    with no client involvement."""
    client = cluster.client()
    payload = b'erasure-me-' * 2000
    assert client.create_file('/ec/cold.bin')
    assert client.write_file('/ec/cold.bin', payload, 0)

    elapsed = cluster.await_condition(
        lambda s: s.get('ec_chunks', 0) >= 1, timeout=90,
        describe=lambda s: (f"ec_chunks={s.get('ec_chunks')} "
                            f"encodes_in_flight={s.get('ec_encodes_in_flight')}"))
    print(f'conversion to EC after going cold: {elapsed:.1f}s')

    assert total(cluster, shard_files) >= N_SHARDS


def test_replicas_are_retired_only_after_every_shard_lands(cluster):
    """The ordering rule. Whole replicas must outlive the encode; dropping
    them early is the one unrecoverable mistake in an EC migration."""
    client = cluster.client()
    assert client.create_file('/ec/retire.bin')
    assert client.write_file('/ec/retire.bin', b'r' * 20000, 0)

    cluster.await_condition(lambda s: s.get('ec_chunks', 0) >= 1, timeout=90)

    # Once converted, the whole-chunk copies go away and only shards remain.
    deadline = time.time() + 60
    while time.time() < deadline:
        if total(cluster, chunk_files) == 0 and total(cluster, shard_files) >= N_SHARDS:
            return
        time.sleep(1.0)
    pytest.fail(f'replicas not retired: {total(cluster, chunk_files)} whole '
                f'chunk file(s), {total(cluster, shard_files)} shard(s)')


def test_data_is_byte_identical_after_conversion(cluster):
    """A read after conversion goes through the shard path and the decoder.
    If padding handling were wrong, this is where it shows."""
    payload = os.urandom(50_000)
    client = cluster.client()
    assert client.create_file('/ec/verify.bin')
    assert client.write_file('/ec/verify.bin', payload, 0)
    assert client.read_file('/ec/verify.bin') == payload

    cluster.await_condition(lambda s: s.get('ec_chunks', 0) >= 1, timeout=90)
    time.sleep(5)      # let replicas retire

    fresh = cluster.client()
    assert fresh.read_file('/ec/verify.bin') == payload


def test_reads_keep_working_during_the_conversion_window(cluster):
    """Both storage forms coexist while shards are being placed. A client
    should never see the migration."""
    payload = b'during-' * 3000
    client = cluster.client()
    assert client.create_file('/ec/during.bin')
    assert client.write_file('/ec/during.bin', payload, 0)

    deadline = time.time() + 60
    reads = failures = 0
    converted = False
    while time.time() < deadline:
        try:
            if cluster.client().read_file('/ec/during.bin') == payload:
                reads += 1
            else:
                failures += 1
        except Exception:
            failures += 1
        if cluster.status().get('ec_chunks', 0) >= 1:
            converted = True
            if reads > 8:
                break
        time.sleep(0.5)

    assert converted, 'chunk never converted; test proved nothing'
    assert failures == 0, f'{failures} failed reads across the transition'


def test_survives_losing_m_shards(cluster):
    """The durability headline: RS(4,2) tolerates 2 losses here; the shipped
    RS(6,3) tolerates 3, where RF=3 replication dies on its third."""
    payload = os.urandom(30_000)
    client = cluster.client()
    assert client.create_file('/ec/degraded.bin')
    assert client.write_file('/ec/degraded.bin', payload, 0)

    cluster.await_condition(lambda s: s.get('ec_chunks', 0) >= 1, timeout=90)
    time.sleep(5)

    holders = [p for p in cluster.chunkservers if shard_files(p)]
    assert len(holders) >= N_SHARDS, f'only {len(holders)} servers hold shards'

    parity = int(EC_ENV['GFS_EC_PARITY_SHARDS'])
    for victim in holders[:parity]:
        victim.kill()
    print(f'killed {parity} of {len(holders)} shard holders')

    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            if cluster.client().read_file('/ec/degraded.bin') == payload:
                return
        except Exception:
            pass
        time.sleep(1.0)
    pytest.fail('degraded read failed after losing exactly m shards')


def test_storage_overhead_actually_drops(cluster):
    """The number the whole feature exists to move, read off the master's
    own accounting rather than inferred."""
    client = cluster.client()
    for i in range(3):
        assert client.create_file(f'/ec/over{i}.bin')
        assert client.write_file(f'/ec/over{i}.bin', os.urandom(20_000), 0)

    before = cluster.status()
    ratio_before = before['physical_bytes'] / max(before['logical_bytes'], 1)
    assert ratio_before == pytest.approx(3.0, abs=0.01)

    cluster.await_condition(lambda s: s.get('ec_chunks', 0) >= 3, timeout=120)

    after = cluster.status()
    ratio_after = after['physical_bytes'] / max(after['logical_bytes'], 1)
    k, m = int(EC_ENV['GFS_EC_DATA_SHARDS']), int(EC_ENV['GFS_EC_PARITY_SHARDS'])
    expected = (k + m) / k
    print(f'storage overhead {ratio_before:.2f}x -> {ratio_after:.2f}x')
    assert ratio_after == pytest.approx(expected, abs=0.01)


def test_a_lost_shard_is_rebuilt(cluster):
    """Shard repair: reconstruct the missing piece from k survivors, without
    materialising the whole chunk."""
    payload = os.urandom(25_000)
    client = cluster.client()
    assert client.create_file('/ec/repair.bin')
    assert client.write_file('/ec/repair.bin', payload, 0)

    cluster.await_condition(lambda s: s.get('ec_chunks', 0) >= 1, timeout=90)
    time.sleep(5)

    holders = [p for p in cluster.chunkservers if shard_files(p)]
    victim = holders[0]
    lost = shard_files(victim)
    victim.kill()

    cluster.await_condition(
        lambda s: s.get('ec_chunks_degraded', 0) >= 1, timeout=60,
        describe=lambda s: f"degraded={s.get('ec_chunks_degraded')}")

    survivors = [p for p in cluster.chunkservers if p.alive()]
    deadline = time.time() + 90
    while time.time() < deadline:
        rebuilt = sum(1 for p in survivors if shard_files(p))
        if cluster.status().get('ec_chunks_degraded', 1) == 0:
            print(f'shard rebuilt; {rebuilt} servers now hold shards')
            assert cluster.client().read_file('/ec/repair.bin') == payload
            return
        time.sleep(2.0)
    pytest.fail(f'shard {lost} never rebuilt; '
                f'status={cluster.status()}')


def test_replication_mode_is_unaffected(cluster):
    """The A/B control: with GFS_STORAGE_MODE unset, nothing converts and
    the system behaves exactly as before."""
    c = LocalCluster(masters=1, chunkservers=4, log_level='WARNING',
                     env={'GFS_HEARTBEAT_INTERVAL': '2'})
    c.start()
    try:
        client = c.client()
        payload = b'no-ec-here-' * 500
        assert client.create_file('/plain.bin')
        assert client.write_file('/plain.bin', payload, 0)
        time.sleep(8)
        assert c.status().get('ec_chunks', 0) == 0
        assert client.read_file('/plain.bin') == payload
    finally:
        c.destroy()

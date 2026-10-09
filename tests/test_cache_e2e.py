#!/usr/bin/env python3
"""The client cache against a real cluster.

A cache is only worth having if it is both faster *and* still correct. These
tests check the correctness half -- that caching does not let a client read
stale data, miss a write, or keep talking to a replica that no longer exists.
The speed half is measured in Phase 3.
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
    c = LocalCluster(masters=3, chunkservers=4, log_level='WARNING')
    c.start()
    yield c
    c.destroy()


def test_a_second_read_is_served_without_touching_the_master(cluster):
    """The point of the cache: a repeat read of a hot file costs zero master
    RPCs. This is the property the paper relies on to justify one master."""
    payload = b'cache-me-' * 1000
    client = cluster.client()
    assert client.create_file('/cache/hot.bin')
    assert client.write_file('/cache/hot.bin', payload, 0)

    client.cache.clear()
    assert client.read_file('/cache/hot.bin') == payload
    saved_after_first = client.cache.master_rpcs_saved

    for _ in range(10):
        assert client.read_file('/cache/hot.bin') == payload

    gained = client.cache.master_rpcs_saved - saved_after_first
    assert gained >= 10, f'cache saved only {gained} RPCs across 10 reads'
    assert client.cache.stats()['combined_hit_rate'] > 0.5


def test_a_clients_own_write_is_visible_to_its_next_read(cluster):
    """Read-your-writes through the cache. If the size entry is not
    invalidated on write, the next read truncates to the stale length."""
    client = cluster.client()
    assert client.create_file('/cache/rw.bin')

    first = b'A' * 2000
    assert client.write_file('/cache/rw.bin', first, 0)
    assert client.read_file('/cache/rw.bin') == first

    longer = b'B' * 5000
    assert client.write_file('/cache/rw.bin', longer, 0)
    got = client.read_file('/cache/rw.bin')
    assert got == longer, f'read {len(got) if got else 0} bytes, expected 5000'


def test_a_deleted_file_is_not_served_from_cache(cluster):
    client = cluster.client()
    assert client.create_file('/cache/gone.bin')
    assert client.write_file('/cache/gone.bin', b'x' * 1000, 0)
    assert client.read_file('/cache/gone.bin') is not None

    assert client.delete_file('/cache/gone.bin')
    assert client.get_file_info('/cache/gone.bin') is None


def test_recreating_a_file_does_not_read_the_old_chunks(cluster):
    """The nastiest cache bug available here: same path, new chunks. If the
    old chunk handles survive in the cache, the client silently reads the
    previous file's contents."""
    client = cluster.client()
    assert client.create_file('/cache/reuse.bin')
    assert client.write_file('/cache/reuse.bin', b'OLD' * 500, 0)
    assert client.read_file('/cache/reuse.bin') is not None

    assert client.delete_file('/cache/reuse.bin')
    assert client.create_file('/cache/reuse.bin')
    new_payload = b'NEW' * 700
    assert client.write_file('/cache/reuse.bin', new_payload, 0)

    assert client.read_file('/cache/reuse.bin') == new_payload


def test_a_recursive_delete_invalidates_the_whole_subtree(cluster):
    """Files the client never named individually still have to leave the
    cache -- it has no way to enumerate what was under the directory."""
    client = cluster.client()
    for i in range(3):
        assert client.create_file(f'/cache/tree/f{i}.bin')
        assert client.write_file(f'/cache/tree/f{i}.bin', b'q' * 500, 0)
    for i in range(3):
        assert client.get_file_info(f'/cache/tree/f{i}.bin') is not None

    assert client.delete_directory('/cache/tree', recursive=True)

    for i in range(3):
        assert client.get_file_info(f'/cache/tree/f{i}.bin') is None


def test_reads_recover_when_a_cached_replica_dies(cluster):
    """The invalidation rule that makes caching safe.

    Warm the cache, then kill a server it cached. The client must notice its
    cached locations are wrong, refresh from the master, and still return the
    data -- not keep retrying dead replicas until the TTL expires.
    """
    payload = b'survive-the-kill-' * 300
    client = cluster.client()
    assert client.create_file('/cache/resil.bin')
    assert client.write_file('/cache/resil.bin', payload, 0)

    assert client.read_file('/cache/resil.bin') == payload   # warm
    cached_servers = set()
    for locs in [client.cache.locations.get(h)
                 for h in (client.get_file_info('/cache/resil.bin') or {}).get('chunks', [])]:
        for loc in locs or []:
            cached_servers.add(loc['server_id'])
    assert cached_servers, 'nothing cached, test would prove nothing'

    victim = next((p for p in cluster.chunkservers if p.name in cached_servers), None)
    assert victim is not None
    victim.kill()

    deadline = time.time() + 45
    while time.time() < deadline:
        try:
            if client.read_file('/cache/resil.bin') == payload:
                return
        except Exception:
            pass
        time.sleep(1.0)
    pytest.fail('client never recovered after a cached replica died')


def test_cache_off_still_passes_everything(cluster):
    """With caching disabled the client must behave identically -- which is
    what makes the A/B benchmark a fair comparison."""
    from metacache import MetadataCache
    client = cluster.client()
    client.cache = MetadataCache(mode='off')

    payload = b'nocache-' * 800
    assert client.create_file('/cache/off.bin')
    assert client.write_file('/cache/off.bin', payload, 0)
    assert client.read_file('/cache/off.bin') == payload
    assert client.cache.master_rpcs_saved == 0

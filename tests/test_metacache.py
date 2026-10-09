#!/usr/bin/env python3
"""Unit tests for the client metadata cache.

A cache that only ever returns correct answers is easy; the tests that matter
are the ones where the cached answer has gone wrong. Staleness is injected
with a fake clock rather than sleeping, so the TTL behaviour is tested
exactly instead of approximately.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metacache import MetadataCache, NullCache, TTLCache  # noqa: E402


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


# -------------------------------------------------------------- TTLCache
def test_hit_returns_the_stored_value():
    c = TTLCache(maxsize=4, ttl=60)
    c.put('k', 'v')
    assert c.get('k') == 'v'
    assert (c.hits, c.misses) == (1, 0)


def test_miss_on_unknown_key():
    c = TTLCache()
    assert c.get('nope') is None
    assert c.misses == 1


def test_entry_expires_exactly_at_its_ttl():
    clock = FakeClock()
    c = TTLCache(maxsize=4, ttl=10, clock=clock)
    c.put('k', 'v')

    clock.advance(9.9)
    assert c.get('k') == 'v'

    clock.advance(0.2)
    assert c.get('k') is None
    assert c.expirations == 1


def test_lru_evicts_the_least_recently_used():
    c = TTLCache(maxsize=3, ttl=60)
    for k in 'abc':
        c.put(k, k)
    c.get('a')          # 'b' is now the oldest
    c.put('d', 'd')

    assert c.get('b') is None
    assert c.get('a') == 'a'
    assert c.evictions == 1


def test_cache_never_exceeds_its_bound():
    """A client walking a large namespace must not grow this without limit."""
    c = TTLCache(maxsize=10, ttl=60)
    for i in range(500):
        c.put(f'k{i}', i)
    assert len(c) == 10


def test_reinserting_a_key_refreshes_its_ttl():
    clock = FakeClock()
    c = TTLCache(maxsize=4, ttl=10, clock=clock)
    c.put('k', 'v1')
    clock.advance(8)
    c.put('k', 'v2')
    clock.advance(5)
    assert c.get('k') == 'v2'


def test_invalidate_prefix_drops_only_matching_keys():
    c = TTLCache(maxsize=20, ttl=60)
    for k in ('/a/1', '/a/2', '/b/1'):
        c.put(k, k)
    assert c.invalidate_prefix('/a/') == 2
    assert c.get('/b/1') == '/b/1'
    assert c.get('/a/1') is None


def test_hit_rate_reflects_actual_usage():
    c = TTLCache()
    c.put('k', 'v')
    for _ in range(3):
        c.get('k')
    c.get('absent')
    assert c.hit_rate == pytest.approx(0.75)


# ------------------------------------------------------------- NullCache
def test_null_cache_never_hits():
    """`GFS_CLIENT_CACHE=off` swaps the object, not the call sites, so the
    A/B benchmark differs only in caching and nothing else."""
    c = NullCache()
    c.put('k', 'v')
    assert c.get('k') is None
    assert c.hit_rate == 0.0


def test_metadata_cache_in_off_mode_is_transparent():
    mc = MetadataCache(mode='off')
    mc.put_file_info('/f', {'size': 1, 'chunks': ['c1']})
    assert mc.get_file_info('/f') is None
    assert mc.master_rpcs_saved == 0


# --------------------------------------------------------- MetadataCache
def test_file_info_roundtrip_counts_a_saved_rpc():
    mc = MetadataCache(mode='lru')
    mc.put_file_info('/f', {'size': 10, 'chunks': ['c1']})
    assert mc.get_file_info('/f')['size'] == 10
    assert mc.master_rpcs_saved == 1


def test_invalidating_a_file_also_drops_its_chunk_locations():
    """Otherwise the locations outlive the file that owned them, and a
    recreated file with new chunks can read the old ones."""
    mc = MetadataCache(mode='lru')
    mc.put_file_info('/f', {'size': 10, 'chunks': ['c1', 'c2']})
    mc.put_locations('c1', [{'server_id': 's1'}])
    mc.put_locations('c2', [{'server_id': 's2'}])

    mc.invalidate_file('/f')

    assert mc.get_file_info('/f') is None
    assert mc.get_locations('c1') is None
    assert mc.get_locations('c2') is None


def test_invalidating_one_file_leaves_other_files_alone():
    mc = MetadataCache(mode='lru')
    mc.put_file_info('/keep', {'size': 1, 'chunks': ['k1']})
    mc.put_locations('k1', [{'server_id': 's1'}])
    mc.put_file_info('/drop', {'size': 1, 'chunks': ['d1']})
    mc.put_locations('d1', [{'server_id': 's1'}])

    mc.invalidate_file('/drop')

    assert mc.get_file_info('/keep') is not None
    assert mc.get_locations('k1') is not None


def test_invalidate_chunk_reports_whether_it_dropped_anything():
    """The client uses this to tell 'the cache was wrong, retry' apart from
    'we never had it cached, so the master's answer is already fresh' --
    without which the read path can recurse forever."""
    mc = MetadataCache(mode='lru')
    mc.put_locations('c1', [{'server_id': 's1'}])
    assert mc.invalidate_chunk('c1') is True
    assert mc.invalidate_chunk('c1') is False
    assert mc.invalidate_chunk('never_seen') is False


def test_invalidate_tree_covers_the_whole_subtree():
    """A recursive delete removes files the client never named."""
    mc = MetadataCache(mode='lru')
    for path in ('/data/a.txt', '/data/sub/b.txt', '/other/c.txt'):
        mc.put_file_info(path, {'size': 1, 'chunks': []})

    mc.invalidate_tree('/data')

    assert mc.get_file_info('/data/a.txt') is None
    assert mc.get_file_info('/data/sub/b.txt') is None
    assert mc.get_file_info('/other/c.txt') is not None


def test_invalidate_tree_handles_a_trailing_slash():
    mc = MetadataCache(mode='lru')
    mc.put_file_info('/d/x', {'size': 1, 'chunks': []})
    mc.invalidate_tree('/d/')
    assert mc.get_file_info('/d/x') is None


def test_stats_report_both_caches():
    mc = MetadataCache(mode='lru')
    mc.put_file_info('/f', {'size': 1, 'chunks': ['c1']})
    mc.put_locations('c1', [{'server_id': 's1'}])
    mc.get_file_info('/f')
    mc.get_locations('c1')
    mc.get_file_info('/missing')

    st = mc.stats()
    assert st['mode'] == 'lru'
    assert st['master_rpcs_saved'] == 2
    assert 0 < st['combined_hit_rate'] < 1


def test_clear_empties_everything():
    mc = MetadataCache(mode='lru')
    mc.put_file_info('/f', {'size': 1, 'chunks': ['c1']})
    mc.put_locations('c1', [{'server_id': 's1'}])
    mc.clear()
    assert mc.get_file_info('/f') is None
    assert mc.get_locations('c1') is None

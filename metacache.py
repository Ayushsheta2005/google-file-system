#!/usr/bin/env python3
"""Client-side metadata cache.

Every read in the original client made two round trips to the master before
touching any data: `GetFileInfo` to learn the chunk list, then
`GetChunkLocations` per chunk. For a 1 GB file (16 chunks) that is 17 master
RPCs to read data the client may already know where to find -- and it does it
again on the next read of the same file.

The paper's whole argument for why one master is enough (§2.4) depends on
clients not doing this:

    "Clients never read and write file data through the master. Instead, a
    client asks the master which chunkservers it should contact. It caches
    this information for a limited time and interacts with the chunkservers
    directly for many subsequent operations."

Two properties make this safe, and both have to hold:

  * **Bounded staleness.** Entries expire on a TTL, so a client cannot hold a
    wrong answer indefinitely.
  * **Invalidate on contradiction.** Any error from a chunk server that
    implies the cached answer is wrong -- NOT_FOUND, a version mismatch, an
    unreachable host -- drops the entry and retries uncached. This half is
    the one that matters: a cache without it will keep sending reads to a
    replica that no longer has the chunk until the TTL happens to run out.

See ADR-006.
"""

import threading
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, Optional


class _Entry:
    __slots__ = ('value', 'expires_at')

    def __init__(self, value: Any, expires_at: float):
        self.value = value
        self.expires_at = expires_at


class TTLCache:
    """Bounded LRU with per-entry expiry. Thread-safe.

    `OrderedDict.move_to_end` gives LRU ordering in O(1); eviction pops the
    oldest. Size is bounded so a client walking a large namespace cannot grow
    the cache without limit.
    """

    def __init__(self, maxsize: int = 4096, ttl: float = 60.0, clock=time.monotonic):
        self.maxsize = max(1, maxsize)
        self.ttl = ttl
        self._clock = clock
        self._data: 'OrderedDict[Any, _Entry]' = OrderedDict()
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0
        self.expirations = 0
        self.invalidations = 0
        self.evictions = 0

    def get(self, key):
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                self.misses += 1
                return None
            if self._clock() >= entry.expires_at:
                del self._data[key]
                self.expirations += 1
                self.misses += 1
                return None
            self._data.move_to_end(key)
            self.hits += 1
            return entry.value

    def put(self, key, value, ttl: Optional[float] = None):
        with self._lock:
            if key in self._data:
                del self._data[key]
            self._data[key] = _Entry(value, self._clock() + (ttl if ttl is not None else self.ttl))
            while len(self._data) > self.maxsize:
                self._data.popitem(last=False)
                self.evictions += 1

    def invalidate(self, key) -> bool:
        with self._lock:
            if key in self._data:
                del self._data[key]
                self.invalidations += 1
                return True
            return False

    def invalidate_prefix(self, prefix: str) -> int:
        """Drop every string key starting with `prefix`.

        Needed for directory operations: deleting /a/ has to invalidate
        /a/b.txt too, and the client has no way to enumerate what the master
        considered to be under that directory.
        """
        with self._lock:
            doomed = [k for k in self._data
                      if isinstance(k, str) and k.startswith(prefix)]
            for k in doomed:
                del self._data[k]
            self.invalidations += len(doomed)
            return len(doomed)

    def clear(self):
        with self._lock:
            n = len(self._data)
            self._data.clear()
            self.invalidations += n

    def __len__(self):
        with self._lock:
            return len(self._data)

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'size': len(self._data),
                'maxsize': self.maxsize,
                'ttl': self.ttl,
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': round(self.hit_rate, 4),
                'expirations': self.expirations,
                'invalidations': self.invalidations,
                'evictions': self.evictions,
            }


class NullCache:
    """Disabled cache: every lookup misses.

    Exists so `GFS_CLIENT_CACHE=off` needs no branches at the call sites --
    the A/B benchmark swaps the object, not the code path, so the two
    configurations differ only in caching and not in anything else.
    """

    def __init__(self, *a, **kw):
        self.hits = 0
        self.misses = 0

    def get(self, key):
        self.misses += 1
        return None

    def put(self, key, value, ttl=None):
        pass

    def invalidate(self, key):
        return False

    def invalidate_prefix(self, prefix):
        return 0

    def clear(self):
        pass

    def __len__(self):
        return 0

    @property
    def hit_rate(self):
        return 0.0

    def stats(self):
        return {'size': 0, 'hits': 0, 'misses': self.misses,
                'hit_rate': 0.0, 'mode': 'off'}


class MetadataCache:
    """The two caches a GFS client needs, plus the invalidation rules.

    Kept as one object because the invalidation rules span both: deleting a
    file must drop its chunk locations too, and the call sites should not
    have to remember that.
    """

    def __init__(self, mode: str = 'lru', maxsize: int = 4096, ttl: float = 60.0):
        self.mode = mode
        factory = TTLCache if mode == 'lru' else NullCache
        # filename -> {'size', 'chunks', 'chunk_size'}
        self.file_info = factory(maxsize=maxsize, ttl=ttl)
        # chunk_handle -> [{'server_id','address','port'}, ...]
        self.locations = factory(maxsize=maxsize * 4, ttl=ttl)
        self.master_rpcs_saved = 0

    # -------------------------------------------------------------- lookup
    def get_file_info(self, filename: str):
        v = self.file_info.get(filename)
        if v is not None:
            self.master_rpcs_saved += 1
        return v

    def put_file_info(self, filename: str, info: dict):
        self.file_info.put(filename, info)

    def get_locations(self, chunk_handle: str):
        v = self.locations.get(chunk_handle)
        if v is not None:
            self.master_rpcs_saved += 1
        return v

    def put_locations(self, chunk_handle: str, locations: list):
        self.locations.put(chunk_handle, locations)

    # -------------------------------------------------------- invalidation
    def invalidate_file(self, filename: str):
        """Drop a file and the locations of every chunk it owned.

        Reads the cached entry first to find the chunk handles -- after the
        file entry is gone there is no way to know which locations belonged
        to it, and those would then linger until their own TTL.
        """
        cached = self.file_info.get(filename)
        self.file_info.invalidate(filename)
        if cached:
            for handle in cached.get('chunks', []) or []:
                if handle:
                    self.locations.invalidate(handle)

    def invalidate_chunk(self, chunk_handle: str) -> bool:
        """Returns True if an entry was actually dropped, so callers can tell
        'the cache was wrong, retry' apart from 'we never had it cached, so
        the master's answer already is the fresh one'."""
        return self.locations.invalidate(chunk_handle)

    def invalidate_tree(self, path: str):
        """Directory operations: everything under this prefix is suspect."""
        prefix = path if path.endswith('/') else path + '/'
        n = self.file_info.invalidate_prefix(prefix)
        self.file_info.invalidate(path.rstrip('/'))
        return n

    def clear(self):
        self.file_info.clear()
        self.locations.clear()

    # --------------------------------------------------------------- stats
    def stats(self) -> dict:
        fi, loc = self.file_info.stats(), self.locations.stats()
        total_hits = fi.get('hits', 0) + loc.get('hits', 0)
        total = total_hits + fi.get('misses', 0) + loc.get('misses', 0)
        return {
            'mode': self.mode,
            'file_info': fi,
            'locations': loc,
            'master_rpcs_saved': self.master_rpcs_saved,
            'combined_hit_rate': round(total_hits / total, 4) if total else 0.0,
        }

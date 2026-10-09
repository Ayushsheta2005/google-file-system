#!/usr/bin/env python3
"""Shard storage, encoding and repair for a chunk server.

Split out of chunk_server.py because the EC path shares almost nothing with
the replica path: different on-disk layout, different failure handling, and
a hard requirement that a half-finished encode never looks finished.

On-disk layout, one file per shard:

    <handle>.shard<index>.data     the shard bytes
    <handle>.shard<index>.meta     json: index, k, m, original_length, version

The sidecar metadata is per-shard rather than per-chunk on purpose. A shard
is the unit that moves between servers during repair, and a server holding
shard 7 of a chunk may never have seen any other part of it -- so everything
needed to interpret that shard has to travel with it. `original_length` in
particular: without it, decode returns the zero-padded length and every
recovered file is silently longer than it was.

See ADR-005 and ADR-007.
"""

import json
import os
import threading
from typing import Dict, List, Optional, Tuple

from reedsolomon import ReedSolomon


class ShardInfo:
    __slots__ = ('handle', 'index', 'size', 'k', 'm', 'original_length', 'version')

    def __init__(self, handle, index, size, k, m, original_length, version):
        self.handle = handle
        self.index = index
        self.size = size
        self.k = k
        self.m = m
        self.original_length = original_length
        self.version = version

    def as_dict(self):
        return {'handle': self.handle, 'index': self.index, 'size': self.size,
                'k': self.k, 'm': self.m,
                'original_length': self.original_length, 'version': self.version}


class ShardStore:
    """The shards one chunk server holds."""

    def __init__(self, storage_dir: str, logger=None):
        self.dir = storage_dir
        self.logger = logger
        self._lock = threading.RLock()
        self.shards: Dict[Tuple[str, int], ShardInfo] = {}
        os.makedirs(storage_dir, exist_ok=True)
        self.load_existing()

    # ------------------------------------------------------------- paths
    def shard_path(self, handle: str, index: int) -> str:
        return os.path.join(self.dir, f'{handle}.shard{index}.data')

    def meta_path(self, handle: str, index: int) -> str:
        return os.path.join(self.dir, f'{handle}.shard{index}.meta')

    # -------------------------------------------------------------- load
    def load_existing(self):
        """Rebuild the in-memory index from disk at startup.

        A shard whose sidecar is missing or unparseable is treated as absent
        and removed: it cannot be interpreted, and leaving it would let the
        server report a shard it cannot actually serve -- which the master
        would then count toward durability.
        """
        try:
            names = os.listdir(self.dir)
        except OSError:
            return
        for name in names:
            if not name.endswith('.shard') and '.shard' not in name:
                continue
            if not name.endswith('.data'):
                continue
            stem = name[:-len('.data')]
            if '.shard' not in stem:
                continue
            handle, _, idx_s = stem.partition('.shard')
            try:
                index = int(idx_s)
            except ValueError:
                continue

            mpath = self.meta_path(handle, index)
            dpath = self.shard_path(handle, index)
            try:
                with open(mpath) as f:
                    meta = json.load(f)
                size = os.path.getsize(dpath)
            except (OSError, ValueError) as e:
                if self.logger:
                    self.logger.warning(
                        f'discarding uninterpretable shard {handle}#{index}: {e}')
                self._remove_files(handle, index)
                continue

            with self._lock:
                self.shards[(handle, index)] = ShardInfo(
                    handle=handle, index=index, size=size,
                    k=meta.get('k', 0), m=meta.get('m', 0),
                    original_length=meta.get('original_length', 0),
                    version=meta.get('version', 1))

        if self.shards and self.logger:
            self.logger.info(f'loaded {len(self.shards)} erasure-coded shard(s)')

    # ------------------------------------------------------------- store
    def put(self, handle: str, index: int, data: bytes, k: int, m: int,
            original_length: int, version: int) -> ShardInfo:
        """Write a shard durably.

        Data is written and fsync'd *before* the sidecar, and the sidecar is
        renamed into place last. A crash between the two leaves a data file
        with no sidecar, which `load_existing` discards -- so the shard is
        either fully usable or absent, never half-registered.
        """
        dpath = self.shard_path(handle, index)
        mpath = self.meta_path(handle, index)
        tmp_d, tmp_m = dpath + '.tmp', mpath + '.tmp'

        with open(tmp_d, 'wb') as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp_d, dpath)

        meta = {'index': index, 'k': k, 'm': m,
                'original_length': original_length, 'version': version}
        with open(tmp_m, 'w') as f:
            json.dump(meta, f)
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp_m, mpath)

        info = ShardInfo(handle, index, len(data), k, m, original_length, version)
        with self._lock:
            self.shards[(handle, index)] = info
        return info

    def get(self, handle: str, index: int) -> Optional[bytes]:
        with self._lock:
            if (handle, index) not in self.shards:
                return None
        try:
            with open(self.shard_path(handle, index), 'rb') as f:
                return f.read()
        except OSError:
            # On disk a moment ago, gone now. Drop it rather than keep
            # advertising a shard we cannot serve.
            self.forget(handle, index)
            return None

    def info(self, handle: str, index: int) -> Optional[ShardInfo]:
        with self._lock:
            return self.shards.get((handle, index))

    def _remove_files(self, handle: str, index: int):
        for path in (self.shard_path(handle, index), self.meta_path(handle, index)):
            try:
                os.remove(path)
            except OSError:
                pass

    def forget(self, handle: str, index: int):
        with self._lock:
            self.shards.pop((handle, index), None)

    def delete(self, handle: str, index: int) -> bool:
        with self._lock:
            existed = self.shards.pop((handle, index), None) is not None
        self._remove_files(handle, index)
        return existed

    def delete_all_for(self, handle: str) -> int:
        with self._lock:
            keys = [key for key in self.shards if key[0] == handle]
        for h, i in keys:
            self.delete(h, i)
        return len(keys)

    def held(self) -> List[ShardInfo]:
        with self._lock:
            return list(self.shards.values())

    def __len__(self):
        with self._lock:
            return len(self.shards)


# --------------------------------------------------------------------------
# Encode / repair, as performed by a chunk server
# --------------------------------------------------------------------------
class ECWorker:
    """Runs encode and repair tasks handed down on the heartbeat reply.

    Takes `fetch_shard` and `push_shard` callbacks instead of making gRPC
    calls itself, so the whole thing is testable without a cluster.
    """

    def __init__(self, store: ShardStore, fetch_shard, push_shard, logger=None):
        self.store = store
        self.fetch_shard = fetch_shard     # (addr, port, handle, index) -> bytes|None
        self.push_shard = push_shard       # (addr, port, handle, index, data, k, m, length, ver) -> bool
        self.logger = logger

    def encode_and_distribute(self, handle: str, data: bytes, k: int, m: int,
                              version: int, targets: List[dict]) -> Tuple[bool, str]:
        """Encode a whole chunk into k+m shards and place one on each target.

        Returns (all_placed, detail). The caller must NOT drop the original
        replicas unless this returns True: a partial placement means fewer
        than k+m shards exist, and dropping the replicas on top of that is
        how an EC migration loses data.
        """
        if len(targets) != k + m:
            return False, f'need {k + m} targets, got {len(targets)}'

        rs = ReedSolomon(k, m)
        shards, original_length = rs.encode(data)

        placed, failed = 0, []
        for index, (shard, target) in enumerate(zip(shards, targets)):
            ok = self.push_shard(target['address'], target['port'], handle,
                                 index, shard, k, m, original_length, version)
            if ok:
                placed += 1
            else:
                failed.append(f'{target.get("server_id", "?")}#{index}')

        if placed == k + m:
            return True, f'placed {placed} shards'
        return False, f'placed {placed}/{k + m}; failed: {", ".join(failed)}'

    def reconstruct_shard(self, handle: str, index: int, k: int, m: int,
                          original_length: int,
                          sources: List[dict]) -> Tuple[bool, str]:
        """Rebuild one missing shard from any k survivors, and store it.

        Note the cost: repairing a single shard requires reading k shards
        across the network. That is Reed-Solomon's well-known repair-bandwidth
        problem and is the honest downside of EC versus replication, where a
        repair is one copy from one peer. Named in ADR-005.
        """
        rs = ReedSolomon(k, m)
        collected: List[Optional[bytes]] = [None] * (k + m)

        got = 0
        for src in sources:
            if got >= k:
                break
            idx = src['shard_index']
            if idx == index:
                continue
            data = self.fetch_shard(src['address'], src['port'], handle, idx)
            if data:
                collected[idx] = data
                got += 1

        if got < k:
            return False, f'only {got} of {k} shards reachable; cannot reconstruct'

        try:
            rebuilt = rs.reconstruct_shard(collected, index)
        except Exception as e:
            return False, f'reconstruction failed: {e}'

        self.store.put(handle, index, rebuilt, k, m, original_length, 1)
        return True, f'rebuilt shard {index} from {got} survivors'


def decode_chunk(shards_by_index: Dict[int, bytes], k: int, m: int,
                 original_length: int) -> bytes:
    """Reassemble a chunk from whatever shards were collected.

    Used by the client on a degraded read. Fast when the k data shards are
    all present (systematic form means it is a concatenation); only pays a
    matrix inversion when a data shard is actually missing.
    """
    rs = ReedSolomon(k, m)
    slots: List[Optional[bytes]] = [None] * (k + m)
    for index, data in shards_by_index.items():
        if 0 <= index < k + m:
            slots[index] = data
    return rs.decode(slots, original_length)

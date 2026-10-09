#!/usr/bin/env python3
"""Master-side erasure coding: what to encode, where shards go, what to repair.

Mirrors `ReplicationManager` deliberately -- plain facts in, plain decisions
out, no gRPC and no master reference -- so the same object can be driven by
tests and by the Phase 2 simulator without a cluster.

The policy, following SeaweedFS rather than the GFS paper (which has no
erasure coding at all): **hot data stays replicated, warm data gets encoded
in the background.** A chunk is eligible for conversion when it has not been
written for `warm_after` seconds and is currently fully replicated. Writes
never pay the encoding cost, and a chunk still being appended to is never
disturbed.

The ordering rule that matters most: **replicas are dropped only after every
shard is confirmed present.** Between "encode started" and "all k+m shards
reported", the chunk is stored twice over. That wastes space briefly and it
is the only ordering in which a crashed conversion cannot lose data.

See ADR-005 and ADR-007.
"""

import threading
import time
from typing import Dict, List, Optional, Set, Tuple


class ECChunkState:
    """What the master knows about one erasure-coded chunk."""

    __slots__ = ('handle', 'k', 'm', 'original_length', 'version', 'shards')

    def __init__(self, handle, k, m, original_length, version=1):
        self.handle = handle
        self.k = k
        self.m = m
        self.original_length = original_length
        self.version = version
        self.shards: Dict[int, str] = {}      # shard_index -> server_id

    @property
    def n(self) -> int:
        return self.k + self.m

    def missing(self) -> List[int]:
        return [i for i in range(self.n) if i not in self.shards]

    def live_count(self, live_servers: Set[str]) -> int:
        return sum(1 for s in self.shards.values() if s in live_servers)

    def readable(self, live_servers: Set[str]) -> bool:
        return self.live_count(live_servers) >= self.k

    def as_dict(self):
        return {'handle': self.handle, 'k': self.k, 'm': self.m,
                'original_length': self.original_length,
                'version': self.version, 'shards': dict(self.shards)}

    @classmethod
    def from_dict(cls, d):
        st = cls(d['handle'], d['k'], d['m'], d.get('original_length', 0),
                 d.get('version', 1))
        # Shard placement is rebuilt from heartbeats, like replica locations
        # (ADR-001). Restoring it from a checkpoint would mean trusting a
        # record of where shards were, not where they are.
        return st


class ECManager:
    def __init__(self, data_shards: int = 6, parity_shards: int = 3,
                 warm_after: float = 300.0, max_encodes_in_flight: int = 2,
                 encode_timeout: float = 300.0, logger=None):
        self.k = data_shards
        self.m = parity_shards
        self.warm_after = warm_after
        self.max_encodes = max_encodes_in_flight
        self.encode_timeout = encode_timeout
        self.logger = logger

        self._lock = threading.RLock()
        self.ec_chunks: Dict[str, ECChunkState] = {}

        # handle -> (encoder_server_id, deadline)
        self._encoding: Dict[str, Tuple[str, float]] = {}
        # (handle, shard_index) -> (repairer_server_id, deadline)
        self._repairing: Dict[Tuple[str, int], Tuple[str, float]] = {}
        # server_id -> set of (handle, shard_index) to drop
        self._shard_deletes: Dict[str, Set[Tuple[str, int]]] = {}
        # handles whose replicas may now be dropped
        self._replicas_to_drop: Set[str] = set()

        self.stats = {'encodes_started': 0, 'encodes_completed': 0,
                      'encodes_timed_out': 0, 'repairs_started': 0,
                      'repairs_completed': 0, 'bytes_reclaimed': 0}

    @property
    def n(self) -> int:
        return self.k + self.m

    # ------------------------------------------------------------ queries
    def is_ec(self, handle: str) -> bool:
        with self._lock:
            return handle in self.ec_chunks

    def state(self, handle: str) -> Optional[ECChunkState]:
        with self._lock:
            return self.ec_chunks.get(handle)

    # ---------------------------------------------------------- selection
    def eligible_for_encoding(self, chunks: dict, live_servers: Set[str],
                              now: Optional[float] = None) -> List[str]:
        """Chunks that are cold enough, fully replicated, and not already EC.

        Requiring *full* replication before converting is deliberate: a chunk
        that is already degraded should be repaired by the cheap path
        (copy one replica) before being put through an encode that reads the
        whole thing and writes k+m pieces.
        """
        now = now or time.time()
        out = []
        with self._lock:
            for handle, chunk in chunks.items():
                if handle in self.ec_chunks or handle in self._encoding:
                    continue
                if getattr(chunk, 'size', 0) <= 0:
                    continue       # nothing written yet
                alive = [s for s in chunk.locations if s in live_servers]
                if len(alive) < 3:
                    continue
                last_touched = getattr(chunk, 'last_modified', None)
                if last_touched is None:
                    continue
                if now - last_touched < self.warm_after:
                    continue       # still hot
                out.append(handle)
        return out

    def choose_shard_targets(self, servers: dict, live_servers: Set[str]
                             ) -> Optional[List[str]]:
        """Pick n distinct servers, emptiest first.

        Returns None unless there are n of them. Placing two shards of the
        same chunk on one host would silently break the durability claim:
        you would advertise m-failure tolerance while a single host loss
        takes out two shards. Better to not convert than to lie.
        """
        candidates = sorted(
            (s for s in live_servers if s in servers),
            key=lambda s: getattr(servers[s], 'available_space', 0),
            reverse=True)
        if len(candidates) < self.n:
            return None
        return candidates[:self.n]

    # ------------------------------------------------------------- encode
    def plan_encodes(self, chunks: dict, servers: dict, live_servers: Set[str]
                     ) -> int:
        with self._lock:
            self._expire()
            scheduled = 0
            for handle in self.eligible_for_encoding(chunks, live_servers):
                if len(self._encoding) >= self.max_encodes:
                    break
                targets = self.choose_shard_targets(servers, live_servers)
                if targets is None:
                    if self.logger:
                        self.logger.debug(
                            f'EC skipped for {handle}: need {self.n} distinct '
                            f'servers, have {len(live_servers)}')
                    break
                chunk = chunks[handle]
                encoder = next((s for s in chunk.locations if s in live_servers), None)
                if encoder is None:
                    continue
                self._encoding[handle] = (encoder, time.time() + self.encode_timeout)
                self._pending_targets = getattr(self, '_pending_targets', {})
                self._pending_targets[handle] = targets
                self.stats['encodes_started'] += 1
                scheduled += 1
            return scheduled

    def encode_tasks_for(self, server_id: str, chunks: dict, servers: dict
                         ) -> List[dict]:
        with self._lock:
            pending = getattr(self, '_pending_targets', {})
            out = []
            for handle, (encoder, _) in self._encoding.items():
                if encoder != server_id:
                    continue
                targets = pending.get(handle)
                chunk = chunks.get(handle)
                if not targets or chunk is None:
                    continue
                out.append({
                    'chunk_handle': handle,
                    'k': self.k, 'm': self.m,
                    'chunk_version': chunk.version,
                    'targets': [
                        {'server_id': t, 'address': servers[t].address,
                         'port': servers[t].port}
                        for t in targets if t in servers
                    ],
                })
            return out

    def note_shard_present(self, handle: str, index: int, server_id: str,
                           k: int, m: int, original_length: int):
        """A heartbeat reported a shard. Register it, and if the full set is
        now present, mark the chunk converted and its replicas droppable."""
        with self._lock:
            st = self.ec_chunks.get(handle)
            if st is None:
                st = ECChunkState(handle, k or self.k, m or self.m,
                                  original_length)
                self.ec_chunks[handle] = st
            if original_length and not st.original_length:
                st.original_length = original_length
            st.shards[index] = server_id

            if handle in self._encoding and len(st.shards) >= st.n:
                del self._encoding[handle]
                getattr(self, '_pending_targets', {}).pop(handle, None)
                self._replicas_to_drop.add(handle)
                self.stats['encodes_completed'] += 1
                if self.logger:
                    self.logger.info(
                        f'🧮 chunk {handle} now erasure-coded '
                        f'({st.k}+{st.m}); replicas can be dropped')

    def note_shard_absent(self, handle: str, index: int, server_id: str):
        with self._lock:
            st = self.ec_chunks.get(handle)
            if st and st.shards.get(index) == server_id:
                del st.shards[index]

    def take_replicas_to_drop(self) -> List[str]:
        """Chunks whose full shard set is confirmed; their whole replicas are
        now redundant. Consumed once -- the caller deletes them."""
        with self._lock:
            out = list(self._replicas_to_drop)
            self._replicas_to_drop.clear()
            return out

    # ------------------------------------------------------------- repair
    def degraded(self, live_servers: Set[str]) -> List[Tuple[str, List[int]]]:
        """EC chunks missing at least one shard, most urgent first."""
        with self._lock:
            out = []
            for handle, st in self.ec_chunks.items():
                missing = [i for i in range(st.n)
                           if st.shards.get(i) not in live_servers]
                if missing:
                    out.append((handle, missing))
            out.sort(key=lambda t: -len(t[1]))
            return out

    def plan_repairs(self, servers: dict, live_servers: Set[str]) -> int:
        with self._lock:
            self._expire()
            scheduled = 0
            for handle, missing in self.degraded(live_servers):
                st = self.ec_chunks[handle]
                if st.live_count(live_servers) < st.k:
                    if self.logger:
                        self.logger.error(
                            f'EC chunk {handle} unreadable: only '
                            f'{st.live_count(live_servers)} of {st.k} shards live')
                    continue
                holders = {s for s in st.shards.values()}
                for index in missing:
                    if (handle, index) in self._repairing:
                        continue
                    target = next(
                        (s for s in sorted(
                            live_servers,
                            key=lambda x: -getattr(servers[x], 'available_space', 0)
                            if x in servers else 0)
                         if s not in holders), None)
                    if target is None:
                        # Every live server already holds a shard of this
                        # chunk, so there is nowhere to put the rebuilt one
                        # without doubling up -- which would mean claiming
                        # m-failure tolerance while one host held two
                        # shards. A cluster of exactly k+m nodes therefore
                        # cannot self-heal; it needs at least one spare.
                        if self.logger:
                            self.logger.warning(
                                f'EC chunk {handle} shard {index} cannot be '
                                f'repaired: all {len(live_servers)} live servers '
                                f'already hold a shard. Need at least '
                                f'{self.n + 1} servers for self-healing.')
                        break
                    self._repairing[(handle, index)] = (
                        target, time.time() + self.encode_timeout)
                    self.stats['repairs_started'] += 1
                    scheduled += 1
                    break      # one shard at a time per chunk
            return scheduled

    def repair_tasks_for(self, server_id: str, servers: dict) -> List[dict]:
        with self._lock:
            out = []
            for (handle, index), (target, _) in self._repairing.items():
                if target != server_id:
                    continue
                st = self.ec_chunks.get(handle)
                if st is None:
                    continue
                sources = [
                    {'server_id': sid, 'address': servers[sid].address,
                     'port': servers[sid].port, 'shard_index': i}
                    for i, sid in st.shards.items() if sid in servers
                ]
                if len(sources) < st.k:
                    continue
                out.append({'chunk_handle': handle, 'shard_index': index,
                            'k': st.k, 'm': st.m,
                            'original_length': st.original_length,
                            'sources': sources})
            return out

    def note_repair_done(self, handle: str, index: int, server_id: str):
        with self._lock:
            if self._repairing.pop((handle, index), None):
                self.stats['repairs_completed'] += 1

    # ------------------------------------------------------------ cleanup
    def plan_shard_deletions(self, reported: Dict[str, Set[Tuple[str, int]]]
                             ) -> int:
        """Shards a server holds that the master does not recognise."""
        with self._lock:
            queued = 0
            for server_id, held in reported.items():
                for handle, index in held:
                    st = self.ec_chunks.get(handle)
                    if st is None or st.shards.get(index) != server_id:
                        if st is None:
                            self._shard_deletes.setdefault(
                                server_id, set()).add((handle, index))
                            queued += 1
            return queued

    def shard_deletions_for(self, server_id: str, limit: int = 32
                            ) -> List[Tuple[str, int]]:
        with self._lock:
            pending = self._shard_deletes.get(server_id)
            if not pending:
                return []
            batch = list(pending)[:limit]
            for item in batch:
                pending.discard(item)
            if not pending:
                self._shard_deletes.pop(server_id, None)
            return batch

    def forget_chunk(self, handle: str):
        with self._lock:
            self.ec_chunks.pop(handle, None)
            self._encoding.pop(handle, None)
            for key in [k for k in self._repairing if k[0] == handle]:
                del self._repairing[key]

    def _expire(self):
        now = time.time()
        for handle, (server, deadline) in list(self._encoding.items()):
            if now > deadline:
                del self._encoding[handle]
                getattr(self, '_pending_targets', {}).pop(handle, None)
                self.stats['encodes_timed_out'] += 1
                # The partially-placed shards stay; they are harmless and the
                # next attempt overwrites them. Crucially the replicas were
                # never dropped, so nothing was lost.
                if self.logger:
                    self.logger.warning(
                        f'EC encode of {handle} on {server} timed out; '
                        f'replicas retained')
        for key, (server, deadline) in list(self._repairing.items()):
            if now > deadline:
                del self._repairing[key]

    # -------------------------------------------------------------- stats
    def snapshot(self, live_servers: Optional[Set[str]] = None) -> dict:
        with self._lock:
            live = live_servers or set()
            degraded = sum(
                1 for st in self.ec_chunks.values()
                if any(st.shards.get(i) not in live for i in range(st.n)))
            return {
                'ec_chunks': len(self.ec_chunks),
                'degraded': degraded,
                'encodes_in_flight': len(self._encoding),
                'repairs_in_flight': len(self._repairing),
                'stats': dict(self.stats),
            }

    def overhead_for(self, chunks: dict) -> Tuple[float, float]:
        """(logical bytes, physical bytes) across both storage modes.

        This is the number the whole feature exists to move: replicated
        chunks cost 3x, erasure-coded chunks cost (k+m)/k.
        """
        logical = physical = 0.0
        with self._lock:
            for handle, chunk in chunks.items():
                size = float(getattr(chunk, 'size', 0) or 0)
                logical += size
                st = self.ec_chunks.get(handle)
                physical += size * (st.n / st.k if st else 3.0)
        return logical, physical

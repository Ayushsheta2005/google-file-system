#!/usr/bin/env python3
"""Re-replication and garbage collection, driven from chunk server heartbeats.

The original code detected under-replication and then did nothing about it:

    if len(alive_locations) < REPLICATION_FACTOR:
        self.logger.warning(f"Chunk {handle} is under-replicated: ...")

`ReplicationTask` and `chunks_to_delete` existed in the .proto and were
referenced by zero lines of Python, so "replication factor 3, fault tolerant"
was true only until the first node died. This module makes it true.

Design follows the paper (§4.3, §4.4): the master does not push work to chunk
servers. It keeps a priority queue of chunks needing attention and hands out
instructions by **piggybacking on the heartbeat reply**, which is the message
that was going to be sent anyway. A chunk server that is down or slow simply
does not ask for work, and nothing has to time out to discover that.

Who copies the bytes matters too: the **target pulls** from a source using the
ordinary ReadChunk path, rather than the master pushing or a source pushing.
The target is the node with spare capacity by definition (it is the one being
given more data), and a pull means a dead target wastes nobody's bandwidth.
See ADR-003.
"""

import threading
import time
from typing import Callable, Dict, List, Optional, Set, Tuple


class ReplicationManager:
    """Decides which chunks need copying or deleting, and to whom.

    Deliberately holds no gRPC objects and no master reference -- it is fed
    plain facts and returns plain decisions, so the simulation tests in
    Phase 2 can drive it directly without a cluster.
    """

    def __init__(self, replication_factor: int = 3,
                 max_in_flight_per_server: int = 2,
                 max_in_flight_cluster: int = 8,
                 clone_timeout: float = 120.0,
                 logger=None):
        self.rf = replication_factor
        self.max_per_server = max_in_flight_per_server
        self.max_cluster = max_in_flight_cluster
        self.clone_timeout = clone_timeout
        self.logger = logger

        self._lock = threading.RLock()

        # chunk_handle -> (target_server_id, deadline)
        self._in_flight: Dict[str, Tuple[str, float]] = {}

        # server_id -> set of chunk handles that server should delete
        self._pending_deletes: Dict[str, Set[str]] = {}

        self.stats = {
            'tasks_issued': 0,
            'clones_completed': 0,
            'clones_timed_out': 0,
            'deletes_issued': 0,
        }

    # ------------------------------------------------------------ planning
    def under_replicated(self, chunks: dict, live_servers: Set[str]) -> List[Tuple[str, int, Set[str]]]:
        """Chunks with fewer than `rf` live replicas, most urgent first.

        Ordering is by replica count ascending: a chunk down to its last copy
        is one disk failure from being gone forever, and must be copied before
        a chunk that merely dropped from 3 to 2. Getting this ordering wrong
        is the difference between losing data and not.
        """
        out = []
        for handle, chunk in chunks.items():
            alive = {s for s in chunk.locations if s in live_servers}
            if 0 < len(alive) < self.rf:
                out.append((handle, len(alive), alive))
            elif not alive and chunk.locations:
                # Every replica is on a down server. Nothing to copy *from*,
                # so this is unrecoverable until one comes back -- log it
                # rather than queueing a task that can never run.
                if self.logger:
                    self.logger.error(
                        f'chunk {handle} has no live replica; '
                        f'unavailable until one of {sorted(chunk.locations)} returns')
        out.sort(key=lambda t: t[1])
        return out

    def choose_target(self, handle: str, current: Set[str],
                      servers: dict, live_servers: Set[str]) -> Optional[str]:
        """Pick where the next replica of `handle` should go.

        Most free space wins, among live servers that do not already hold it
        and are not already busy with their quota of clones.
        """
        busy = self._busy_counts()
        candidates = [
            sid for sid in live_servers
            if sid not in current
            and sid in servers
            and busy.get(sid, 0) < self.max_per_server
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: getattr(servers[s], 'available_space', 0))

    def _busy_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for _, (target, _) in self._in_flight.items():
            counts[target] = counts.get(target, 0) + 1
        return counts

    def plan(self, chunks: dict, servers: dict, live_servers: Set[str]) -> int:
        """Queue clone work for everything under-replicated. Returns how many
        new tasks were scheduled."""
        with self._lock:
            self._expire_stale()
            scheduled = 0
            for handle, _, alive in self.under_replicated(chunks, live_servers):
                if len(self._in_flight) >= self.max_cluster:
                    break
                if handle in self._in_flight:
                    continue
                target = self.choose_target(handle, alive, servers, live_servers)
                if target is None:
                    continue
                self._in_flight[handle] = (target, time.time() + self.clone_timeout)
                scheduled += 1
            return scheduled

    def _expire_stale(self):
        """Drop clone assignments that never completed.

        Without this a target that dies mid-clone holds its chunk's slot
        forever and that chunk is never re-queued -- the queue silently stops
        making progress while still looking busy.
        """
        now = time.time()
        for handle, (target, deadline) in list(self._in_flight.items()):
            if now > deadline:
                del self._in_flight[handle]
                self.stats['clones_timed_out'] += 1
                if self.logger:
                    self.logger.warning(
                        f'clone of {handle} to {target} timed out; requeueing')

    # --------------------------------------------------------- heartbeat IO
    def tasks_for(self, server_id: str, chunks: dict, servers: dict) -> List[dict]:
        """Clone instructions to piggyback on this server's heartbeat reply."""
        with self._lock:
            out = []
            for handle, (target, _) in self._in_flight.items():
                if target != server_id:
                    continue
                chunk = chunks.get(handle)
                if chunk is None:
                    continue
                sources = [s for s in chunk.locations if s in servers and s != server_id]
                if not sources:
                    continue
                out.append({
                    'chunk_handle': handle,
                    'chunk_version': chunk.version,
                    'sources': [
                        {'server_id': s,
                         'address': servers[s].address,
                         'port': servers[s].port}
                        for s in sources
                    ],
                    'target': {'server_id': server_id,
                               'address': servers[server_id].address,
                               'port': servers[server_id].port},
                })
            self.stats['tasks_issued'] += len(out)
            return out

    def note_replica_present(self, handle: str, server_id: str):
        """A heartbeat reported this chunk present -- the clone landed.

        Completion is learned from the heartbeat rather than acknowledged by
        the chunk server, for the same reason the master never persists
        locations: the chunk server's disk is the authority, and an ack only
        says it *believed* the write succeeded.
        """
        with self._lock:
            pending = self._in_flight.get(handle)
            if pending and pending[0] == server_id:
                del self._in_flight[handle]
                self.stats['clones_completed'] += 1
                if self.logger:
                    self.logger.info(f'replica of {handle} landed on {server_id}')

    # ---------------------------------------------------------- collection
    def plan_deletions(self, reported: Dict[str, Set[str]], chunks: dict) -> int:
        """Queue reclamation of chunks a server holds but the master does not.

        Covers two cases at once:
          * the chunk belongs to a deleted file -- the master dropped it from
            its map, so the server's copy is orphaned
          * the chunk exists but the server's copy is not a recognised
            location, e.g. a stale replica left behind by a partition

        Note this is driven by what servers *report*, not by a master-side
        list. A server that was down when a file was deleted still gets the
        instruction the first time it heartbeats again, with no bookkeeping
        kept for it in the meantime. See ADR-004.
        """
        with self._lock:
            queued = 0
            for server_id, held in reported.items():
                orphans = {h for h in held if h not in chunks}
                if orphans:
                    self._pending_deletes.setdefault(server_id, set()).update(orphans)
                    queued += len(orphans)
            self.stats['deletes_issued'] += queued
            return queued

    def deletions_for(self, server_id: str, limit: int = 64) -> List[str]:
        """Handles for this server to reclaim, dequeued as they are handed out."""
        with self._lock:
            pending = self._pending_deletes.get(server_id)
            if not pending:
                return []
            batch = list(pending)[:limit]
            for h in batch:
                pending.discard(h)
            if not pending:
                self._pending_deletes.pop(server_id, None)
            return batch

    # --------------------------------------------------------------- admin
    def snapshot(self) -> dict:
        with self._lock:
            return {
                'in_flight': {h: t for h, (t, _) in self._in_flight.items()},
                'pending_deletes': {s: len(v) for s, v in self._pending_deletes.items()},
                'stats': dict(self.stats),
            }

#!/usr/bin/env python3
"""Simulated cluster: masters, chunk servers and clients over virtual time.

This models the *protocol* in master.py -- the election state machine, lease
granting, heartbeat-driven placement -- rather than importing the real
classes. The real ones are built around threads, gRPC stubs and
`time.time()`, none of which exist here.

That is a real limitation and worth stating plainly: a bug the simulator
finds is a bug in the protocol as modelled, so the next step is always to
confirm it against the real implementation. What the model does buy is the
ability to run ten thousand fault schedules in the time one real cluster
takes to boot, and to replay any of them exactly.

The replication and erasure-coding planners are **not** modelled -- the real
`ReplicationManager` and `ECManager` are imported and driven directly. They
were written to hold no gRPC objects and no master reference precisely so
this would be possible, which is why those decisions are in their ADRs.

See ADR-007.
"""

import os
import sys
from typing import Dict, List, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulation.core import Network, Simulator  # noqa: E402


# --------------------------------------------------------------- messages
class Msg:
    __slots__ = ('kind', 'term', 'sender', 'payload')

    def __init__(self, kind, term=0, sender='', payload=None):
        self.kind = kind
        self.term = term
        self.sender = sender
        self.payload = payload or {}

    def __repr__(self):
        return f'<{self.kind} term={self.term} from={self.sender}>'


FOLLOWER, CANDIDATE, LEADER = 'follower', 'candidate', 'leader'


class SimMaster:
    """The election state machine from master.py, over virtual time.

    Timing constants come from gfs_config so the simulation exercises the
    values actually shipped, not a parallel set that could drift.
    """

    def __init__(self, node_id: str, peers: List[str], sim: Simulator,
                 net: Network, election_min: float, election_max: float,
                 heartbeat_interval: float):
        self.id = node_id
        self.peers = list(peers)
        self.sim = sim
        self.net = net
        self.election_min = election_min
        self.election_max = election_max
        self.heartbeat_interval = heartbeat_interval

        self.state = FOLLOWER
        self.term = 0
        self.voted_for: Optional[str] = None
        self.votes: Set[str] = set()
        self.leader_id: Optional[str] = None
        self.last_heard = 0.0
        # When a majority last confirmed this node is still leader. Used to
        # refuse lease grants from a partitioned leader -- see grant_lease.
        self.last_majority_ack = 0.0
        self.recent_acks: Dict[str, float] = {}
        self.election_timeout = self._random_timeout()

        # What a leader tracks
        self.chunk_servers: Dict[str, float] = {}     # id -> last heartbeat
        self.leases: Dict[str, dict] = {}             # chunk -> lease
        self.committed: Set[str] = set()              # acknowledged writes

        # Populated by SimCluster so catch-up can read peer state directly.
        # A real master does this over SyncMetadata RPCs; modelling the wire
        # format would add nothing to what is being tested here.
        self.cluster_peers: Optional[Dict[str, 'SimMaster']] = None

        net.register(node_id, self.on_message)

    def _random_timeout(self) -> float:
        return self.sim.rng.uniform(self.election_min, self.election_max)

    @property
    def running(self) -> bool:
        return self.net.is_running(self.id)

    # ----------------------------------------------------------- timers
    def tick(self):
        if not self.running:
            return
        if self.state == LEADER:
            self.send_heartbeats()
            return
        if self.sim.clock.now - self.last_heard >= self.election_timeout:
            self.start_election()

    def start_election(self):
        self.state = CANDIDATE
        self.term += 1
        self.voted_for = self.id
        self.votes = {self.id}
        self.leader_id = None
        self.last_heard = self.sim.clock.now
        self.election_timeout = self._random_timeout()
        self.sim.record(f'{self.id} starts election term={self.term}')

        # Mirrors the fix in master.py: a node's own vote can already be a
        # majority, and checking only on vote *arrival* means a single-node
        # cluster campaigns forever.
        if len(self.votes) > (len(self.peers) + 1) // 2:
            self.become_leader()
            return

        for peer in self.peers:
            self.net.send(self.id, peer,
                          Msg('RequestVote', self.term, self.id))

    # Set False to reproduce the pre-fix behaviour the simulator found.
    CATCH_UP_ON_PROMOTION = True

    def become_leader(self):
        if self.state != CANDIDATE:
            return
        self.state = LEADER
        self.leader_id = self.id
        # A fresh leader just won a majority of votes, so it has majority
        # contact as of now.
        self.last_majority_ack = self.sim.clock.now
        self.recent_acks = {}

        if self.CATCH_UP_ON_PROMOTION:
            # Found by the simulator (ADR-007): a master that was down or
            # partitioned while writes were acknowledged could win the next
            # election and serve state missing them. Pull from every
            # reachable peer before serving.
            #
            # This narrows the window; it does not close it. A write
            # acknowledged by the old leader alone, which no surviving peer
            # ever saw, is still gone -- only majority-ack-before-reply
            # fixes that, and that means a real replicated log.
            gained = 0
            for peer_id in self.peers:
                peer = self.cluster_peers.get(peer_id) if self.cluster_peers else None
                if peer is None or not self.net.reachable(self.id, peer_id):
                    continue
                missing = peer.committed - self.committed
                if missing:
                    self.committed |= missing
                    gained += len(missing)
            if gained:
                self.sim.record(
                    f'{self.id} caught up {gained} write(s) before serving')

        self.sim.record(f'{self.id} BECOMES LEADER term={self.term}')
        self.send_heartbeats()

    def send_heartbeats(self):
        for peer in self.peers:
            self.net.send(self.id, peer,
                          Msg('AppendEntries', self.term, self.id))

    # ---------------------------------------------------------- messages
    def on_message(self, src: str, msg: Msg):
        if not self.running:
            return

        # Any message from a higher term means this node is stale.
        if msg.term > self.term:
            self.term = msg.term
            self.state = FOLLOWER
            self.voted_for = None
            self.votes = set()

        if msg.kind == 'RequestVote':
            self._on_request_vote(src, msg)
        elif msg.kind == 'VoteGranted':
            self._on_vote_granted(src, msg)
        elif msg.kind == 'AppendEntries':
            self._on_append_entries(src, msg)
        elif msg.kind == 'AppendEntriesAck':
            self._on_append_ack(src, msg)
        elif msg.kind == 'ChunkHeartbeat':
            self._on_chunk_heartbeat(src, msg)

    def _on_request_vote(self, src, msg):
        grant = (msg.term >= self.term
                 and self.voted_for in (None, src))
        if grant:
            self.voted_for = src
            self.last_heard = self.sim.clock.now
            self.net.send(self.id, src, Msg('VoteGranted', self.term, self.id))

    def _on_vote_granted(self, src, msg):
        if self.state != CANDIDATE or msg.term != self.term:
            return
        self.votes.add(src)
        if len(self.votes) > (len(self.peers) + 1) // 2:
            self.become_leader()

    def _on_append_entries(self, src, msg):
        if msg.term >= self.term:
            self.term = msg.term
            self.state = FOLLOWER
            self.leader_id = src
            self.last_heard = self.sim.clock.now
            self.net.send(self.id, src,
                          Msg('AppendEntriesAck', self.term, self.id))

    def _on_append_ack(self, src, msg):
        if self.state != LEADER or msg.term != self.term:
            return
        now = self.sim.clock.now
        self.recent_acks[src] = now
        # A majority includes this node itself, so it needs acks from
        # ceil(n/2) peers out of n total.
        fresh = sum(1 for t in self.recent_acks.values()
                    if now - t <= self.LEADER_LEASE_WINDOW)
        if fresh + 1 > (len(self.peers) + 1) // 2:
            self.last_majority_ack = now

    def _on_chunk_heartbeat(self, src, msg):
        self.chunk_servers[src] = self.sim.clock.now

    # ------------------------------------------------------------ leases
    # A leader may grant leases only while it has had majority contact
    # within this window. Must be comfortably shorter than the minimum
    # election timeout, so a leader that loses contact stops granting
    # strictly before any replacement can be elected. Found necessary by the
    # simulator -- see ADR-007.
    LEADER_LEASE_WINDOW = 0.4
    ENFORCE_LEADER_LEASE = True

    def grant_lease(self, chunk: str, holder: str, duration: float) -> bool:
        """Grant a write lease, if one is not already outstanding.

        The invariant this must preserve is that two servers never hold a
        lease on the same chunk at the same time -- that is what orders
        concurrent mutations in GFS. Two primaries means two independent
        mutation orders and replicas that silently diverge.
        """
        if self.state != LEADER or not self.running:
            return False
        now = self.sim.clock.now

        # A partitioned leader does not know it has been deposed: nothing
        # reaches it to say so. Without this check it keeps handing out
        # leases while the majority side elects a new leader doing the same
        # thing -- which the simulator reproduced on seed 165 as 240ms of
        # two primaries on one chunk.
        if self.ENFORCE_LEADER_LEASE:
            if now - self.last_majority_ack > self.LEADER_LEASE_WINDOW:
                self.sim.record(
                    f'{self.id} REFUSES lease on {chunk}: no majority contact '
                    f'for {now - self.last_majority_ack:.3f}s')
                return False
        existing = self.leases.get(chunk)
        if existing and existing['expires'] > now:
            return existing['holder'] == holder
        self.leases[chunk] = {'holder': holder, 'expires': now + duration,
                              'term': self.term}
        self.sim.record(f'{self.id} grants lease on {chunk} to {holder} '
                        f'until {now + duration:.3f}')
        return True

    def active_leases(self) -> Dict[str, dict]:
        now = self.sim.clock.now
        return {c: l for c, l in self.leases.items() if l['expires'] > now}

    def crash(self):
        """Lose all volatile state, keep what the oplog would have kept.

        `committed` survives because ADR-001 makes it durable before the
        client is told yes; leases and election state do not, which is the
        whole point of the lease expiry mechanism.
        """
        self.state = FOLLOWER
        self.votes = set()
        self.leader_id = None
        self.leases = {}
        self.chunk_servers = {}
        self.last_heard = self.sim.clock.now
        self.election_timeout = self._random_timeout()


class SimChunkServer:
    def __init__(self, node_id: str, masters: List[str], sim: Simulator,
                 net: Network, heartbeat_interval: float):
        self.id = node_id
        self.masters = list(masters)
        self.sim = sim
        self.net = net
        self.heartbeat_interval = heartbeat_interval
        self.chunks: Set[str] = set()
        net.register(node_id, self.on_message)

    @property
    def running(self) -> bool:
        return self.net.is_running(self.id)

    def tick(self):
        if not self.running:
            return
        for m in self.masters:
            self.net.send(self.id, m,
                          Msg('ChunkHeartbeat', 0, self.id,
                              {'chunks': set(self.chunks)}))

    def on_message(self, src: str, msg: Msg):
        pass


class SimCluster:
    """A whole simulated cluster, wired up and ready to run."""

    def __init__(self, seed: int, n_masters: int = 3, n_chunkservers: int = 4,
                 drop_rate: float = 0.0, min_latency: float = 0.001,
                 max_latency: float = 0.010):
        from gfs_config import (ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX,
                                MASTER_HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL)

        self.sim = Simulator(seed)
        self.net = Network(self.sim, min_latency=min_latency,
                           max_latency=max_latency, drop_rate=drop_rate)

        master_ids = [f'm{i}' for i in range(n_masters)]
        self.masters: Dict[str, SimMaster] = {}
        for mid in master_ids:
            peers = [p for p in master_ids if p != mid]
            self.masters[mid] = SimMaster(
                mid, peers, self.sim, self.net,
                election_min=ELECTION_TIMEOUT_MIN / 1000.0,
                election_max=ELECTION_TIMEOUT_MAX / 1000.0,
                heartbeat_interval=MASTER_HEARTBEAT_INTERVAL / 1000.0)

        self.chunkservers: Dict[str, SimChunkServer] = {}
        for i in range(n_chunkservers):
            cid = f'cs{i}'
            self.chunkservers[cid] = SimChunkServer(
                cid, master_ids, self.sim, self.net,
                heartbeat_interval=HEARTBEAT_INTERVAL)

        for m in self.masters.values():
            m.cluster_peers = self.masters

        # Timers. Jitter on the master tick is what prevents every node from
        # timing out in lockstep and splitting the vote forever.
        for m in self.masters.values():
            self.sim.every(m.heartbeat_interval, m.tick, f'tick-{m.id}',
                           jitter=m.heartbeat_interval * 0.1)
        for c in self.chunkservers.values():
            self.sim.every(c.heartbeat_interval, c.tick, f'tick-{c.id}',
                           jitter=c.heartbeat_interval * 0.1)

    # ------------------------------------------------------------ queries
    def leaders(self) -> List[SimMaster]:
        return [m for m in self.masters.values()
                if m.state == LEADER and m.running]

    def leaders_in_term(self, term: int) -> List[SimMaster]:
        return [m for m in self.masters.values() if m.state == LEADER
                and m.term == term]

    def run(self, duration: float, on_step=None):
        return self.sim.run_until(self.sim.clock.now + duration, on_step)

#!/usr/bin/env python3

import grpc
from concurrent import futures
import threading
import time
import logging
import json
import uuid
import os
import random
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict
from enum import Enum

import gfs_pb2
import gfs_pb2_grpc
from oplog import (
    OperationLog, CheckpointStore,
    OP_CREATE_FILE, OP_DELETE_FILE, OP_CREATE_DIR, OP_DELETE_DIR, OP_MOVE,
    OP_ALLOCATE_CHUNK, OP_UPDATE_CHUNK_SIZE, OP_UPDATE_FILE_SIZE,
    OP_BUMP_CHUNK_VERSION,
)
from gfs_config import (
    OPLOG_MODE, OPLOG_GROUP_COMMIT_MS, CHECKPOINT_EVERY_OPS, CHECKPOINT_KEEP,
    MAX_CLONES_IN_FLIGHT_PER_SERVER, MAX_CLONES_IN_FLIGHT_CLUSTER,
    REPLICATION_SCAN_INTERVAL, STORAGE_MODE, EC_DATA_SHARDS, EC_PARITY_SHARDS,
    EC_WARM_AFTER_SECONDS, LEADER_LEASE_WINDOW,
)
from replication import ReplicationManager
from ec_manager import ECManager

# Configuration constants live in gfs_config so that every process in the
# cluster -- and the benchmark harness -- reads the same knobs. See ADR-008.
from gfs_config import (
    CHUNK_SIZE,
    REPLICATION_FACTOR,
    HEARTBEAT_INTERVAL,
    HEARTBEAT_MISS_LIMIT,
    LEASE_TIMEOUT,
    ELECTION_TIMEOUT_MIN,
    ELECTION_TIMEOUT_MAX,
    MASTER_HEARTBEAT_INTERVAL,
    INITIAL_ELECTION_TIMEOUT,
    DEFAULT_MAX_WORKERS,
    GRPC_SERVER_OPTIONS,
    GRPC_CHANNEL_OPTIONS,
)

# Path constants
PATH_SEPARATOR = '/'
ROOT_PATH = '/'
MAX_PATH_LENGTH = 4096
MAX_FILENAME_LENGTH = 255

class MasterState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

@dataclass
class ChunkMetadata:
    handle: str
    version: int
    locations: Set[str]
    primary: Optional[str] = None
    lease_expiry: float = 0
    size: int = 0
    checksum: Optional[str] = None
    # When this chunk was last written. The hot/warm split in ADR-005 needs
    # it: a chunk still being appended to must never be pulled into an
    # encode. Defaults to 0 so a chunk recovered from an old checkpoint
    # without the field is treated as cold, which is the safe direction --
    # the eligibility check also requires full replication.
    last_modified: float = 0.0

@dataclass
class FileMetadata:
    filename: str
    chunks: List[str]
    size: int
    created_time: float
    modified_time: float

@dataclass
class DirectoryMetadata:
    dirname: str
    created_time: float
    modified_time: float
    parent_dir: Optional[str] = None

@dataclass
class ChunkServerInfo:
    server_id: str
    address: str
    port: int
    last_heartbeat: float
    available_space: int
    chunks: Set[str]

@dataclass
class MasterPeer:
    master_id: str
    address: str
    port: int
    last_heartbeat: float = 0
    term: int = 0
    is_alive: bool = True

@dataclass
class ChunkLease:
    """Represents a lease granted to a primary replica"""
    chunk_handle: str
    primary_server_id: str
    lease_id: str
    granted_time: float
    expiry_time: float
    version: int = 1

class MultiMasterGFS:
    def __init__(self, master_id: str = None, port: int = 9000, 
                 metadata_file: str = None, peers: List[str] = None):
        self.master_id = master_id or f"master_{uuid.uuid4().hex[:8]}"
        self.port = port
        self.metadata_file = metadata_file or f'master_metadata_{self.master_id}.json'
        
        # Initialize logger with more detailed formatting
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(f"Master-{self.master_id}")
        
        # Log startup information
        self.logger.info(f"Starting Multi-Master GFS node: {self.master_id} on port {port}")
        self.logger.info(f"Peers configured: {peers}")
        
        # Raft-like consensus state
        self.current_term = 0
        self.voted_for = None
        self.state = MasterState.FOLLOWER
        self.leader_id = None
        self.votes_received = set()
        self.metadata_version = 0
        
        # When a majority of peers last confirmed this node is still the
        # leader. A partitioned leader has no way to learn it was deposed --
        # nothing reaches it to say so -- so lease grants are gated on this.
        # Found necessary by the simulator (ADR-007).
        self.last_majority_ack = 0.0
        self.peer_acks: Dict[str, float] = {}

        # Election timing
        self.last_heartbeat_time = time.time()
        self.election_timeout = self._get_initial_election_timeout()  # Start with long timeout
        self.has_connected_to_cluster = False  # Track if we've connected to existing cluster
        
        # Peer management
        self.peers: Dict[str, MasterPeer] = {}
        self.peer_stubs: Dict[str, gfs_pb2_grpc.MasterElectionServiceStub] = {}
        
        if peers:
            for peer_addr in peers:
                if ':' in peer_addr:
                    addr, port_str = peer_addr.split(':')
                    peer_port = int(port_str)
                else:
                    addr = peer_addr
                    peer_port = 9000
                
                peer_id = f"master_{addr}_{peer_port}"
                self.peers[peer_id] = MasterPeer(
                    master_id=peer_id,
                    address=addr,
                    port=peer_port
                )
                self.logger.info(f"Added peer: {peer_id} at {addr}:{peer_port}")
        
        # Metadata storage
        self.files: Dict[str, FileMetadata] = {}
        self.directories: Dict[str, DirectoryMetadata] = {}
        self.chunks: Dict[str, ChunkMetadata] = {}
        self.chunk_servers: Dict[str, ChunkServerInfo] = {}
        
        # Thread safety
        self.metadata_lock = threading.RLock()
        self.lease_lock = threading.RLock()
        self.state_lock = threading.RLock()
        
        # Lease management - critical for GFS write coordination
        self.chunk_leases: Dict[str, ChunkLease] = {}  # chunk_handle -> ChunkLease
        self.lease_duration = 60.0  # 60 seconds lease duration
        
        # Background tasks
        self.running = False
        self.background_thread = None
        self.election_thread = None
        self.heartbeat_thread = None
        
        # Re-replication and chunk GC (ADR-003, ADR-004). The proto has
        # carried ReplicationTask and chunks_to_delete from the start; this is
        # what finally issues them.
        self.replication = ReplicationManager(
            replication_factor=REPLICATION_FACTOR,
            max_in_flight_per_server=MAX_CLONES_IN_FLIGHT_PER_SERVER,
            max_in_flight_cluster=MAX_CLONES_IN_FLIGHT_CLUSTER,
            logger=self.logger)

        # Erasure coding (ADR-005). Only converts anything when
        # GFS_STORAGE_MODE=ec; otherwise it tracks nothing and costs nothing,
        # which is what makes the A/B benchmark a fair comparison.
        self.ec_enabled = (STORAGE_MODE == 'ec')
        self.ec = ECManager(data_shards=EC_DATA_SHARDS,
                            parity_shards=EC_PARITY_SHARDS,
                            warm_after=EC_WARM_AFTER_SECONDS,
                            logger=self.logger)

        # Durability: operation log + checkpoints (ADR-001)
        self._init_durability()

        # Rebuild state from the newest checkpoint plus the log tail
        self.recover()
        
        # Ensure root directory exists
        self._ensure_root_directory()

    def _get_random_election_timeout(self) -> float:
        """Get random election timeout to avoid split votes"""
        return (ELECTION_TIMEOUT_MIN + 
                random.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN)) / 1000.0

    def _get_initial_election_timeout(self) -> float:
        """Get initial long election timeout for startup discovery"""
        return INITIAL_ELECTION_TIMEOUT / 1000.0

    def _switch_to_normal_timeout(self):
        """Switch from initial long timeout to normal election timeout"""
        if not self.has_connected_to_cluster:
            self.has_connected_to_cluster = True
            self.election_timeout = self._get_random_election_timeout()
            self.logger.info(f"Connected to cluster, switching to normal election timeout: {self.election_timeout:.1f}s")

    def connect_to_peer(self, peer_id: str) -> Optional[gfs_pb2_grpc.MasterElectionServiceStub]:
        """Establish connection to a peer master"""
        if peer_id not in self.peers:
            return None
            
        if peer_id not in self.peer_stubs:
            try:
                peer = self.peers[peer_id]
                channel = grpc.insecure_channel(f"{peer.address}:{peer.port}", options=GRPC_CHANNEL_OPTIONS)
                stub = gfs_pb2_grpc.MasterElectionServiceStub(channel)
                self.peer_stubs[peer_id] = stub
                self.logger.info(f"Connected to peer {peer_id}")
            except Exception as e:
                self.logger.error(f"Failed to connect to peer {peer_id}: {e}")
                return None
        
        return self.peer_stubs[peer_id]

    def discover_existing_leader(self):
        """
        Check if there's already a leader in the cluster at startup.
        If found, become a follower. Returns True if leader found.
        """
        self.logger.info("Discovering existing leader...")
        
        for peer_id in self.peers:
            try:
                stub = self.connect_to_peer(peer_id)
                if not stub:
                    continue
                
                # Send a heartbeat request to check if peer is a leader
                request = gfs_pb2.MasterHeartbeatRequest(
                    master_id=self.master_id,
                    term=self.current_term,
                    is_primary=False,  # We're not claiming to be primary
                    metadata_version=self.metadata_version
                )
                
                response = stub.SendHeartbeat(request, timeout=2.0)
                
                if response.success and response.acknowledge_primary:
                    # This peer is a leader and acknowledged us
                    with self.state_lock:
                        if response.term >= self.current_term:
                            self.current_term = response.term
                            self.state = MasterState.FOLLOWER
                            self.leader_id = peer_id
                            self.voted_for = None
                            self.last_heartbeat_time = time.time()
                            
                            # FIXED: Switch to normal timeout after connecting to cluster
                            self._switch_to_normal_timeout()
                            
                            self.logger.info(f"Found existing leader: {peer_id} with term {response.term}")
                            return True
                        
            except Exception as e:
                self.logger.debug(f"Failed to contact peer {peer_id} during leader discovery: {e}")
                continue
        
        self.logger.info("No existing leader found, will participate in election")
        return False

    def start_election(self):
        """Start leader election process"""
        with self.state_lock:
            if self.state == MasterState.LEADER:
                return
                
            self.state = MasterState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.master_id
            self.votes_received = {self.master_id}
            self.leader_id = None
            
            self.logger.info(f"Starting election for term {self.current_term}")

            # A node's own vote can already be a majority. The majority test
            # otherwise only runs when a peer's vote *comes back*, so a
            # single-master deployment -- no peers, nothing to come back --
            # campaigned forever and never served a request. Same arithmetic
            # as in _request_vote_from_peer, applied to the self-vote.
            total_nodes = len(self.peers) + 1
            if len(self.votes_received) > total_nodes // 2:
                self.logger.info(
                    f"Won election for term {self.current_term} on own vote "
                    f"({len(self.votes_received)}/{total_nodes})")
                threading.Thread(target=self.become_leader, daemon=True).start()
                return

            # Request votes from all peers
            for peer_id in self.peers:
                threading.Thread(target=self._request_vote_from_peer, 
                               args=(peer_id,), daemon=True).start()
    
    def _request_vote_from_peer(self, peer_id: str):
        """Request vote from a specific peer"""
        try:
            stub = self.connect_to_peer(peer_id)
            if not stub:
                return
                
            request = gfs_pb2.ElectionRequest(
                candidate_id=self.master_id,
                term=self.current_term,
                last_log_index=self.metadata_version,
                last_log_term=self.current_term
            )
            
            response = stub.RequestVote(request, timeout=1.0)
            
            with self.state_lock:
                if response.term > self.current_term:
                    self.current_term = response.term
                    self.voted_for = None
                    self.state = MasterState.FOLLOWER
                    return
                
                if (response.vote_granted and 
                    response.term == self.current_term and 
                    self.state == MasterState.CANDIDATE):
                    
                    self.votes_received.add(peer_id)
                    
                    # Check if we have majority
                    total_nodes = len(self.peers) + 1  # +1 for self
                    if len(self.votes_received) > total_nodes // 2:
                        self.become_leader()
                        
        except Exception as e:
            self.logger.debug(f"Vote request to {peer_id} failed: {e}")

    def become_leader(self):
        """Become the cluster leader"""
        with self.state_lock:
            if self.state != MasterState.CANDIDATE:
                return
                
            self.state = MasterState.LEADER
            self.leader_id = self.master_id
            # Winning an election means majority contact as of now.
            self.last_majority_ack = time.time()
            self.peer_acks = {}

            self.logger.info(f"Became leader for term {self.current_term}")

        # Outside the state lock: this makes RPCs and must not block the
        # election machinery.
        self.catch_up_from_peers()

        with self.state_lock:
            # Send immediate heartbeats to establish leadership
            self.send_heartbeats_to_peers()

    def catch_up_from_peers(self):
        """Adopt any peer's metadata that is newer than ours, before serving.

        A master that was down or partitioned while writes were acknowledged
        can win the next election holding stale state, and would then serve
        a namespace missing those writes. The simulator reproduced this on
        7.6% of fault schedules before the fix (ADR-007).

        This narrows the window rather than closing it. A write acknowledged
        by the old leader alone, which no surviving peer ever received, is
        still lost -- only majority-ack-before-reply fixes that, and that
        means a genuine replicated log. Stated plainly because the
        difference matters: this is not consensus.
        """
        best_peer, best_version = None, self.metadata_version
        for peer_id in list(self.peers):
            try:
                stub = self.connect_to_peer(peer_id)
                if not stub:
                    continue
                resp = stub.SendHeartbeat(gfs_pb2.MasterHeartbeatRequest(
                    master_id=self.master_id, term=self.current_term,
                    is_primary=True, metadata_version=self.metadata_version),
                    timeout=2.0)
                if resp.success and resp.metadata_version > best_version:
                    best_peer, best_version = peer_id, resp.metadata_version
            except Exception as e:
                self.logger.debug(f'catch-up probe of {peer_id} failed: {e}')

        if not best_peer:
            self.logger.info(
                f'catch-up: no peer newer than version {self.metadata_version}')
            return

        try:
            stub = self.connect_to_peer(best_peer)
            resp = stub.FetchMetadata(
                gfs_pb2.FetchMetadataRequest(requester_id=self.master_id),
                timeout=10.0)
            if not resp.success or resp.version <= self.metadata_version:
                return
            self.apply_metadata_blob(resp.metadata_json, resp.version)
            self.logger.info(
                f'caught up from {best_peer}: version '
                f'{self.metadata_version} -> {resp.version}')
        except Exception as e:
            # Serving slightly stale state is bad; refusing to serve at all
            # is worse. Log loudly and carry on -- the periodic sync will
            # converge things shortly.
            self.logger.error(
                f'catch-up from {best_peer} failed ({e}); serving possibly '
                f'stale metadata at version {self.metadata_version}')

    def apply_metadata_blob(self, metadata_json: str, version: int):
        """Adopt a peer's serialized metadata wholesale."""
        data = json.loads(metadata_json)
        with self.metadata_lock:
            self.files = {k: FileMetadata(**v)
                          for k, v in data.get('files', {}).items()}
            self.directories = {k: DirectoryMetadata(**v)
                                for k, v in data.get('directories', {}).items()}
            self.chunks = {}
            for handle, cd in data.get('chunks', {}).items():
                cd = dict(cd)
                # Locations are rebuilt from heartbeats, never adopted from a
                # peer's view of them (ADR-001).
                cd['locations'] = set()
                cd.pop('primary', None)
                cd.pop('lease_expiry', None)
                self.chunks[handle] = ChunkMetadata(**cd)
            self.metadata_version = version
            self.persist_synced_state()

    def send_heartbeats_to_peers(self):
        """Send heartbeats to all peers as leader"""
        if self.state != MasterState.LEADER:
            return
            
        for peer_id in self.peers:
            threading.Thread(target=self._send_heartbeat_to_peer, 
                           args=(peer_id,), daemon=True).start()

    def _send_heartbeat_to_peer(self, peer_id: str):
        """Send heartbeat to a specific peer"""
        try:
            stub = self.connect_to_peer(peer_id)
            if not stub:
                return
                
            request = gfs_pb2.MasterHeartbeatRequest(
                master_id=self.master_id,
                term=self.current_term,
                is_primary=True,
                metadata_version=self.metadata_version
            )
            
            response = stub.SendHeartbeat(request, timeout=1.0)

            with self.state_lock:
                if response.term > self.current_term:
                    self.current_term = response.term
                    self.voted_for = None
                    self.state = MasterState.FOLLOWER
                    self.leader_id = None
                    return

                # A peer answered, so this node is still in contact with it.
                if response.success and self.state == MasterState.LEADER:
                    now = time.time()
                    self.peer_acks[peer_id] = now
                    fresh = sum(1 for t in self.peer_acks.values()
                                if now - t <= LEADER_LEASE_WINDOW)
                    if fresh + 1 > (len(self.peers) + 1) // 2:
                        self.last_majority_ack = now
                    
        except Exception as e:
            self.logger.debug(f"Heartbeat to {peer_id} failed: {e}")

    def sync_metadata_to_peers(self):
        """Synchronize metadata to backup masters"""
        if self.state != MasterState.LEADER:
            return
            
        metadata_json = self._serialize_metadata()
        
        for peer_id in self.peers:
            threading.Thread(target=self._sync_metadata_to_peer, 
                           args=(peer_id, metadata_json), daemon=True).start()

    def _sync_metadata_to_peer(self, peer_id: str, metadata_json: str):
        """Sync metadata to a specific peer"""
        try:
            stub = self.connect_to_peer(peer_id)
            if not stub:
                return
                
            request = gfs_pb2.SyncMetadataRequest(
                master_id=self.master_id,
                term=self.current_term,
                metadata_json=metadata_json,
                version=self.metadata_version
            )
            
            response = stub.SyncMetadata(request, timeout=5.0)
            
            if not response.success:
                self.logger.warning(f"Failed to sync metadata to {peer_id}: {response.message}")
                
        except Exception as e:
            self.logger.debug(f"Metadata sync to {peer_id} failed: {e}")

    def _serialize_metadata(self) -> str:
        """Serialize current metadata to JSON"""
        with self.metadata_lock:
            data = {
                'files': {k: asdict(v) for k, v in self.files.items()},
                'directories': {k: asdict(v) for k, v in self.directories.items()},
                'chunks': {},
                'version': self.metadata_version,
                'term': self.current_term
            }
            
            # Convert sets to lists for JSON serialization
            for handle, chunk in self.chunks.items():
                chunk_dict = asdict(chunk)
                chunk_dict['locations'] = list(chunk_dict['locations'])
                data['chunks'][handle] = chunk_dict
            
            return json.dumps(data)

    def election_timeout_loop(self):
        """Background loop for election timeout management"""
        while self.running:
            time.sleep(0.01)  # Check every 10ms
            
            with self.state_lock:
                if self.state == MasterState.LEADER:
                    continue
                    
                time_since_heartbeat = time.time() - self.last_heartbeat_time
                
                if time_since_heartbeat > self.election_timeout:
                    self.logger.info(f"Election timeout, starting election")
                    self.start_election()
                    self.last_heartbeat_time = time.time()
                    self.election_timeout = self._get_random_election_timeout()

    def master_heartbeat_loop(self):
        """Background loop for sending leader heartbeats"""
        while self.running:
            time.sleep(MASTER_HEARTBEAT_INTERVAL / 1000.0)
            
            with self.state_lock:
                if self.state == MasterState.LEADER:
                    self.send_heartbeats_to_peers()

    def is_leader(self) -> bool:
        """Check if this master is the current leader"""
        with self.state_lock:
            return self.state == MasterState.LEADER

    def get_leader_info(self) -> Optional[Dict]:
        """Get information about the current leader"""
        with self.state_lock:
            if self.leader_id:
                if self.leader_id == self.master_id:
                    return {
                        'master_id': self.master_id,
                        'address': 'localhost',
                        'port': self.port,
                        'term': self.current_term
                    }
                elif self.leader_id in self.peers:
                    peer = self.peers[self.leader_id]
                    return {
                        'master_id': peer.master_id,
                        'address': peer.address,
                        'port': peer.port,
                        'term': self.current_term
                    }
            return None


    # ======================================================================
    # Durability: operation log + checkpoints (ADR-001, ADR-002)
    # ======================================================================
    # The original master dumped all metadata to JSON every 30 seconds from a
    # background thread, so a crash lost up to 30s of acknowledged operations.
    # Here every mutation is appended to an fsync'd log before the client is
    # told it succeeded, and checkpoints keep replay bounded.

    def _init_durability(self):
        """Open the log and checkpoint store. Called from __init__."""
        self.data_dir = os.path.dirname(os.path.abspath(self.metadata_file)) or '.'
        os.makedirs(self.data_dir, exist_ok=True)

        self.durable = (OPLOG_MODE == 'oplog')
        self.checkpoint_number = 0
        self.oplog = None
        self.checkpoints = None

        if self.durable:
            self.oplog = OperationLog(
                os.path.join(self.data_dir, 'oplog.bin'),
                group_commit_ms=OPLOG_GROUP_COMMIT_MS,
                logger=self.logger)
            self.checkpoints = CheckpointStore(
                self.data_dir, keep=CHECKPOINT_KEEP, logger=self.logger)
        self._checkpoint_lock = threading.Lock()


    # ======================================================================
    # Erasure coding cycle (ADR-005)
    # ======================================================================

    def _run_ec_cycle(self, live: Set[str]):
        """Convert cold chunks, repair degraded ones, retire superseded
        replicas. Called from the background loop, leader only."""
        # Replicas of chunks whose full shard set is now confirmed. Doing
        # this first means the space is reclaimed before more encodes start.
        for handle in self.ec.take_replicas_to_drop():
            chunk = self.chunks.get(handle)
            if chunk is None:
                continue
            st = self.ec.state(handle)
            if st is None or len(st.shards) < st.n:
                # Should not happen, but dropping replicas on a partial
                # shard set is the one unrecoverable mistake here.
                self.logger.error(
                    f'refusing to drop replicas of {handle}: shard set incomplete')
                continue
            freed = chunk.locations
            chunk.locations = set()

            # Clearing master-side locations is not enough: the bytes are
            # still on those disks, and the ordinary GC diff will never flag
            # them because the master *does* still know this chunk -- it is
            # erasure-coded now, not deleted. Without this the conversion
            # adds 1.5x of shards on top of 3x of replicas and makes storage
            # worse, which is the exact opposite of the point.
            for server_id in freed:
                self.replication._pending_deletes.setdefault(
                    server_id, set()).add(handle)
            self.logger.info(
                f'🗑️  retiring {len(freed)} whole replica(s) of {handle} '
                f'now that {st.n} shards are placed')

        n_enc = self.ec.plan_encodes(self.chunks, self.chunk_servers, live)
        n_rep = self.ec.plan_repairs(self.chunk_servers, live)
        if n_enc or n_rep:
            self.logger.info(
                f'EC cycle: {n_enc} encode(s), {n_rep} shard repair(s) scheduled')

    def live_chunk_servers(self) -> Set[str]:
        """Servers that have heartbeated recently enough to be trusted.

        The old code spelled this `HEARTBEAT_INTERVAL * 2` inline in four
        places. With a 30s interval that meant a dead server stayed 'live'
        for a full minute. Named and configurable now -- see ADR-008.
        """
        cutoff = time.time() - (HEARTBEAT_INTERVAL * HEARTBEAT_MISS_LIMIT)
        return {sid for sid, s in self.chunk_servers.items()
                if s.last_heartbeat >= cutoff}

    def commit(self, op: str, **fields):
        """Make a mutation durable, then propagate it.

        Call this *after* mutating in-memory state and *before* replying to
        the client. The ordering is the whole guarantee: when the client sees
        success, the record is already fsync'd.
        """
        self.metadata_version += 1
        if self.durable:
            self.oplog.append(op, version=self.metadata_version, **fields)
            self._maybe_checkpoint()
        else:
            # Legacy snapshot mode, kept so the benchmark suite can measure
            # what the operation log actually bought us.
            self.save_metadata()
        self.sync_metadata_to_peers()

    def apply_record(self, rec: dict):
        """Re-apply one logged record to in-memory state during recovery.

        Records store the *effect* of an operation (which paths changed), not
        the request that caused it. That means replay never re-runs path
        validation or placement logic, so recovery cannot diverge from the
        original execution just because a config value changed in between.
        """
        op = rec.get('op')

        if op == OP_CREATE_FILE:
            t = rec.get('created_time', rec.get('ts', 0))
            self.files[rec['filename']] = FileMetadata(
                filename=rec['filename'], chunks=[], size=0,
                created_time=t, modified_time=t)

        elif op == OP_DELETE_FILE:
            for name in rec.get('filenames', []):
                fm = self.files.pop(name, None)
                if fm:
                    for h in fm.chunks:
                        self.chunks.pop(h, None)

        elif op == OP_CREATE_DIR:
            t = rec.get('created_time', rec.get('ts', 0))
            self.directories[rec['dirname']] = DirectoryMetadata(
                dirname=rec['dirname'], created_time=t, modified_time=t,
                parent_dir=rec.get('parent_dir'))

        elif op == OP_DELETE_DIR:
            for name in rec.get('filenames', []):
                fm = self.files.pop(name, None)
                if fm:
                    for h in fm.chunks:
                        self.chunks.pop(h, None)
            for name in rec.get('dirnames', []):
                self.directories.pop(name, None)

        elif op == OP_MOVE:
            t = rec.get('ts', 0)
            for src, dst in rec.get('files', []):
                fm = self.files.pop(src, None)
                if fm:
                    fm.filename = dst
                    fm.modified_time = t
                    self.files[dst] = fm
            for src, dst in rec.get('dirs', []):
                dm = self.directories.pop(src, None)
                if dm:
                    dm.dirname = dst
                    dm.modified_time = t
                    self.directories[dst] = dm

        elif op == OP_ALLOCATE_CHUNK:
            handle = rec['chunk_handle']
            self.chunks[handle] = ChunkMetadata(
                handle=handle,
                version=rec.get('chunk_version', 1),
                locations=set(),   # rebuilt from heartbeats, never logged
                size=0)
            fm = self.files.get(rec['filename'])
            if fm is not None:
                idx = rec.get('chunk_index', len(fm.chunks))
                while len(fm.chunks) <= idx:
                    fm.chunks.append(None)
                fm.chunks[idx] = handle

        elif op == OP_UPDATE_CHUNK_SIZE:
            c = self.chunks.get(rec['chunk_handle'])
            if c is not None:
                c.size = rec['size']
                c.last_modified = rec.get('ts', c.last_modified)
            owner = rec.get('filename')
            if owner and rec.get('file_size') is not None:
                fm = self.files.get(owner)
                if fm is not None:
                    fm.size = rec['file_size']

        elif op == OP_UPDATE_FILE_SIZE:
            fm = self.files.get(rec['filename'])
            if fm is not None:
                fm.size = rec['size']
                fm.modified_time = rec.get('modified_time', rec.get('ts', fm.modified_time))

        elif op == OP_BUMP_CHUNK_VERSION:
            c = self.chunks.get(rec['chunk_handle'])
            if c is not None:
                c.version = rec['chunk_version']

        else:
            self.logger.warning(f'recovery: ignoring unknown op {op!r}')

        v = rec.get('version')
        if isinstance(v, int) and v > self.metadata_version:
            self.metadata_version = v

    def _snapshot_state(self) -> dict:
        """Serialize everything a checkpoint must carry.

        Replica locations are excluded on purpose: the chunk servers are the
        authority on what they actually hold, and the master rebuilds the
        location map from heartbeats within one heartbeat interval of
        starting. A logged location is a lie as soon as a disk dies.
        """
        return {
            'files': {k: asdict(v) for k, v in self.files.items()},
            'directories': {k: asdict(v) for k, v in self.directories.items()},
            'chunks': {
                h: {'handle': c.handle, 'version': c.version,
                    'size': c.size, 'checksum': c.checksum}
                for h, c in self.chunks.items()
            },
            'version': self.metadata_version,
            'term': self.current_term,
        }

    def _restore_state(self, snap: dict):
        self.files.clear()
        self.directories.clear()
        self.chunks.clear()
        for name, d in snap.get('files', {}).items():
            self.files[name] = FileMetadata(**d)
        for name, d in snap.get('directories', {}).items():
            self.directories[name] = DirectoryMetadata(**d)
        for handle, d in snap.get('chunks', {}).items():
            self.chunks[handle] = ChunkMetadata(
                handle=d['handle'], version=d.get('version', 1),
                locations=set(), size=d.get('size', 0),
                checksum=d.get('checksum'))
        self.metadata_version = snap.get('version', 0)
        self.current_term = max(self.current_term, snap.get('term', 0))

    def write_checkpoint(self) -> bool:
        """Checkpoint, then truncate the log it covers."""
        if not self.durable:
            return False
        with self._checkpoint_lock:
            with self.metadata_lock:
                snap = self._snapshot_state()
                self.checkpoint_number += 1
                number = self.checkpoint_number
            self.checkpoints.write(number, snap)
            # Safe only because the checkpoint is already fsync'd and renamed.
            self.oplog.reset()
            self.logger.info(
                f'checkpoint {number} written '
                f'({len(snap["files"])} files, {len(snap["chunks"])} chunks); oplog truncated')
            return True

    def persist_synced_state(self):
        """Make state received from the leader durable on a follower.

        A follower does not write operation-log records -- it never saw the
        individual mutations, only the leader's snapshot -- so it checkpoints
        instead. This matters because a follower that wins the next election
        serves this state as authoritative, and losing it on restart would
        silently roll the namespace back.
        """
        if not self.durable:
            self.save_metadata()
            return
        try:
            self.write_checkpoint()
        except Exception as e:
            self.logger.error(f'follower checkpoint failed: {e}')

    def _maybe_checkpoint(self):
        if not self.durable:
            return
        if self.oplog.records_since_checkpoint < CHECKPOINT_EVERY_OPS:
            return
        # Off the request path; a checkpoint must never block a client write.
        t = threading.Thread(target=self._checkpoint_quietly, daemon=True)
        t.start()

    def _checkpoint_quietly(self):
        try:
            self.write_checkpoint()
        except Exception as e:
            self.logger.error(f'checkpoint failed: {e}')

    def recover(self):
        """Rebuild state at startup: newest good checkpoint, then log tail."""
        if not self.durable:
            self.load_metadata()
            return

        discarded = self.oplog.truncate_tail()

        latest = self.checkpoints.latest()
        if latest:
            number, snap = latest
            self.checkpoint_number = number
            self._restore_state(snap)
            self.logger.info(
                f'recovered checkpoint {number}: {len(self.files)} files, '
                f'{len(self.chunks)} chunks, version {self.metadata_version}')
        else:
            # First boot after migrating off the old JSON snapshot: adopt it
            # once, then never read it again.
            self.load_metadata()
            if self.files or self.directories:
                self.logger.info(
                    f'migrated legacy metadata.json: {len(self.files)} files')

        replayed = 0
        for rec in self.oplog.replay(tolerate_tail=True):
            self.apply_record(rec)
            replayed += 1

        self.logger.info(
            f'recovery complete: replayed {replayed} log records'
            + (f', discarded {discarded}B torn tail' if discarded else '')
            + f'; now {len(self.files)} files / {len(self.chunks)} chunks '
              f'at version {self.metadata_version}')

    def load_metadata(self):
        """Load metadata from persistent storage"""
        try:
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'r') as f:
                    data = json.load(f)
                    
                # Restore files metadata
                for filename, file_data in data.get('files', {}).items():
                    self.files[filename] = FileMetadata(**file_data)
                
                # Restore directories metadata (FIXED: Handle missing directories key)
                for dirname, dir_data in data.get('directories', {}).items():
                    self.directories[dirname] = DirectoryMetadata(**dir_data)
                
                # Restore chunks metadata
                for handle, chunk_data in data.get('chunks', {}).items():
                    chunk_data['locations'] = set(chunk_data['locations'])
                    self.chunks[handle] = ChunkMetadata(**chunk_data)
                
                # Restore consensus state
                self.metadata_version = data.get('version', 0)
                self.current_term = data.get('term', 0)
                    
                self.logger.info(f"Loaded metadata: {len(self.files)} files, {len(self.directories)} directories, {len(self.chunks)} chunks")
        except Exception as e:
            self.logger.error(f"Failed to load metadata: {e}")

    def save_metadata(self):
        """Save metadata to persistent storage"""
        try:
            with self.metadata_lock:
                data = {
                    'files': {k: asdict(v) for k, v in self.files.items()},
                    'directories': {k: asdict(v) for k, v in self.directories.items()},
                    'chunks': {},
                    'version': self.metadata_version,
                    'term': self.current_term
                }
                
                # Convert sets to lists for JSON serialization
                for handle, chunk in self.chunks.items():
                    chunk_dict = asdict(chunk)
                    chunk_dict['locations'] = list(chunk_dict['locations'])
                    data['chunks'][handle] = chunk_dict
                
                with open(self.metadata_file, 'w') as f:
                    json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save metadata: {e}")

    def generate_chunk_handle(self) -> str:
        """Generate unique chunk handle"""
        return f"chunk_{uuid.uuid4().hex}"

    def select_chunk_servers(self, exclude: Set[str] = None) -> List[str]:
        """Select chunk servers for new chunk placement"""
        if exclude is None:
            exclude = set()
            
        available_servers = [
            server_id for server_id, server in self.chunk_servers.items()
            if server_id not in exclude and 
            time.time() - server.last_heartbeat < HEARTBEAT_INTERVAL * 2
        ]
        
        # Sort by available space (descending) and select top REPLICATION_FACTOR
        available_servers.sort(
            key=lambda s: self.chunk_servers[s].available_space,
            reverse=True
        )
        
        return available_servers[:REPLICATION_FACTOR]

    def has_leader_lease(self) -> bool:
        """Whether this node may still act as leader for writes.

        A leader that has lost contact with a majority cannot know whether
        it has been deposed, and a replacement may already be granting
        leases on the other side of the partition. Refusing here, strictly
        before any replacement could have been elected, is what keeps the
        'one primary per chunk' invariant true.

        The window must be comfortably shorter than the minimum election
        timeout. With 800ms as the floor there, 400ms leaves a margin the
        other side cannot beat. See ADR-007.
        """
        if self.state != MasterState.LEADER:
            return False
        if not self.peers:
            return True      # single-master: it is trivially the majority
        return (time.time() - self.last_majority_ack) <= LEADER_LEASE_WINDOW

    def grant_lease(self, chunk_handle: str) -> Optional[str]:
        """Grant lease to a chunk server for writing"""
        with self.lease_lock:
            if chunk_handle not in self.chunks:
                return None

            if not self.has_leader_lease():
                stale = time.time() - self.last_majority_ack
                self.logger.warning(
                    f'refusing lease on {chunk_handle}: no majority contact '
                    f'for {stale:.2f}s -- this node may have been deposed')
                return None
                
            chunk = self.chunks[chunk_handle]
            
            # Check if current lease is still valid
            if chunk.primary and time.time() < chunk.lease_expiry:
                return chunk.primary
            
            chunk.version+=1 # this helps in detect stale state
            
            # Select new primary from available replicas
            available_servers = [
                server_id for server_id in chunk.locations
                if server_id in self.chunk_servers and
                time.time() - self.chunk_servers[server_id].last_heartbeat < HEARTBEAT_INTERVAL * 2
            ]
            
            if not available_servers:
                return None
            
            # Grant lease to server with most available space
            primary = max(available_servers, 
                         key=lambda s: self.chunk_servers[s].available_space)
            
            chunk.primary = primary
            chunk.lease_expiry = time.time() + LEASE_TIMEOUT
            # A granted lease means a write is coming, so treat the chunk as
            # hot now rather than after the write lands.
            chunk.last_modified = time.time()
            
            self.logger.info(f"Granted lease for chunk {chunk_handle} to {primary} , version = {chunk.version}")
            return primary

    def background_tasks(self):
        """Background tasks for garbage collection and replication monitoring"""
        while self.running:
            try:
                with self.metadata_lock:
                    # Clean up expired leases
                    current_time = time.time()
                    for chunk in self.chunks.values():
                        if chunk.primary and current_time >= chunk.lease_expiry:
                            chunk.primary = None
                            chunk.lease_expiry = 0
                    
                    # Queue clone work for anything under-replicated. The
                    # tasks go out on the next heartbeat reply to whichever
                    # server was chosen as target. ADR-003.
                    if self.is_leader():
                        live = self.live_chunk_servers()
                        scheduled = self.replication.plan(
                            self.chunks, self.chunk_servers, live)
                        if scheduled:
                            self.logger.info(
                                f'scheduled {scheduled} re-replication task(s)')

                        if self.ec_enabled:
                            self._run_ec_cycle(live)
                
                # Save metadata periodically (only if leader)
                if self.is_leader():
                    # Durability is handled per-mutation by the operation log now.
                    # This loop only rolls checkpoints and re-syncs peers. ADR-001.
                    if self.durable:
                        if self.oplog.records_since_checkpoint >= CHECKPOINT_EVERY_OPS:
                            self._checkpoint_quietly()
                    else:
                        self.save_metadata()
                    self.sync_metadata_to_peers()
                
            except Exception as e:
                self.logger.error(f"Background task error: {e}")
            
            time.sleep(REPLICATION_SCAN_INTERVAL)

    def start_background_tasks(self):
        """Start background tasks"""
        self.running = True
        self.background_thread = threading.Thread(target=self.background_tasks)
        self.background_thread.daemon = True
        self.background_thread.start()
        
        self.election_thread = threading.Thread(target=self.election_timeout_loop)
        self.election_thread.daemon = True
        self.election_thread.start()
        
        self.heartbeat_thread = threading.Thread(target=self.master_heartbeat_loop)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def stop_background_tasks(self):
        """Stop background tasks"""
        self.running = False
        if self.background_thread:
            self.background_thread.join()
        if self.election_thread:
            self.election_thread.join()
        if self.heartbeat_thread:
            self.heartbeat_thread.join()

    # Hierarchical Path Management Functions
    def normalize_path(self, path: str) -> str:
        """Normalize a file path to standard format"""
        if not path:
            return ROOT_PATH
        
        # Handle relative paths and resolve . and ..
        if not path.startswith(PATH_SEPARATOR):
            path = ROOT_PATH + path
        
        # Split path into components
        components = []
        for part in path.split(PATH_SEPARATOR):
            if part == '' or part == '.':
                continue
            elif part == '..':
                if components:
                    components.pop()
            else:
                components.append(part)
        
        # Reconstruct normalized path
        if not components:
            return ROOT_PATH
        
        return PATH_SEPARATOR + PATH_SEPARATOR.join(components)

    def validate_path(self, path: str) -> tuple[bool, str]:
        """Validate a file path"""
        if not path:
            return False, "Empty path"
        
        if len(path) > MAX_PATH_LENGTH:
            return False, f"Path too long (max {MAX_PATH_LENGTH} characters)"
        
        # Check for invalid characters
        invalid_chars = ['\\', '<', '>', ':', '"', '|', '?', '*', '\0']
        for char in invalid_chars:
            if char in path:
                return False, f"Invalid character '{char}' in path"
        
        # Normalize and check components
        normalized = self.normalize_path(path)
        components = normalized.strip(PATH_SEPARATOR).split(PATH_SEPARATOR) if normalized != ROOT_PATH else []
        
        for component in components:
            if not component:
                continue
            if len(component) > MAX_FILENAME_LENGTH:
                return False, f"Filename component too long: '{component}' (max {MAX_FILENAME_LENGTH} characters)"
            if component.startswith('.') and len(component) > 1:
                # Allow hidden files but validate the rest
                pass
        
        return True, normalized

    def get_parent_directory(self, path: str) -> str:
        """Get the parent directory of a path"""
        normalized = self.normalize_path(path)
        if normalized == ROOT_PATH:
            return ROOT_PATH
        
        components = normalized.strip(PATH_SEPARATOR).split(PATH_SEPARATOR)
        if len(components) <= 1:
            return ROOT_PATH
        
        return PATH_SEPARATOR + PATH_SEPARATOR.join(components[:-1])

    def get_filename(self, path: str) -> str:
        """Extract filename from path"""
        normalized = self.normalize_path(path)
        if normalized == ROOT_PATH:
            return ""
        
        components = normalized.strip(PATH_SEPARATOR).split(PATH_SEPARATOR)
        return components[-1] if components else ""

    def is_directory_path(self, path: str) -> bool:
        """Check if path represents a directory (ends with /)"""
        return path.endswith(PATH_SEPARATOR) and path != ROOT_PATH

    def create_directory_structure(self, path: str) -> bool:
        """Create directory structure for a file path"""
        parent_dir = self.get_parent_directory(path)
        if parent_dir == ROOT_PATH:
            return True
        
        # For now, we'll just validate the path structure
        # In a full implementation, you might want to create directory metadata
        valid, _ = self.validate_path(parent_dir)
        return valid

    def list_directory_contents(self, directory_path: str) -> List[str]:
        """List contents of a directory"""
        normalized_dir = self.normalize_path(directory_path)
        if not normalized_dir.endswith(PATH_SEPARATOR):
            normalized_dir += PATH_SEPARATOR
        
        contents = []
        with self.metadata_lock:
            for filepath in self.files.keys():
                if filepath.startswith(normalized_dir):
                    # Get the relative path from the directory
                    relative_path = filepath[len(normalized_dir):]
                    # Only include direct children (no subdirectories)
                    if PATH_SEPARATOR not in relative_path:
                        contents.append(relative_path)
        
        return sorted(contents)

    def path_exists(self, path: str) -> bool:
        """Check if a path exists in the filesystem"""
        normalized = self.normalize_path(path)
        
        with self.metadata_lock:
            # Check if it's a file
            if normalized in self.files:
                return True
            
            # Check if it's a directory (has files under it)
            if not normalized.endswith(PATH_SEPARATOR):
                normalized += PATH_SEPARATOR
            
            for filepath in self.files.keys():
                if filepath.startswith(normalized):
                    return True
        
        return False

    def delete_directory_recursive(self, directory_path: str) -> List[str]:
        """Delete all files in a directory recursively"""
        normalized_dir = self.normalize_path(directory_path)
        if not normalized_dir.endswith(PATH_SEPARATOR):
            normalized_dir += PATH_SEPARATOR
        
        deleted_files = []
        with self.metadata_lock:
            files_to_delete = [
                filepath for filepath in self.files.keys()
                if filepath.startswith(normalized_dir)
            ]
            
            for filepath in files_to_delete:
                file_metadata = self.files[filepath]
                # Mark chunks for deletion
                for chunk_handle in file_metadata.chunks:
                    if chunk_handle in self.chunks:
                        del self.chunks[chunk_handle]
                
                del self.files[filepath]
                deleted_files.append(filepath)
            
            if deleted_files:
                self.commit(OP_DELETE_FILE, filenames=deleted_files)
        
        return deleted_files

    # Enhanced Directory Management Functions
    def create_directory(self, directory_path: str) -> bool:
        """Create a directory"""
        valid, normalized_path = self.validate_path(directory_path)
        if not valid:
            return False
        
        # Ensure it's treated as a directory
        if not normalized_path.endswith(PATH_SEPARATOR):
            normalized_path += PATH_SEPARATOR
        
        with self.metadata_lock:
            # Check if directory already exists
            if normalized_path in self.directories:
                return False  # Directory already exists
            
            # Create parent directories if they don't exist
            parent_dir = self.get_parent_directory(normalized_path.rstrip(PATH_SEPARATOR))
            if parent_dir != ROOT_PATH:
                parent_dir_with_slash = parent_dir + PATH_SEPARATOR
                if parent_dir_with_slash not in self.directories:
                    # Recursively create parent directories
                    if not self.create_directory(parent_dir):
                        return False
            
            # Create directory metadata
            current_time = time.time()
            dir_metadata = DirectoryMetadata(
                dirname=normalized_path,
                created_time=current_time,
                modified_time=current_time,
                parent_dir=parent_dir if parent_dir != ROOT_PATH else None
            )
            
            self.directories[normalized_path] = dir_metadata
            self.commit(OP_CREATE_DIR, dirname=normalized_path,
                        created_time=current_time,
                        parent_dir=dir_metadata.parent_dir)
            
            self.logger.info(f"Created directory: {normalized_path}")
            return True

    def directory_exists(self, directory_path: str) -> bool:
        """Check if a directory exists"""
        valid, normalized_path = self.validate_path(directory_path)
        if not valid:
            return False
        
        if not normalized_path.endswith(PATH_SEPARATOR):
            normalized_path += PATH_SEPARATOR
        
        with self.metadata_lock:
            return normalized_path in self.directories

    def list_directory_entries(self, directory_path: str) -> Dict[str, str]:
        """List directory entries with their types (file/directory)"""
        valid, normalized_dir = self.validate_path(directory_path)
        if not valid:
            return {}
        
        if not normalized_dir.endswith(PATH_SEPARATOR):
            normalized_dir += PATH_SEPARATOR
        
        entries = {}
        with self.metadata_lock:
            # FIXED: Add files that exist in this directory
            for filepath in self.files.keys():
                if filepath.startswith(normalized_dir):
                    relative_path = filepath[len(normalized_dir):]
                    # Only include direct children
                    if PATH_SEPARATOR not in relative_path:
                        entries[relative_path] = 'file'
                    else:
                        # FIXED: Auto-discover intermediate directories from file paths
                        first_component = relative_path.split(PATH_SEPARATOR)[0]
                        if first_component and first_component not in entries:
                            entries[first_component] = 'directory'
            
            # Add explicitly created subdirectories
            for dirpath in self.directories.keys():
                if dirpath.startswith(normalized_dir) and dirpath != normalized_dir:
                    relative_path = dirpath[len(normalized_dir):].rstrip(PATH_SEPARATOR)
                    # Only include direct children
                    if PATH_SEPARATOR not in relative_path:
                        entries[relative_path] = 'directory'
        
        return entries

    def delete_directory(self, directory_path: str, recursive: bool = False) -> Tuple[bool, str, List[str]]:
        """Delete a directory"""
        valid, normalized_dir = self.validate_path(directory_path)
        if not valid:
            return False, f"Invalid path: {normalized_dir}", []
        
        if not normalized_dir.endswith(PATH_SEPARATOR):
            normalized_dir += PATH_SEPARATOR
        
        with self.metadata_lock:
            # Check if directory exists
            if normalized_dir not in self.directories:
                return False, f"Directory {normalized_dir} does not exist", []
            
            # Check if directory is empty or if recursive deletion is requested
            entries = self.list_directory_entries(normalized_dir)
            if entries and not recursive:
                return False, f"Directory {normalized_dir} is not empty. Use recursive=True to delete.", []
            
            deleted_items = []
            
            if recursive:
                # Delete all files in the directory
                files_to_delete = [
                    filepath for filepath in self.files.keys()
                    if filepath.startswith(normalized_dir)
                ]
                
                for filepath in files_to_delete:
                    file_metadata = self.files[filepath]
                    # Mark chunks for deletion
                    for chunk_handle in file_metadata.chunks:
                        if chunk_handle in self.chunks:
                            del self.chunks[chunk_handle]
                    
                    del self.files[filepath]
                    deleted_items.append(filepath)
                
                # Delete all subdirectories
                dirs_to_delete = [
                    dirpath for dirpath in self.directories.keys()
                    if dirpath.startswith(normalized_dir) and dirpath != normalized_dir
                ]
                
                # Sort by depth (deepest first) to delete in correct order
                dirs_to_delete.sort(key=lambda x: x.count(PATH_SEPARATOR), reverse=True)
                
                for dirpath in dirs_to_delete:
                    del self.directories[dirpath]
                    deleted_items.append(f"dir:{dirpath}")
            
            # Delete the directory itself
            del self.directories[normalized_dir]
            deleted_items.append(f"dir:{normalized_dir}")
            
            self.commit(
                OP_DELETE_DIR,
                dirnames=[i[4:] for i in deleted_items if i.startswith('dir:')],
                filenames=[i for i in deleted_items if not i.startswith('dir:')])
            
            self.logger.info(f"Deleted directory: {normalized_dir} ({'recursive' if recursive else 'empty'})")
            return True, f"Directory {normalized_dir} deleted successfully", deleted_items

    def get_directory_info(self, directory_path: str) -> Optional[Dict]:
        """Get directory information"""
        valid, normalized_dir = self.validate_path(directory_path)
        if not valid:
            return None
        
        if not normalized_dir.endswith(PATH_SEPARATOR):
            normalized_dir += PATH_SEPARATOR
        
        with self.metadata_lock:
            if normalized_dir not in self.directories:
                return None
            
            dir_metadata = self.directories[normalized_dir]
            entries = self.list_directory_entries(normalized_dir)
            
            return {
                'dirname': dir_metadata.dirname,
                'created_time': dir_metadata.created_time,
                'modified_time': dir_metadata.modified_time,
                'parent_dir': dir_metadata.parent_dir,
                'entries': entries,
                'entry_count': len(entries)
            }

    def move_path(self, source_path: str, dest_path: str) -> Tuple[bool, str]:
        """Move/rename a file or directory"""
        valid_src, normalized_src = self.validate_path(source_path)
        valid_dst, normalized_dst = self.validate_path(dest_path)
        
        if not valid_src:
            return False, f"Invalid source path: {normalized_src}"
        if not valid_dst:
            return False, f"Invalid destination path: {normalized_dst}"
        
        with self.metadata_lock:
            # Check if source exists
            is_file = normalized_src in self.files
            is_dir = (normalized_src + PATH_SEPARATOR) in self.directories
            
            if not is_file and not is_dir:
                return False, f"Source path {normalized_src} does not exist"
            
            # Handle file move
            if is_file:
                if normalized_dst in self.files:
                    return False, f"Destination file {normalized_dst} already exists"
                
                file_metadata = self.files[normalized_src]
                file_metadata.filename = normalized_dst
                file_metadata.modified_time = time.time()
                
                self.files[normalized_dst] = file_metadata
                del self.files[normalized_src]
                
                self.commit(OP_MOVE, files=[[normalized_src, normalized_dst]], dirs=[])
                
                self.logger.info(f"Moved file: {normalized_src} -> {normalized_dst}")
                return True, f"File moved successfully"
            
            # Handle directory move
            else:
                src_dir = normalized_src + PATH_SEPARATOR
                dst_dir = normalized_dst + PATH_SEPARATOR
                
                if dst_dir in self.directories:
                    return False, f"Destination directory {normalized_dst} already exists"
                
                # Move directory and all its contents
                items_to_move = []
                
                # Find all files and directories under source
                for filepath in list(self.files.keys()):
                    if filepath.startswith(src_dir):
                        new_filepath = dst_dir + filepath[len(src_dir):]
                        items_to_move.append(('file', filepath, new_filepath))
                
                # `src_dir` itself matches this prefix test, and the block
                # below already moves it explicitly. Without the exclusion it
                # lands in items_to_move too and the second pass raises
                # KeyError on a directory it just deleted -- so moving any
                # directory that contained a subdirectory crashed the master.
                for dirpath in list(self.directories.keys()):
                    if dirpath.startswith(src_dir) and dirpath != src_dir:
                        new_dirpath = dst_dir + dirpath[len(src_dir):]
                        items_to_move.append(('dir', dirpath, new_dirpath))
                
                # Move the root directory
                dir_metadata = self.directories[src_dir]
                dir_metadata.dirname = dst_dir
                dir_metadata.modified_time = time.time()
                
                self.directories[dst_dir] = dir_metadata
                del self.directories[src_dir]
                
                # Move all contents
                for item_type, old_path, new_path in items_to_move:
                    if item_type == 'file':
                        file_metadata = self.files[old_path]
                        file_metadata.filename = new_path
                        file_metadata.modified_time = time.time()
                        self.files[new_path] = file_metadata
                        del self.files[old_path]
                    else:  # directory
                        dir_metadata = self.directories[old_path]
                        dir_metadata.dirname = new_path
                        dir_metadata.modified_time = time.time()
                        self.directories[new_path] = dir_metadata
                        del self.directories[old_path]
                
                self.commit(
                    OP_MOVE,
                    files=[[o, n] for t, o, n in items_to_move if t == 'file'],
                    dirs=[[src_dir, dst_dir]]
                         + [[o, n] for t, o, n in items_to_move if t == 'dir'])
                
                self.logger.info(f"Moved directory: {normalized_src} -> {normalized_dst}")
                return True, f"Directory moved successfully"

    # Initialize root directory if it doesn't exist
    def _ensure_root_directory(self):
        """Ensure root directory exists"""
        with self.metadata_lock:
            if ROOT_PATH not in self.directories:
                root_metadata = DirectoryMetadata(
                    dirname=ROOT_PATH,
                    created_time=time.time(),
                    modified_time=time.time(),
                    parent_dir=None
                )
                self.directories[ROOT_PATH] = root_metadata
                self.commit(OP_CREATE_DIR, dirname=ROOT_PATH,
                            created_time=root_metadata.created_time, parent_dir=None)
                self.logger.info("Created root directory metadata")

class MasterServiceServicer(gfs_pb2_grpc.MasterServiceServicer):
    def __init__(self, master: MultiMasterGFS):
        self.master = master

    def _ensure_leader(self):
        """Ensure this master is the leader before processing requests"""
        if not self.master.is_leader():
            raise grpc.RpcError("Not the leader master")

    def CreateFile(self, request, context):
        """Create a new file in the file system"""
        try:
            self._ensure_leader()
        except grpc.RpcError:
            return gfs_pb2.CreateFileResponse(
                success=False,
                message="Not the leader master, please retry with the leader"
            )
            
        filename = request.filename
        
        # Validate and normalize the path
        valid, normalized_path = self.master.validate_path(filename)
        if not valid:
            return gfs_pb2.CreateFileResponse(
                success=False,
                message=f"Invalid path: {normalized_path}"
            )
        
        # Check if it's a directory path
        if self.master.is_directory_path(normalized_path):
            return gfs_pb2.CreateFileResponse(
                success=False,
                message="Cannot create file with directory path (ends with /)"
            )
        
        with self.master.metadata_lock:
            if normalized_path in self.master.files:
                return gfs_pb2.CreateFileResponse(
                    success=False,
                    message=f"File {normalized_path} already exists"
                )
            
            # FIXED: Auto-create parent directories
            parent_dir = self.master.get_parent_directory(normalized_path)
            if parent_dir != ROOT_PATH:
                parent_dir_with_slash = parent_dir + PATH_SEPARATOR
                if parent_dir_with_slash not in self.master.directories:
                    # Recursively create parent directories
                    if not self.master.create_directory(parent_dir):
                        return gfs_pb2.CreateFileResponse(
                            success=False,
                            message=f"Failed to create parent directory structure for {normalized_path}"
                        )
            
            # Create file metadata
            current_time = time.time()
            file_metadata = FileMetadata(
                filename=normalized_path,
                chunks=[],
                size=0,
                created_time=current_time,
                modified_time=current_time
            )
            
            self.master.files[normalized_path] = file_metadata
            self.master.commit(OP_CREATE_FILE, filename=normalized_path,
                               created_time=current_time)
            
            self.master.logger.info(f"Created file: {normalized_path}")
            return gfs_pb2.CreateFileResponse(
                success=True,
                message=f"File {normalized_path} created successfully"
            )

    def DeleteFile(self, request, context):
        """Delete a file from the file system"""
        try:
            self._ensure_leader()
        except grpc.RpcError:
            return gfs_pb2.DeleteFileResponse(
                success=False,
                message="Not the leader master, please retry with the leader"
            )
            
        filename = request.filename
        
        # Validate and normalize the path
        valid, normalized_path = self.master.validate_path(filename)
        if not valid:
            return gfs_pb2.DeleteFileResponse(
                success=False,
                message=f"Invalid path: {normalized_path}"
            )
        
        with self.master.metadata_lock:
            # Check if it's a directory deletion request
            if self.master.is_directory_path(normalized_path):
                deleted_files = self.master.delete_directory_recursive(normalized_path)
                if deleted_files:
                    self.master.logger.info(f"Deleted directory: {normalized_path} ({len(deleted_files)} files)")
                    return gfs_pb2.DeleteFileResponse(
                        success=True,
                        message=f"Directory {normalized_path} and {len(deleted_files)} files deleted successfully"
                    )
                else:
                    return gfs_pb2.DeleteFileResponse(
                        success=False,
                        message=f"Directory {normalized_path} not found or empty"
                    )
            
            # Single file deletion
            if normalized_path not in self.master.files:
                return gfs_pb2.DeleteFileResponse(
                    success=False,
                    message=f"File {normalized_path} not found"
                )
            
            # Mark chunks for deletion
            file_metadata = self.master.files[normalized_path]
            for chunk_handle in file_metadata.chunks:
                if chunk_handle in self.master.chunks:
                    del self.master.chunks[chunk_handle]
            
            del self.master.files[normalized_path]
            self.master.commit(OP_DELETE_FILE, filenames=[normalized_path])
            
            self.master.logger.info(f"Deleted file: {normalized_path}")
            return gfs_pb2.DeleteFileResponse(
                success=True,
                message=f"File {normalized_path} deleted successfully"
            )

    def GetFileInfo(self, request, context):
        """Get file information"""
        filename = request.filename
        
        # Validate and normalize the path
        valid, normalized_path = self.master.validate_path(filename)
        if not valid:
            return gfs_pb2.GetFileInfoResponse(
                success=False,
                message=f"Invalid path: {normalized_path}"
            )
        
        with self.master.metadata_lock:
            # Check if it's a directory listing request
            if self.master.is_directory_path(normalized_path):
                contents = self.master.list_directory_contents(normalized_path)
                # For directory listing, return a special response
                # Note: This might need protocol buffer changes to properly support directories
                return gfs_pb2.GetFileInfoResponse(
                    success=False,
                    message=f"Directory listing: {', '.join(contents) if contents else 'empty'}"
                )
            
            if normalized_path not in self.master.files:
                return gfs_pb2.GetFileInfoResponse(
                    success=False,
                    message=f"File {normalized_path} not found"
                )
            
            file_metadata = self.master.files[normalized_path]
            chunk_handles = [gfs_pb2.ChunkHandle(handle=h) for h in file_metadata.chunks]
            
            file_info = gfs_pb2.FileInfo(
                filename=normalized_path,
                size=file_metadata.size,
                chunk_size=CHUNK_SIZE,
                chunks=chunk_handles
            )
            
            return gfs_pb2.GetFileInfoResponse(
                success=True,
                file_info=file_info
            )

    def GetChunkLocations(self, request, context):
        """Get locations of a chunk"""
        chunk_handle = request.chunk_handle.handle
        
        with self.master.metadata_lock:
            if chunk_handle not in self.master.chunks:
                return gfs_pb2.GetChunkLocationsResponse(
                    success=False,
                    message=f"Chunk {chunk_handle} not found"
                )
            
            chunk = self.master.chunks[chunk_handle]
            locations = []

            for server_id in chunk.locations:
                if server_id in self.master.chunk_servers:
                    server = self.master.chunk_servers[server_id]
                    locations.append(gfs_pb2.ChunkLocation(
                        server_id=server_id,
                        address=server.address,
                        port=server.port
                    ))

            resp = gfs_pb2.GetChunkLocationsResponse(
                success=True,
                locations=locations
            )

            # If this chunk has been erasure-coded, hand back the shard map
            # too (ADR-005). Both are returned during the conversion window:
            # replicas still exist until every shard is confirmed, and a
            # client that can use either is a client that never sees the
            # migration. Once the replicas are retired, `locations` is empty
            # and the client falls through to the shard path.
            st = self.master.ec.state(chunk_handle)
            if st is not None and st.shards:
                resp.ec.is_ec = True
                resp.ec.data_shards = st.k
                resp.ec.parity_shards = st.m
                resp.ec.original_length = st.original_length
                for index, server_id in sorted(st.shards.items()):
                    server = self.master.chunk_servers.get(server_id)
                    if server is None:
                        continue
                    sl = resp.ec.shards.add()
                    sl.server_id = server_id
                    sl.address = server.address
                    sl.port = server.port
                    sl.shard_index = index

            return resp

    def AllocateChunk(self, request, context):
        """Allocate a new chunk for a file"""
        try:
            self._ensure_leader()
        except grpc.RpcError:
            return gfs_pb2.AllocateChunkResponse(
                success=False,
                message="Not the leader master, please retry with the leader"
            )
            
        filename = request.filename
        chunk_index = request.chunk_index
        
        # Validate and normalize the path (FIXED: Added path normalization)
        valid, normalized_filename = self.master.validate_path(filename)
        if not valid:
            return gfs_pb2.AllocateChunkResponse(
                success=False,
                message=f"Invalid path: {normalized_filename}"
            )
        
        with self.master.metadata_lock:
            if normalized_filename not in self.master.files:
                return gfs_pb2.AllocateChunkResponse(
                    success=False,
                    message=f"File {normalized_filename} not found"
                )
            
            # Select chunk servers
            selected_servers = self.master.select_chunk_servers()
            if len(selected_servers) < REPLICATION_FACTOR:
                return gfs_pb2.AllocateChunkResponse(
                    success=False,
                    message="Not enough chunk servers available"
                )
            
            # Create chunk metadata
            chunk_handle = self.master.generate_chunk_handle()
            chunk_metadata = ChunkMetadata(
                handle=chunk_handle,
                version=1,
                locations=set(selected_servers),
                last_modified=time.time()
            )
            
            self.master.chunks[chunk_handle] = chunk_metadata
            
            # Update file metadata (FIXED: Using normalized filename)
            file_metadata = self.master.files[normalized_filename]
            if chunk_index >= len(file_metadata.chunks):
                file_metadata.chunks.extend([None] * (chunk_index - len(file_metadata.chunks) + 1))
            file_metadata.chunks[chunk_index] = chunk_handle
            file_metadata.modified_time = time.time()
            
            # Update chunk server metadata
            for server_id in selected_servers:
                if server_id in self.master.chunk_servers:
                    self.master.chunk_servers[server_id].chunks.add(chunk_handle)
            
            self.master.commit(OP_ALLOCATE_CHUNK,
                               filename=normalized_filename,
                               chunk_index=chunk_index,
                               chunk_handle=chunk_handle,
                               chunk_version=chunk_metadata.version)
            
            # Prepare response
            locations = []
            for server_id in selected_servers:
                if server_id in self.master.chunk_servers:
                    server = self.master.chunk_servers[server_id]
                    locations.append(gfs_pb2.ChunkLocation(
                        server_id=server_id,
                        address=server.address,
                        port=server.port
                    ))
            
            self.master.logger.info(f"Allocated chunk {chunk_handle} for file {normalized_filename}")
            return gfs_pb2.AllocateChunkResponse(
                success=True,
                chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                locations=locations
            )

    # def Heartbeat(self, request, context):
    #     """Handle heartbeat from chunk server"""
    #     # FIXED: Use the correct field names from protobuf
    #     server_id = request.server_id
    #     server_address = request.chunk_server_address
    #     avachunks = set(request.available_chunks)
    #     total_chunks = request.total_chunks
    #     available_space = request.available_space
    #     port = request.port
        
    #     # # Extract address and port from server_address or context
    #     # if server_address and ':' in server_address:
    #     #     address, port_str = server_address.rsplit(':', 1)
    #     #     try:
    #     #         port = int(port_str)
    #     #     except ValueError:
    #     #         port = 9001  # Default chunk server port
    #     # else:
    #     #     # Fallback to extracting from context
    #     #     peer = context.peer()
    #     #     address = peer.split(':')[1] if ':' in peer else 'localhost'
    #     #     port = 9001  # Default chunk server port


        
    #     with self.master.metadata_lock:
    #         # Check if this is a new chunk server or existing one
    #         is_new_server = server_id not in self.master.chunk_servers
            
    #         # Update or create chunk server info
    #         if is_new_server:
    #             self.master.chunk_servers[server_id] = ChunkServerInfo(
    #                 server_id=server_id,
    #                 address=address,
    #                 port=port,
    #                 last_heartbeat=time.time(),
    #                 available_space=available_space,
    #                 chunks=available_chunks
    #             )
    #             self.master.logger.info(f"🆕 New chunk server registered: {server_id} at {server_address} with {total_chunks} chunks and {available_space // (1024**3):.1f} GB free")
    #         else:
    #             server_info = self.master.chunk_servers[server_id]
    #             server_info.last_heartbeat = time.time()
    #             server_info.available_space = available_space
    #             server_info.chunks = available_chunks

    #             server_info.address = address
    #             server_info.port = port
                
    #             # FIXED: Log all heartbeats at INFO level for better monitoring
    #             if not hasattr(server_info, 'heartbeat_count'):
    #                 server_info.heartbeat_count = 0
    #             server_info.heartbeat_count += 1
                
    #             # Log every heartbeat but with different detail levels
    #             self.master.logger.info(f"💓 Heartbeat #{server_info.heartbeat_count} from {server_id}: {total_chunks} chunks, {available_space // (1024**3):.1f} GB free")
            
    #         return gfs_pb2.HeartbeatResponse(
    #             status="OK",
    #             message="Heartbeat received successfully"
    #         )

    def Heartbeat(self, request, context):
        """Handle heartbeat from chunk server"""

        server_id = request.server_id
        server_address = request.server_address
        chunks = set(chunk.handle for chunk in request.chunks)
        total_chunks = request.total_chunks
        available_space = request.available_space
        port = request.port
        
        # Extract address and port from server_address or context
        if server_address and ':' in server_address:
            address, port_str = server_address.rsplit(':', 1)
            try:
                port = int(port_str)
            except ValueError:
                port = 9001  # Default chunk server port
        else:
            # Fallback to extracting from context
            peer = context.peer()
            address = peer.split(':')[1] if ':' in peer else 'localhost'
            port = 9001  # Default chunk server port
        
        with self.master.metadata_lock:
            # Update or create chunk server info
            if server_id not in self.master.chunk_servers:
                self.master.chunk_servers[server_id] = ChunkServerInfo(
                    server_id=server_id,
                    address=address,  # ✅ From gRPC peer
                    port=port,        # ✅ From heartbeat request
                    last_heartbeat=time.time(),
                    available_space=available_space,
                    chunks=chunks
                )
                self.master.logger.info(f"New chunk server registered: {server_id} at {address}:{port}")
            else:
                server_info = self.master.chunk_servers[server_id]
                server_info.last_heartbeat = time.time()
                server_info.available_space = available_space
                server_info.chunks = chunks
                # Update address and port in case they changed
                server_info.address = address
                server_info.port = port

            # ---- shard reports (ADR-005) ----
            # Each report is self-describing, so the master learns a chunk's
            # coding parameters and original length from the first shard it
            # hears about -- it never has to have been told separately.
            reported_shards = set()
            for sl in request.shards:
                reported_shards.add((sl.chunk_handle, sl.shard_index))
                self.master.ec.note_shard_present(
                    sl.chunk_handle, sl.shard_index, server_id,
                    sl.data_shards, sl.parity_shards, sl.original_length)
            # A shard this server stopped reporting is gone from it.
            for base, st in list(self.master.ec.ec_chunks.items()):
                for idx, holder in list(st.shards.items()):
                    if holder == server_id and (base, idx) not in reported_shards:
                        self.master.ec.note_shard_absent(base, idx, server_id)

            # ---- learn replica locations from what the server reports ----
            # The master never persists locations (ADR-001); this is where
            # they come from. It is also how a completed clone is noticed:
            # the target simply starts reporting the chunk.
            for handle in chunks:
                chunk = self.master.chunks.get(handle)
                if chunk is not None:
                    if server_id not in chunk.locations:
                        chunk.locations.add(server_id)
                        self.master.replication.note_replica_present(handle, server_id)

            # A server that stops reporting a chunk no longer has it.
            for handle, chunk in self.master.chunks.items():
                if server_id in chunk.locations and handle not in chunks:
                    chunk.locations.discard(server_id)

            if not self.master.is_leader():
                # Followers track locations so they are useful immediately on
                # promotion, but only the leader hands out work.
                return gfs_pb2.HeartbeatResponse(success=True)

            # ---- reclaim orphaned and stale replicas (ADR-004) ----
            self.master.replication.plan_deletions({server_id: chunks},
                                                   self.master.chunks)
            to_delete = self.master.replication.deletions_for(server_id)

            # ---- hand out clone work (ADR-003) ----
            tasks = self.master.replication.tasks_for(
                server_id, self.master.chunks, self.master.chunk_servers)

            # ---- hand out erasure-coding work (ADR-005) ----
            ec_encodes, ec_repairs, shard_deletes = [], [], []
            if self.master.ec_enabled:
                ec_encodes = self.master.ec.encode_tasks_for(
                    server_id, self.master.chunks, self.master.chunk_servers)
                ec_repairs = self.master.ec.repair_tasks_for(
                    server_id, self.master.chunk_servers)
                self.master.ec.plan_shard_deletions({server_id: reported_shards})
                shard_deletes = self.master.ec.shard_deletions_for(server_id)

        response = gfs_pb2.HeartbeatResponse(success=True)
        for handle in to_delete:
            response.chunks_to_delete.add(handle=handle)
        for t in tasks:
            task = response.replication_tasks.add()
            task.chunk_handle.handle = t['chunk_handle']
            task.chunk_version = t['chunk_version']
            for src in t['sources']:
                loc = task.source_locations.add()
                loc.server_id, loc.address, loc.port = (
                    src['server_id'], src['address'], src['port'])
            task.target_location.server_id = t['target']['server_id']
            task.target_location.address = t['target']['address']
            task.target_location.port = t['target']['port']

        for t in ec_encodes:
            task = response.ec_encode_tasks.add()
            task.chunk_handle.handle = t['chunk_handle']
            task.data_shards, task.parity_shards = t['k'], t['m']
            task.chunk_version = t['chunk_version']
            for tgt in t['targets']:
                loc = task.shard_targets.add()
                loc.server_id, loc.address, loc.port = (
                    tgt['server_id'], tgt['address'], tgt['port'])

        for t in ec_repairs:
            task = response.ec_repair_tasks.add()
            task.chunk_handle.handle = t['chunk_handle']
            task.shard_index = t['shard_index']
            task.data_shards, task.parity_shards = t['k'], t['m']
            task.original_length = t['original_length']
            for src in t['sources']:
                loc = task.sources.add()
                loc.server_id, loc.address, loc.port, loc.shard_index = (
                    src['server_id'], src['address'], src['port'],
                    src['shard_index'])

        for handle, idx in shard_deletes:
            d = response.shards_to_delete.add()
            d.chunk_handle.handle = handle
            d.shard_index = idx

        if to_delete or tasks or ec_encodes or ec_repairs:
            self.master.logger.info(
                f'heartbeat -> {server_id}: {len(tasks)} clone, '
                f'{len(to_delete)} delete, {len(ec_encodes)} ec-encode, '
                f'{len(ec_repairs)} ec-repair')
        return response

    # def GetPrimaryMaster(self, request, context):
    #     """Return information about the primary master"""
    #     leader_info = self.master.get_leader_info()
        
    #     if leader_info:
    #         primary_master = gfs_pb2.MasterInfo(
    #             master_id=leader_info['master_id'],
    #             address=leader_info['address'],
    #             port=leader_info['port'],
    #             term=leader_info['term'],
    #             is_primary=True,
    #             last_heartbeat=int(time.time())
    #         )
            
    #         all_masters = [primary_master]
    #         for peer_id, peer in self.master.peers.items():
    #             all_masters.append(gfs_pb2.MasterInfo(
    #                 master_id=peer.master_id,
    #                 address=peer.address,
    #                 port=peer.port,
    #                 term=peer.term,
    #                 is_primary=False,
    #                 last_heartbeat=int(peer.last_heartbeat)
    #             ))
            
    #         return gfs_pb2.GetPrimaryMasterResponse(
    #             success=True,
    #             primary_master=primary_master,
    #             all_masters=all_masters
    #         )
    #     else:
    #         return gfs_pb2.GetPrimaryMasterResponse(
    #             success=False,
    #             message="No leader currently elected"
    #         )


    def GetPrimaryMaster(self, request, context):
        """Return information about the primary master - chunk servers use this to discover leader"""
        with self.master.state_lock:
            # If THIS master is the leader, return success with is_primary=True
            if self.master.is_leader():
                primary_master = gfs_pb2.MasterInfo(
                    master_id=self.master.master_id,
                    address='localhost',
                    port=self.master.port,
                    term=self.master.current_term,
                    is_primary=True,
                    last_heartbeat=int(time.time())
                )
                
                # Add information about peer masters
                all_masters = [primary_master]
                for peer_id, peer in self.master.peers.items():
                    all_masters.append(gfs_pb2.MasterInfo(
                        master_id=peer.master_id,
                        address=peer.address,
                        port=peer.port,
                        term=peer.term,
                        is_primary=False,
                        last_heartbeat=int(peer.last_heartbeat)
                    ))
                
                return gfs_pb2.GetPrimaryMasterResponse(
                    success=True,
                    primary_master=primary_master,
                    all_masters=all_masters,
                    message="I am the current leader"
                )
            else:
                # This master is not the leader
                leader_info = self.master.get_leader_info()
                if leader_info:
                    # We know who the leader is, redirect chunk server
                    # Create primary master info to redirect chunk server
                    primary_master = gfs_pb2.MasterInfo(
                        master_id=leader_info['master_id'],
                        address=leader_info['address'],
                        port=leader_info['port'],
                        term=leader_info['term'],
                        is_primary=True,
                        last_heartbeat=int(time.time())
                    )
                    
                    return gfs_pb2.GetPrimaryMasterResponse(
                        success=True,
                        primary_master=primary_master,
                        message=f"Not leader. Current leader is {leader_info['master_id']} at {leader_info['address']}:{leader_info['port']}"
                    )
                else:
                    # No leader currently known
                    return gfs_pb2.GetPrimaryMasterResponse(
                        success=False,
                        message="No leader currently elected, election in progress"
                    )

    def CreateDirectory(self, request, context):
        """Create a new directory"""
        try:
            self._ensure_leader()
        except grpc.RpcError:
            return gfs_pb2.CreateDirectoryResponse(
                success=False,
                message="Not the leader master, please retry with the leader"
            )
            
        directory_path = request.directory_path
        
        # Validate and normalize the path
        valid, normalized_path = self.master.validate_path(directory_path)
        if not valid:
            return gfs_pb2.CreateDirectoryResponse(
                success=False,
                message=f"Invalid path: {normalized_path}"
            )
        
        if self.master.create_directory(normalized_path):
            return gfs_pb2.CreateDirectoryResponse(
                success=True,
                message=f"Directory {normalized_path} created successfully"
            )
        else:
            return gfs_pb2.CreateDirectoryResponse(
                success=False,
                message=f"Failed to create directory {normalized_path} (may already exist)"
            )

    def DeleteDirectory(self, request, context):
        """Delete a directory"""
        try:
            self._ensure_leader()
        except grpc.RpcError:
            return gfs_pb2.DeleteDirectoryResponse(
                success=False,
                message="Not the leader master, please retry with the leader",
                items_deleted=0
            )
            
        directory_path = request.directory_path
        recursive = request.recursive
        
        # Validate and normalize the path
        valid, normalized_path = self.master.validate_path(directory_path)
        if not valid:
            return gfs_pb2.DeleteDirectoryResponse(
                success=False,
                message=f"Invalid path: {normalized_path}",
                items_deleted=0
            )
        
        success, message, deleted_items = self.master.delete_directory(normalized_path, recursive)
        
        return gfs_pb2.DeleteDirectoryResponse(
            success=success,
            message=message,
            items_deleted=len(deleted_items)
        )

    def ListDirectory(self, request, context):
        """List directory contents"""
        directory_path = request.directory_path
        
        # Validate and normalize the path
        valid, normalized_path = self.master.validate_path(directory_path)
        if not valid:
            return gfs_pb2.ListDirectoryResponse(
                success=False,
                entries=[],
                message=f"Invalid path: {normalized_path}"
            )
        
        dir_info = self.master.get_directory_info(normalized_path)
        if dir_info:
            entries = []
            for name, entry_type in dir_info['entries'].items():
                # For directories, we don't have size/time info easily available
                # In a full implementation, you'd want to store this in DirectoryMetadata
                entry = gfs_pb2.DirectoryEntry(
                    name=name,
                    type=entry_type,
                    size=0 if entry_type == 'directory' else 0,  # Would need file metadata lookup
                    created_time=0,  # Would need metadata lookup
                    modified_time=0  # Would need metadata lookup
                )
                entries.append(entry)
            
            return gfs_pb2.ListDirectoryResponse(
                success=True,
                entries=entries,
                message=f"Directory listing successful ({len(entries)} items)"
            )
        else:
            return gfs_pb2.ListDirectoryResponse(
                success=False,
                entries=[],
                message=f"Directory {normalized_path} not found"
            )

    def ListAllFiles(self, request, context):
        """List all files and directories in the filesystem"""
        with self.master.metadata_lock:
            entries = []
            
            # Add all directories
            for dirpath, dir_metadata in self.master.directories.items():
                entry = gfs_pb2.FileSystemEntry(
                    path=dirpath,
                    type='directory',
                    size=0,
                    created_time=int(dir_metadata.created_time),
                    modified_time=int(dir_metadata.modified_time)
                )
                entries.append(entry)
            
            # Add all files
            for filepath, file_metadata in self.master.files.items():
                entry = gfs_pb2.FileSystemEntry(
                    path=filepath,
                    type='file',
                    size=file_metadata.size,
                    created_time=int(file_metadata.created_time),
                    modified_time=int(file_metadata.modified_time)
                )
                entries.append(entry)
            
            # Sort entries by path for consistent output
            entries.sort(key=lambda x: x.path)
            
            return gfs_pb2.ListAllFilesResponse(
                success=True,
                entries=entries,
                message=f"Found {len(entries)} items in filesystem"
            )

    def MoveFile(self, request, context):
        """Move/rename a file or directory"""
        try:
            self._ensure_leader()
        except grpc.RpcError:
            return gfs_pb2.CreateFileResponse(  # Reusing CreateFileResponse for now
                success=False,
                message="Not the leader master, please retry with the leader"
            )
        
        # For now, we'll use the filename field for source and a custom approach for destination
        # This would ideally need a new RPC message type
        source_path = request.filename
        
        # This is a limitation - we need proper RPC support for move operations
        # For now, return an error suggesting to use delete+create
        return gfs_pb2.CreateFileResponse(
            success=False,
            message="Move operation not yet supported via RPC. Use delete + create as workaround."
        )

    def UpdateFileSize(self, request, context):
        """Update file size after a successful write operation"""
        try:
            self._ensure_leader()
        except grpc.RpcError:
            return gfs_pb2.UpdateFileSizeResponse(
                success=False,
                message="Not the leader master, please retry with the leader"
            )
            
        filename = request.filename
        new_size = request.new_size
        offset = request.offset
        length = request.length
        
        # Validate and normalize the path
        valid, normalized_path = self.master.validate_path(filename)
        if not valid:
            return gfs_pb2.UpdateFileSizeResponse(
                success=False,
                message=f"Invalid path: {normalized_path}"
            )
        
        with self.master.metadata_lock:
            if normalized_path not in self.master.files:
                return gfs_pb2.UpdateFileSizeResponse(
                    success=False,
                    message=f"File {normalized_path} not found"
                )
            
            file_metadata = self.master.files[normalized_path]
            
            # Update file size and modification time
            old_size = file_metadata.size
            file_metadata.size = new_size
            file_metadata.modified_time = time.time()
            
            self.master.commit(OP_UPDATE_FILE_SIZE,
                               filename=normalized_path, size=new_size,
                               modified_time=file_metadata.modified_time)
            
            self.master.logger.info(f"Updated file size: {normalized_path} from {old_size} to {new_size} bytes (write at offset {offset}, length {length})")
            
            return gfs_pb2.UpdateFileSizeResponse(
                success=True,
                message=f"File size updated to {new_size} bytes"
            )

    def GetClusterStatus(self, request, context):
        """Replication health, for tests, benchmarks and monitoring.

        Served by followers too (with is_leader=False) so a probe does not
        need to find the leader first. Only the leader's counts are
        authoritative for replication state, since only the leader plans.
        """
        m = self.master
        with m.metadata_lock:
            live = m.live_chunk_servers()
            now = time.time()

            under = 0
            unavailable = 0
            for chunk in m.chunks.values():
                alive = len([s for s in chunk.locations if s in live])
                if alive == 0 and chunk.locations:
                    unavailable += 1
                elif alive < REPLICATION_FACTOR:
                    under += 1

            repl = m.replication.snapshot()
            ecs = m.ec.snapshot(live_servers=live)
            logical, physical = m.ec.overhead_for(m.chunks)
            resp = gfs_pb2.GetClusterStatusResponse(
                success=True,
                is_leader=m.is_leader(),
                leader_id=m.leader_id or '',
                term=m.current_term,
                metadata_version=m.metadata_version,
                total_files=len(m.files),
                total_chunks=len(m.chunks),
                under_replicated_chunks=under,
                unavailable_chunks=unavailable,
                clones_in_flight=len(repl['in_flight']),
                pending_deletions=sum(repl['pending_deletes'].values()),
                ec_chunks=ecs['ec_chunks'],
                replicated_chunks=len(m.chunks) - ecs['ec_chunks'],
                ec_chunks_degraded=ecs['degraded'],
                ec_encodes_in_flight=ecs['encodes_in_flight'],
                logical_bytes=logical,
                physical_bytes=physical,
            )
            for sid, info in m.chunk_servers.items():
                cs = resp.chunk_servers.add()
                cs.server_id = sid
                cs.address = info.address
                cs.port = info.port
                cs.available_space = info.available_space
                cs.total_chunks = len(info.chunks)
                cs.heartbeat_age_seconds = now - info.last_heartbeat
                cs.alive = sid in live
            return resp

    def RequestLease(self, request, context):
        """Grant write lease for a chunk"""
        try:
            self._ensure_leader()   
        except grpc.RpcError:
            return gfs_pb2.RequestLeaseResponse(
                success=False,
                message="Not the leader master, please retry with the leader"
            )
        
        chunk_handle = request.chunk_handle.handle
        
        with self.master.lease_lock:
            if chunk_handle not in self.master.chunks:
                return gfs_pb2.RequestLeaseResponse(
                    success=False,
                    message=f"Chunk {chunk_handle} not found"
                )
            
            primary_server = self.master.grant_lease(chunk_handle)
            if not primary_server:
                return gfs_pb2.RequestLeaseResponse(
                    success=False,
                    message="No available servers for lease"
                )
            
            chunk = self.master.chunks[chunk_handle]
            locations = []
            for server_id in chunk.locations:
                if server_id in self.master.chunk_servers:
                    server = self.master.chunk_servers[server_id]
                    locations.append(gfs_pb2.ChunkLocation(
                        server_id=server_id,
                        address=server.address,
                        port=server.port
                    ))
            
            # Notify primary server about its lease (simplified)
            try:
                primary_address = None
                primary_port = None
                for loc in locations:
                    if loc.server_id == primary_server:
                        primary_address = loc.address
                        primary_port = loc.port
                        break
                
                if primary_address and primary_port:
                    # In a real implementation, we'd have a SetPrimaryLease RPC
                    # For now, we rely on the chunk server checking lease in WriteChunk
                    pass
                    
            except Exception as e:
                self.master.logger.warning(f"Failed to notify primary server: {e}")
            
            return gfs_pb2.RequestLeaseResponse(
                success=True,
                primary_server=primary_server,
                locations=locations,
                lease_expiry=int(chunk.lease_expiry),
                chunk_version=chunk.version,  # ✅ Send version for validation
                message="Lease granted successfully"
            )

    def UpdateChunkSize(self, request, context):
        """Update chunk size after write operations"""
        try:
            self._ensure_leader()
        except grpc.RpcError:
            return gfs_pb2.UpdateChunkSizeResponse(
                success=False,
                message="Not the leader master, please retry with the leader"
            )
        
        chunk_handle = request.chunk_handle.handle
        new_size = request.size
        
        with self.master.metadata_lock:
            if chunk_handle not in self.master.chunks:
                return gfs_pb2.UpdateChunkSizeResponse(
                    success=False,
                    message=f"Chunk {chunk_handle} not found"
                )
            
            # Update chunk size
            chunk = self.master.chunks[chunk_handle]
            old_size = chunk.size
            chunk.size = new_size
            # The hot/warm clock. Without this every chunk looks cold from
            # the moment it is allocated and EC would fight live writes.
            chunk.last_modified = time.time()
            
            # Update file size - find which file this chunk belongs to
            for filename, file_metadata in self.master.files.items():
                if chunk_handle in file_metadata.chunks:
                    chunk_index = file_metadata.chunks.index(chunk_handle)
                    
                    # Calculate new file size
                    total_size = 0
                    for i, ch_handle in enumerate(file_metadata.chunks):
                        if ch_handle and ch_handle in self.master.chunks:
                            chunk_obj = self.master.chunks[ch_handle]
                            total_size += chunk_obj.size
                    
                    file_metadata.size = total_size
                    file_metadata.modified_time = time.time()
                    owner_filename = filename
                    owner_size = total_size
                    break
            else:
                owner_filename = None
                owner_size = None

            # The owning file's size is derived here, so the record has to
            # carry it -- otherwise replay rebuilds chunks with the right
            # sizes but leaves every file reporting 0 bytes.
            self.master.commit(OP_UPDATE_CHUNK_SIZE,
                               chunk_handle=chunk_handle, size=new_size,
                               filename=owner_filename, file_size=owner_size)
            
            self.master.logger.info(f"Updated chunk {chunk_handle} size from {old_size} to {new_size}")
            return gfs_pb2.UpdateChunkSizeResponse(
                success=True,
                message=f"Chunk size updated from {old_size} to {new_size}"
            )

class MasterElectionServiceServicer(gfs_pb2_grpc.MasterElectionServiceServicer):
    def FetchMetadata(self, request, context):
        """Hand this node's metadata to a newly elected leader catching up."""
        m = self.master
        try:
            return gfs_pb2.FetchMetadataResponse(
                success=True,
                metadata_json=m._serialize_metadata(),
                version=m.metadata_version,
                term=m.current_term)
        except Exception as e:
            m.logger.error(f'FetchMetadata failed: {e}')
            return gfs_pb2.FetchMetadataResponse(success=False)

    def __init__(self, master: MultiMasterGFS):
        self.master = master

    def RequestVote(self, request, context):
        """Handle vote requests from candidates"""
        candidate_id = request.candidate_id
        term = request.term
        
        with self.master.state_lock:
            # Update term if newer
            if term > self.master.current_term:
                self.master.current_term = term
                self.master.voted_for = None
                self.master.state = MasterState.FOLLOWER
                self.master.leader_id = None
            
            # Grant vote if conditions are met
            vote_granted = False
            if (term == self.master.current_term and 
                (self.master.voted_for is None or self.master.voted_for == candidate_id)):
                
                self.master.voted_for = candidate_id
                self.master.last_heartbeat_time = time.time()
                vote_granted = True
                
                self.master.logger.info(f"Voted for {candidate_id} in term {term}")
            
            return gfs_pb2.ElectionResponse(
                voter_id=self.master.master_id,
                term=self.master.current_term,
                vote_granted=vote_granted
            )

    def SendHeartbeat(self, request, context):
        """Handle heartbeat from leader"""
        leader_id = request.master_id
        term = request.term
        
        with self.master.state_lock:
            # Update term if newer
            if term > self.master.current_term:
                self.master.current_term = term
                self.master.voted_for = None
                self.master.state = MasterState.FOLLOWER
                self.master.leader_id = leader_id
                
                # FIXED: Switch to normal timeout when receiving heartbeat from leader
                self.master._switch_to_normal_timeout()
            
            # Accept heartbeat if term is current
            acknowledge = False
            if term == self.master.current_term:
                self.master.state = MasterState.FOLLOWER
                self.master.leader_id = leader_id
                self.master.last_heartbeat_time = time.time()
                acknowledge = True
                
                # FIXED: Also switch to normal timeout on successful heartbeat
                if not self.master.has_connected_to_cluster:
                    self.master._switch_to_normal_timeout()
            
            return gfs_pb2.MasterHeartbeatResponse(
                success=acknowledge,
                term=self.master.current_term,
                acknowledge_primary=acknowledge,
                # Lets a freshly elected leader see that this peer holds
                # newer state and pull it before serving (ADR-007).
                metadata_version=self.master.metadata_version
            )

    def SyncMetadata(self, request, context):
        """Handle metadata synchronization from leader"""
        leader_id = request.master_id
        term = request.term
        metadata_json = request.metadata_json
        version = request.version
        
        with self.master.state_lock:
            # Only accept sync from current leader
            if (term == self.master.current_term and 
                leader_id == self.master.leader_id and
                version >= self.master.metadata_version):  # Changed from > to >=
                
                try:
                    # Parse and apply metadata
                    data = json.loads(metadata_json)
                    
                    with self.master.metadata_lock:
                        # Update files
                        self.master.files.clear()
                        for filename, file_data in data.get('files', {}).items():
                            self.master.files[filename] = FileMetadata(**file_data)
                        
                        # Update directories
                        self.master.directories.clear()
                        for dirname, dir_data in data.get('directories', {}).items():
                            self.master.directories[dirname] = DirectoryMetadata(**dir_data)
                        
                        # Update chunks
                        self.master.chunks.clear()
                        for handle, chunk_data in data.get('chunks', {}).items():
                            chunk_data['locations'] = set(chunk_data['locations'])
                            self.master.chunks[handle] = ChunkMetadata(**chunk_data)
                        
                        self.master.metadata_version = version
                        self.master.metadata_version = version
                        # A follower must make synced state durable too: if it wins the
                        # next election it serves this state as authoritative. ADR-001.
                        self.master.persist_synced_state()
                    
                    self.master.logger.info(f"Synced metadata from leader, version {version}")
                    
                    return gfs_pb2.SyncMetadataResponse(
                        success=True,
                        current_version=self.master.metadata_version
                    )
                    
                except Exception as e:
                    self.master.logger.error(f"Failed to sync metadata: {e}")
                    return gfs_pb2.SyncMetadataResponse(
                        success=False,
                        current_version=self.master.metadata_version,
                        message=str(e)
                    )
            
            # Add more detailed logging for debugging
            self.master.logger.debug(f"Rejected sync from {leader_id}: term={term} (current={self.master.current_term}), "
                                   f"leader_id={leader_id} (expected={self.master.leader_id}), "
                                   f"version={version} (current={self.master.metadata_version})")
            
            return gfs_pb2.SyncMetadataResponse(
                success=False,
                current_version=self.master.metadata_version,
                message="Invalid sync request"
            )

def serve(master_id: str = None, port: int = 9000, peers: List[str] = None,
          data_dir: str = None, max_workers: int = DEFAULT_MAX_WORKERS):
    """Start the GFS Master server with multi-master support"""
    metadata_file = None
    if data_dir:
        os.makedirs(data_dir, exist_ok=True)
        metadata_file = os.path.join(data_dir, 'metadata.json')

    master = MultiMasterGFS(master_id=master_id, port=port, peers=peers,
                            metadata_file=metadata_file)
    master.start_background_tasks()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers),
                         options=GRPC_SERVER_OPTIONS)
    gfs_pb2_grpc.add_MasterServiceServicer_to_server(
        MasterServiceServicer(master), server
    )
    gfs_pb2_grpc.add_MasterElectionServiceServicer_to_server(
        MasterElectionServiceServicer(master), server
    )
    
    listen_addr = f'[::]:{port}'
    server.add_insecure_port(listen_addr)
    
    server.start()
    logging.info(f"Multi-Master GFS server {master.master_id} started on port {port}")
    
    try:
        while True:
            time.sleep(86400)  # Sleep for a day
    except KeyboardInterrupt:
        logging.info(f"Shutting down GFS Master server {master.master_id}")
        master.stop_background_tasks()
        server.stop(0)

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='GFS Multi-Master Server')
    parser.add_argument('--master-id', type=str, help='Unique master identifier')
    parser.add_argument('--port', type=int, default=9000, help='Port to listen on')
    parser.add_argument('--peers', type=str, nargs='*', help='List of peer master addresses (host:port)')
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Directory for this master\'s metadata, oplog and checkpoints')
    parser.add_argument('--max-workers', type=int, default=DEFAULT_MAX_WORKERS,
                        help='gRPC server thread pool size')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    serve(args.master_id, args.port, args.peers or [],
          data_dir=args.data_dir, max_workers=args.max_workers)

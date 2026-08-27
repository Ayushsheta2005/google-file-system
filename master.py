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

# Configuration constants
CHUNK_SIZE = 64 * 1024 * 1024  # 64MB
REPLICATION_FACTOR = 3
HEARTBEAT_INTERVAL = 30  # seconds
LEASE_TIMEOUT = 60  # seconds
ELECTION_TIMEOUT_MIN = 5000  # milliseconds - increased for network delays
ELECTION_TIMEOUT_MAX = 8000  # milliseconds - increased for network delays
MASTER_HEARTBEAT_INTERVAL = 200  # milliseconds - increased for network delays
INITIAL_ELECTION_TIMEOUT = 10000  # milliseconds - 10 secs for startup discovery

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
        
        # Load existing metadata (now logger is available)
        self.load_metadata()
        
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
                channel = grpc.insecure_channel(f"{peer.address}:{peer.port}")
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
            
            self.logger.info(f"Became leader for term {self.current_term}")
            
            # Send immediate heartbeats to establish leadership
            self.send_heartbeats_to_peers()

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

    def grant_lease(self, chunk_handle: str) -> Optional[str]:
        """Grant lease to a chunk server for writing"""
        with self.lease_lock:
            if chunk_handle not in self.chunks:
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
                    
                    # Check for under-replicated chunks
                    for handle, chunk in self.chunks.items():
                        alive_locations = [
                            loc for loc in chunk.locations
                            if loc in self.chunk_servers and
                            current_time - self.chunk_servers[loc].last_heartbeat < HEARTBEAT_INTERVAL * 2
                        ]
                        
                        if len(alive_locations) < REPLICATION_FACTOR:
                            self.logger.warning(f"Chunk {handle} is under-replicated: {len(alive_locations)}/{REPLICATION_FACTOR}")
                
                # Save metadata periodically (only if leader)
                if self.is_leader():
                    self.save_metadata()
                    self.sync_metadata_to_peers()
                
            except Exception as e:
                self.logger.error(f"Background task error: {e}")
            
            time.sleep(30)  # Run every 30 seconds

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
                self.metadata_version += 1
                self.save_metadata()
                self.sync_metadata_to_peers()
        
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
            self.metadata_version += 1
            self.save_metadata()
            self.sync_metadata_to_peers()
            
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
            
            self.metadata_version += 1
            self.save_metadata()
            self.sync_metadata_to_peers()
            
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
                
                self.metadata_version += 1
                self.save_metadata()
                self.sync_metadata_to_peers()
                
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
                
                for dirpath in list(self.directories.keys()):
                    if dirpath.startswith(src_dir):
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
                
                self.metadata_version += 1
                self.save_metadata()
                self.sync_metadata_to_peers()
                
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
                self.metadata_version += 1
                self.save_metadata()
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
            self.master.metadata_version += 1
            self.master.save_metadata()
            self.master.sync_metadata_to_peers()
            
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
            self.master.metadata_version += 1
            self.master.save_metadata()
            self.master.sync_metadata_to_peers()
            
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
            
            return gfs_pb2.GetChunkLocationsResponse(
                success=True,
                locations=locations
            )

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
                locations=set(selected_servers)
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
            
            self.master.metadata_version += 1
            self.master.save_metadata()
            self.master.sync_metadata_to_peers()
            
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
            
            return gfs_pb2.HeartbeatResponse(success=True)

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
            
            self.master.metadata_version += 1
            self.master.save_metadata()
            self.master.sync_metadata_to_peers()
            
            self.master.logger.info(f"Updated file size: {normalized_path} from {old_size} to {new_size} bytes (write at offset {offset}, length {length})")
            
            return gfs_pb2.UpdateFileSizeResponse(
                success=True,
                message=f"File size updated to {new_size} bytes"
            )

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
                    break
            
            self.master.metadata_version += 1
            self.master.save_metadata()
            self.master.sync_metadata_to_peers()
            
            self.master.logger.info(f"Updated chunk {chunk_handle} size from {old_size} to {new_size}")
            return gfs_pb2.UpdateChunkSizeResponse(
                success=True,
                message=f"Chunk size updated from {old_size} to {new_size}"
            )

class MasterElectionServiceServicer(gfs_pb2_grpc.MasterElectionServiceServicer):
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
                acknowledge_primary=acknowledge
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
                        self.master.save_metadata()
                    
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

def serve(master_id: str = None, port: int = 9000, peers: List[str] = None):
    """Start the GFS Master server with multi-master support"""
    master = MultiMasterGFS(master_id=master_id, port=port, peers=peers)
    master.start_background_tasks()
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
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
    args = parser.parse_args()
    
    serve(args.master_id, args.port, args.peers or [])
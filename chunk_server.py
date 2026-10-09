#!/usr/bin/env python3

import grpc
from concurrent import futures
import threading
import time
import logging
import os
import hashlib
import uuid
import shutil
import argparse
import socket
from typing import Dict, Optional, List
from dataclasses import dataclass

import gfs_pb2
import gfs_pb2_grpc
from ec_storage import ShardStore, ECWorker

# Configuration constants -- see gfs_config.py and docs/adr/
from gfs_config import (
    CHUNK_SIZE,
    HEARTBEAT_INTERVAL,
    LEADER_DISCOVERY_INTERVAL,
    LEADER_DISCOVERY_RETRY_INTERVAL,
    DEFAULT_MAX_WORKERS,
    GRPC_SERVER_OPTIONS,
    GRPC_CHANNEL_OPTIONS,
)

@dataclass
class ChunkInfo:
    handle: str
    size: int
    checksum: str
    version: int = 1

def get_local_ip():
    """Get local IP address"""
    try:
        # Connect to a remote address to determine local IP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"

class GFSChunkServer:
    def __init__(self, server_id: str = None, port: int = 9001, 
                 master_addresses: List[str] = None, 
                 storage_dir: str = "chunks",
                 external_address: str = None,
                 external_port: int = None,
                 log_level: str = "DEBUG"):
        self.server_id = server_id or f"chunkserver_{uuid.uuid4().hex[:8]}"
        self.port = port
        self.master_addresses = master_addresses or []
        self.storage_dir = storage_dir
        # NEW: Support for external address (for Docker networking)
        self.external_address = external_address
        self.external_port = external_port or port
        
        # Initialize logging first
        os.makedirs(storage_dir, exist_ok=True)
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(os.path.join(storage_dir, f'{self.server_id}.log'))
            ]
        )
        logging.getLogger().setLevel(getattr(logging, log_level))
        self.logger = logging.getLogger(__name__)
        
        # Log all initialization details
        self.logger.info("=" * 80)
        self.logger.info(f"🚀 INITIALIZING GFS CHUNK SERVER")
        self.logger.info("=" * 80)
        self.logger.info(f"📋 Server ID: {self.server_id}")
        self.logger.info(f"🌐 Port: {self.port}")
        self.logger.info(f"💾 Storage Directory: {self.storage_dir}")
        self.logger.info(f"📡 Master Addresses Received: {len(self.master_addresses)}")
        for i, addr in enumerate(self.master_addresses):
            self.logger.info(f"   Master {i+1}: {addr}")
        
        # Get network information
        local_ip = get_local_ip()
        self.logger.info(f"🌍 Local IP Address: {local_ip}")
        if self.external_address:
            self.logger.info(f"🌐 External Address: {self.external_address}:{self.external_port}")
        self.logger.info(f"🔧 Chunk Size: {CHUNK_SIZE // (1024*1024)} MB")
        self.logger.info(f"💓 Heartbeat Interval: {HEARTBEAT_INTERVAL} seconds")
        
        # Create storage directory
        os.makedirs(storage_dir, exist_ok=True)
        self.logger.info(f"📁 Storage directory created/verified: {os.path.abspath(storage_dir)}")
        
        # Chunk management
        self.chunks: Dict[str, ChunkInfo] = {}
        self.chunk_lock = threading.RLock()

        
        # old
        # # Master connection management
        self.current_master = None
        self.master_stub = None
        self.master_channel = None
        # Guards current_master / master_stub. It had been commented out
        # while three call sites still used it, so every code path that
        # reached them raised AttributeError. Those paths only run when no
        # leader is currently known, which is why it survived: the happy
        # path never touches them. Re-entrant because connect_to_master()
        # can be reached from a call that already holds it.
        self.master_lock = threading.RLock()

        # GFS Write Protocol - Data caching and leases
        self.cached_data: Dict[str, bytes] = {}  # data_id -> data
        self.data_cache_lock = threading.RLock()
        self.primary_leases: Dict[str, float] = {}  # chunk_handle -> lease_expiry
        self.mutation_serial = 0  # For ordering mutations
        self.serial_lock = threading.Lock()
        
        # Multi-master connection management
        self.current_leader_address = None
        self.master_channels = {}  # address -> channel
        self.master_stubs = {}     # address -> stub
        self.leader_stub = None    # Current leader stub
        # Backs off only while a leader is known. A chunk server that starts
        # before the masters finish electing used to sleep a flat 10s before
        # retrying, which dominated cluster start-up time. See ADR-008.
        self.leader_discovery_interval = LEADER_DISCOVERY_INTERVAL
        self.leader_discovery_retry_interval = LEADER_DISCOVERY_RETRY_INTERVAL
        
        # Erasure-coded shards this server holds (ADR-005). Kept separate
        # from self.chunks: a shard is not a chunk, and conflating them would
        # let the master count a single shard as a whole replica.
        self.shard_store = ShardStore(storage_dir, logger=self.logger)
        self.ec_worker = ECWorker(
            self.shard_store,
            fetch_shard=self._fetch_shard_remote,
            push_shard=self._push_shard_remote,
            logger=self.logger)
        self._ec_lock = threading.RLock()
        self._ec_in_progress = set()

        # Clone bookkeeping (ADR-003)
        self._clone_lock = threading.RLock()
        self._clones_in_progress = set()
        self._max_concurrent_clones = 2

        # Background tasks
        self.running = False
        self.heartbeat_thread = None
        self.leader_discovery_thread = None
        
        # Initialize
        self.load_existing_chunks()
        self.connect_to_all_masters()
        self.discover_leader()
        
        self.logger.info("=" * 80)
        self.logger.info("✅ CHUNK SERVER INITIALIZATION COMPLETE")
        self.logger.info("=" * 80)

    def connect_to_all_masters(self):
        """Establish connections to all master servers"""
        for master_address in self.master_addresses:
            try:
                channel = grpc.insecure_channel(master_address, options=GRPC_CHANNEL_OPTIONS)
                stub = gfs_pb2_grpc.MasterServiceStub(channel)
                self.master_channels[master_address] = channel
                self.master_stubs[master_address] = stub
                self.logger.info(f"Connected to master at {master_address}")
            except Exception as e:
                self.logger.warning(f"Failed to connect to master {master_address}: {e}")
    
    def discover_leader(self):
        """Discover which master is currently the leader"""
        for master_address in self.master_addresses:
            if master_address not in self.master_stubs:
                continue
                
            try:
                stub = self.master_stubs[master_address]
                # Use GetPrimaryMaster RPC to check if this master is leader
                response = stub.GetPrimaryMaster(
                    gfs_pb2.GetPrimaryMasterRequest(), 
                    timeout=2.0
                )
                
                if response.success and response.primary_master and response.primary_master.is_primary:
                    # Found the leader
                    self.current_leader_address = master_address
                    self.leader_stub = stub
                    self.master_stub = stub
                    self.logger.info(f"Discovered leader: {master_address} (ID: {response.primary_master.master_id})")
                    return True
                elif response.success and response.primary_master:
                    # This master knows who the leader is, but it's not this one
                    self.logger.debug(f"{master_address} redirected to leader: {response.primary_master.master_id}")
                else:
                    self.logger.debug(f"{master_address} reports no leader available")
                    
            except Exception as e:
                self.logger.debug(f"Failed to check leader status for {master_address}: {e}")
                
        self.logger.warning("No leader found among masters")
        return False
    
    def switch_to_new_leader(self, new_leader_address: str):
        """Switch to a new leader master"""
        if new_leader_address == self.current_leader_address:
            return  # Already connected to this leader
            
        if new_leader_address in self.master_stubs:
            old_leader = self.current_leader_address
            self.current_leader_address = new_leader_address
            self.leader_stub = self.master_stubs[new_leader_address]
            self.logger.info(f"Switched leader from {old_leader} to {new_leader_address}")
        else:
            self.logger.error(f"Cannot switch to unknown master: {new_leader_address}")
    
    def leader_discovery_loop(self):
        """Background loop for leader discovery and failover"""
        while self.running:
            # Poll fast while leaderless (start-up, or mid-failover), slowly
            # once a leader is known and answering.
            time.sleep(self.leader_discovery_interval if self.leader_stub
                       else self.leader_discovery_retry_interval)

            # Check if current leader is still alive
            if self.current_leader_address and self.leader_stub:
                try:
                    response = self.leader_stub.GetPrimaryMaster(
                        gfs_pb2.GetPrimaryMasterRequest(), 
                        timeout=2.0
                    )
                    
                    if not (response.success and response.primary_master and response.primary_master.is_primary):
                        # Current leader is no longer leader, need to find new one
                        self.logger.warning(f"Current leader {self.current_leader_address} is no longer primary")
                        self.current_leader_address = None
                        self.leader_stub = None
                        
                except Exception as e:
                    # Leader might have failed
                    self.logger.warning(f"Lost connection to leader {self.current_leader_address}: {e}")
                    self.current_leader_address = None
                    self.leader_stub = None
            
            # If no leader, try to discover one
            if not self.current_leader_address:
                self.logger.info("Searching for new leader...")
                self.discover_leader()

    def load_existing_chunks(self):
        """Load existing chunks from storage directory"""
        try:
            for filename in os.listdir(self.storage_dir):
                if filename.startswith("chunk_") and filename.endswith(".data"):
                    chunk_handle = filename[:-5]  # Remove .data extension
                    chunk_path = os.path.join(self.storage_dir, filename)
                    
                    # Calculate size and checksum
                    size = os.path.getsize(chunk_path)
                    checksum = self.calculate_checksum(chunk_path)
                    
                    self.chunks[chunk_handle] = ChunkInfo(
                        handle=chunk_handle,
                        size=size,
                        checksum=checksum
                    )
                    
            self.logger.info(f"Loaded {len(self.chunks)} existing chunks")
        except Exception as e:
            self.logger.error(f"Failed to load existing chunks: {e}")

    def find_leader_master(self) -> Optional[str]:
        """Find the current leader master from the list of masters"""
        self.logger.info(f"Scanning {len(self.master_addresses)} master addresses: {self.master_addresses}")
        
        for i, master_addr in enumerate(self.master_addresses):
            self.logger.debug(f"Attempting connection {i+1}/{len(self.master_addresses)} to master at {master_addr}...")
            
            channel = None
            try:
                self.logger.debug(f"Creating gRPC channel to {master_addr}")
                channel = grpc.insecure_channel(master_addr, options=GRPC_CHANNEL_OPTIONS)
                
                self.logger.debug(f"Creating MasterService stub for {master_addr}")
                stub = gfs_pb2_grpc.MasterServiceStub(channel)
                
                self.logger.debug(f"Preparing heartbeat request for {master_addr}")
                # Try to make a simple request to check if this is the leader
                request = gfs_pb2.HeartbeatRequest(
                    server_id=self.server_id,
                    chunks=[],
                    available_space=self.get_available_space()
                )
                
                self.logger.debug(f"Sending test heartbeat to {master_addr} with 5s timeout...")
                # Set a short timeout for leader detection
                response = stub.Heartbeat(request, timeout=5.0)
                
                self.logger.debug(f"Received response from {master_addr}: success={response.success}, message='{response.message}'")
                
                if response.success:
                    self.logger.info(f"✓ Found leader master at {master_addr}")
                    channel.close()
                    return master_addr
                else:
                    self.logger.debug(f"✗ Master at {master_addr} rejected heartbeat: {response.message}")
                    channel.close()
                    
            except grpc.RpcError as e:
                self.logger.debug(f"✗ gRPC error connecting to {master_addr}: {e.code()} - {e.details()}")
                if channel:
                    try:
                        channel.close()
                    except:
                        pass
                continue
            except Exception as e:
                self.logger.error(f"✗ Unexpected error connecting to {master_addr}: {type(e).__name__}: {e}")
                if channel:
                    try:
                        channel.close()
                    except:
                        pass
                continue
        
        self.logger.warning("❌ No available master found after scanning all addresses")
        return None

    def connect_to_master(self) -> bool:
        """Connect to available master server with detailed logging"""
        with self.master_lock:
            self.logger.info("🔍 ATTEMPTING TO CONNECT TO MASTER SERVERS")
            self.logger.info("-" * 60)
            
            for i, master_addr in enumerate(self.master_addresses):
                self.logger.info(f"🌐 Trying Master {i+1}/{len(self.master_addresses)}: {master_addr}")
                
                try:
                    # Parse address
                    if ':' in master_addr:
                        host, port = master_addr.rsplit(':', 1)
                        port = int(port)
                    else:
                        host, port = master_addr, 9000
                    
                    self.logger.info(f"   📍 Parsed - Host: {host}, Port: {port}")
                    
                    # Create channel with timeout
                    channel = grpc.insecure_channel(
                        f"{host}:{port}",
                        options=GRPC_CHANNEL_OPTIONS
                    )
                    
                    # FIXED: Use MasterServiceStub instead of GFSMasterStub
                    stub = gfs_pb2_grpc.MasterServiceStub(channel)
                    
                    # Test connection with a simple request
                    self.logger.info(f"   🔌 Testing connection to {host}:{port}...")
                    
                    # FIXED: Use external address if available, otherwise use local IP
                    if self.external_address:
                        server_address = f"{self.external_address}:{self.external_port}"
                    else:
                        local_ip = get_local_ip()
                        server_address = f"{local_ip}:{self.port}"
                    
                    # These field names (chunk_server_id / chunk_server_address
                    # / available_chunks) match the HeartbeatRequest that is
                    # commented out in gfs.proto, not the one in use -- so this
                    # call raised ValueError every time it ran. It only runs on
                    # the no-leader-known path, which is why it went unnoticed.
                    request = gfs_pb2.HeartbeatRequest(
                        server_id=self.server_id,
                        server_address=server_address,
                        chunks=[gfs_pb2.ChunkHandle(handle=h)
                                for h in self.chunks.keys()],
                        total_chunks=len(self.chunks),
                        available_space=self.get_available_space(),
                        port=self.external_port
                    )
                    
                    self.logger.info(f"   📤 Sending test heartbeat:")
                    self.logger.info(f"      - Server ID: {self.server_id}")
                    self.logger.info(f"      - Server Address: {server_address}")
                    self.logger.info(f"      - Available Chunks: {len(self.chunks)}")
                    self.logger.info(f"      - Available Space: {self.get_available_space()} bytes")
                    
                    # Send with timeout
                    response = stub.Heartbeat(request, timeout=10)
                    
                    # Close any existing connection
                    if self.master_channel:
                        self.master_channel.close()
                    
                    # Store connection details.
                    #
                    # Two connection-tracking schemes coexist in this class:
                    # the original single-master pair (master_stub /
                    # current_master) and the multi-master pair added later
                    # (leader_stub / current_leader_address). They were never
                    # reconciled, so this fallback path populated the first
                    # pair while send_heartbeat() read the second -- meaning
                    # that whenever leader discovery failed and this ran, the
                    # chunk server reconnected successfully and then
                    # heartbeated into a None stub forever. Set both.
                    self.master_channel = channel
                    self.master_stub = stub
                    self.current_master = f"{host}:{port}"
                    self.leader_stub = stub
                    self.current_leader_address = self.current_master
                    self.master_channels[self.current_master] = channel
                    self.master_stubs[self.current_master] = stub
                    
                    self.logger.info(f"   ✅ Successfully connected to master: {self.current_master}")
                    if hasattr(response, 'status'):
                        self.logger.info(f"   📥 Master response status: {response.status}")
                    if hasattr(response, 'message') and response.message:
                        self.logger.info(f"   💬 Master message: {response.message}")
                    
                    self.logger.info("🎉 MASTER CONNECTION ESTABLISHED")
                    self.logger.info("-" * 60)
                    return True
                    
                except grpc.RpcError as e:
                    self.logger.warning(f"   ❌ gRPC Error connecting to {master_addr}: {e.code()} - {e.details()}")
                except Exception as e:
                    self.logger.warning(f"   ❌ Failed to connect to {master_addr}: {str(e)}")
                
                self.logger.info(f"   ⏭️  Moving to next master...")
            
            self.logger.error("🚨 FAILED TO CONNECT TO ANY MASTER SERVER")
            self.logger.error("-" * 60)
            return False

    def get_master_stub(self):
        """Get the master stub, reconnecting if necessary"""
        self.logger.debug("Checking master stub availability...")
        with self.master_lock:
            if not self.master_stub or not self.current_master:
                self.logger.info("No active master connection, attempting to connect...")
                success = self.connect_to_master()
                if not success:
                    self.logger.error("Failed to establish master connection")
                    return None
                self.logger.debug(f"Master stub ready for {self.current_master}")
            else:
                self.logger.debug(f"Using existing connection to {self.current_master}")
            return self.master_stub

    def get_chunk_path(self, chunk_handle: str) -> str:
        """Get file path for a chunk"""
        return os.path.join(self.storage_dir, f"{chunk_handle}.data")

    def calculate_checksum(self, file_path: str) -> str:
        """Calculate MD5 checksum of a file"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
        except Exception:
            return ""
        return hash_md5.hexdigest()

    def get_available_space(self) -> int:
        """Get available disk space in bytes"""
        try:
            stat = shutil.disk_usage(self.storage_dir)
            return stat.free
        except Exception:
            return 0

    def send_heartbeat(self):
        """Send heartbeat to master with detailed logging"""
        try:
            if not self.leader_stub:
                self.logger.warning("⚠️  No master connection available for heartbeat")
                if not self.connect_to_master():
                    return

            with self.chunk_lock:
                chunk_handles = [
                    gfs_pb2.ChunkHandle(handle=handle) 
                    for handle in self.chunks.keys()
                ]

            
            # FIXED: Use external address if available, otherwise use local IP
            if self.external_address:
                server_address = f"{self.external_address}:{self.external_port}"
            else:
                local_ip = get_local_ip()
                server_address = f"{local_ip}:{self.port}"
            
            shard_reports = [
                gfs_pb2.ShardLocation(server_id=self.server_id,
                                      address=server_address.rsplit(':', 1)[0],
                                      port=self.external_port,
                                      shard_index=si.index,
                                      chunk_handle=si.handle,
                                      original_length=si.original_length,
                                      data_shards=si.k,
                                      parity_shards=si.m)
                for si in self.shard_store.held()
            ]

            request = gfs_pb2.HeartbeatRequest(
                server_id=self.server_id,
                server_address=server_address,
                chunks=chunk_handles,
                total_chunks=len(self.chunks),
                available_space=self.get_available_space(),
                port = self.external_port,
                shards=shard_reports
            )
            
            # Log heartbeat details every 10th heartbeat to avoid spam
            if hasattr(self, 'heartbeat_count'):
                self.heartbeat_count += 1
            else:
                self.heartbeat_count = 1
                
            if self.heartbeat_count % 10 == 1:  # Log first and every 10th heartbeat
                self.logger.info(f"💓 Sending heartbeat #{self.heartbeat_count} to master {self.current_leader_address}")
                self.logger.info(f"   📊 Chunks: {len(self.chunks)}, Space: {self.get_available_space()} bytes")
                self.logger.info(f"   📍 Reporting address: {server_address}")
            
            response = self.leader_stub.Heartbeat(request, timeout=10)
            
            if self.heartbeat_count % 10 == 1:
                if hasattr(response, 'status'):
                    self.logger.info(f"   ✅ Heartbeat response status: {response.status}")
                if hasattr(response, 'message') and response.message:
                    self.logger.info(f"   💬 Master message: {response.message}")

            # The master piggybacks work on the heartbeat reply (ADR-003).
            # Both are handled off this thread: the heartbeat loop must keep
            # ticking, and a 64 MB clone would otherwise stall it long enough
            # for the master to declare this server dead.
            if response.chunks_to_delete:
                self._handle_deletions(
                    [h.handle for h in response.chunks_to_delete])
            if response.replication_tasks:
                self._handle_replication_tasks(response.replication_tasks)
            if response.ec_encode_tasks:
                self._handle_ec_encode_tasks(response.ec_encode_tasks)
            if response.ec_repair_tasks:
                self._handle_ec_repair_tasks(response.ec_repair_tasks)
            if response.shards_to_delete:
                for req in response.shards_to_delete:
                    self.shard_store.delete(req.chunk_handle.handle, req.shard_index)

        except grpc.RpcError as e:
            self.logger.error(f"❌ Heartbeat failed: {e.code()} - {e.details()}")
            self.current_leader_address = None
            self.leader_stub = None
        except Exception as e:
            self.logger.error(f"❌ Heartbeat error: {str(e)}")

    # def send_heartbeat(self):
    #     """Send heartbeat to current leader master"""
    #     if not self.leader_stub or not self.current_leader_address:
    #         self.logger.debug("No leader available for heartbeat")
    #         return
            
    #     try:
    #         with self.chunk_lock:
    #             chunk_handles = [
    #                 gfs_pb2.ChunkHandle(handle=handle) 
    #                 for handle in self.chunks.keys()
    #             ]
            
    #         request = gfs_pb2.HeartbeatRequest(
    #             server_id=self.server_id,
    #             chunks=chunk_handles,
    #             available_space=self.get_available_space(),
    #             port=self.port  
    #         )
            
    #         response = self.leader_stub.Heartbeat(request)
    #         if response.success:
    #             self.logger.info(f"Heartbeat sent successfully to leader {self.current_leader_address}")
    #         else:
    #             self.logger.info(f"Heartbeat rejected by {self.current_leader_address}")
    #             # Leader might have changed, trigger discovery
    #             self.current_leader_address = None
    #             self.leader_stub = None
                
    #     except Exception as e:
    #         self.logger.error(f"Heartbeat error with {self.current_leader_address}: {e}")
    #         # Connection failed, trigger leader rediscovery
    #         self.current_leader_address = None
    #         self.leader_stub = None


    # ==================================================================
    # Re-replication and garbage collection (ADR-003, ADR-004)
    # ==================================================================

    def _handle_deletions(self, handles):
        """Reclaim chunks the master says this server should no longer hold."""
        removed = 0
        for handle in handles:
            try:
                with self.chunk_lock:
                    self.chunks.pop(handle, None)
                path = self.get_chunk_path(handle)
                if os.path.exists(path):
                    os.remove(path)
                    removed += 1
            except OSError as e:
                self.logger.error(f'failed to reclaim chunk {handle}: {e}')
        if removed:
            self.logger.info(f'🗑️  reclaimed {removed} chunk(s) on master instruction')

    def _handle_replication_tasks(self, tasks):
        """Start a clone per task, bounded by a small worker pool.

        Cloning runs in the background so the heartbeat loop keeps ticking;
        a 64 MB transfer on the heartbeat thread would look like a dead
        server to the master and trigger exactly the re-replication storm
        this is supposed to resolve.
        """
        for task in tasks:
            handle = task.chunk_handle.handle
            with self._clone_lock:
                if handle in self._clones_in_progress:
                    continue
                if len(self._clones_in_progress) >= self._max_concurrent_clones:
                    self.logger.debug(f'clone slots full, deferring {handle}')
                    continue
                self._clones_in_progress.add(handle)

            sources = [(loc.address, loc.port, loc.server_id)
                       for loc in task.source_locations]
            t = threading.Thread(
                target=self._clone_chunk,
                args=(handle, sources, task.chunk_version),
                daemon=True)
            t.start()

    def _clone_chunk(self, handle: str, sources, expected_version: int):
        """Pull a chunk from a peer replica and install it locally.

        The *target* pulls rather than a source pushing: the target is by
        definition the node with spare capacity, and a pull means a dead
        target costs nobody else bandwidth.

        Written to a temp file and renamed into place, so a clone that dies
        halfway cannot leave a short chunk that looks complete and then gets
        reported as a healthy replica.
        """
        try:
            for address, port, source_id in sources:
                try:
                    data = self._fetch_chunk_from(address, port, handle)
                    if data is None:
                        continue

                    final = self.get_chunk_path(handle)
                    tmp = final + '.clone.tmp'
                    with open(tmp, 'wb') as f:
                        f.write(data)
                        f.flush()
                        os.fsync(f.fileno())
                    os.rename(tmp, final)

                    with self.chunk_lock:
                        self.chunks[handle] = ChunkInfo(
                            handle=handle,
                            size=len(data),
                            version=expected_version,
                            checksum=self.calculate_checksum(final))

                    self.logger.info(
                        f'📥 cloned chunk {handle} ({len(data)} bytes) from {source_id}')
                    return True
                except Exception as e:
                    self.logger.warning(
                        f'clone of {handle} from {source_id} failed: {e}; trying next source')
                    continue

            self.logger.error(f'clone of {handle} failed from every source')
            return False
        finally:
            # Always release the slot. Leaking it would quietly shrink this
            # server's clone capacity to zero over time.
            with self._clone_lock:
                self._clones_in_progress.discard(handle)

    def _fetch_chunk_from(self, address: str, port: int, handle: str):
        """Read a whole chunk from a peer using the ordinary ReadChunk path."""
        target = f'{address}:{port}'
        with grpc.insecure_channel(target, options=GRPC_CHANNEL_OPTIONS) as channel:
            stub = gfs_pb2_grpc.ChunkServerServiceStub(channel)
            resp = stub.ReadChunk(
                gfs_pb2.ReadChunkRequest(
                    chunk_handle=gfs_pb2.ChunkHandle(handle=handle),
                    offset=0,
                    length=CHUNK_SIZE),
                timeout=120)
            if not resp.success:
                raise RuntimeError(resp.message or 'read refused')
            return resp.data


    # ==================================================================
    # Erasure coding (ADR-005)
    # ==================================================================

    def _push_shard_remote(self, address, port, handle, index, data,
                           k, m, original_length, version) -> bool:
        """Place one shard on a peer (or locally, if we are the target)."""
        if f'{address}:{port}' in (f'{get_local_ip()}:{self.external_port}',
                                   f'127.0.0.1:{self.port}') or port == self.external_port:
            try:
                self.shard_store.put(handle, index, data, k, m,
                                     original_length, version)
                return True
            except OSError as e:
                self.logger.error(f'local shard store failed: {e}')
                return False
        try:
            with grpc.insecure_channel(f'{address}:{port}',
                                       options=GRPC_CHANNEL_OPTIONS) as ch:
                stub = gfs_pb2_grpc.ChunkServerServiceStub(ch)
                resp = stub.StoreShard(gfs_pb2.StoreShardRequest(
                    chunk_handle=gfs_pb2.ChunkHandle(handle=handle),
                    shard_index=index, data=data, chunk_version=version,
                    original_length=original_length,
                    data_shards=k, parity_shards=m), timeout=120)
                return resp.success
        except grpc.RpcError as e:
            self.logger.warning(f'StoreShard to {address}:{port} failed: {e.code()}')
            return False

    def _fetch_shard_remote(self, address, port, handle, index):
        local = self.shard_store.get(handle, index)
        if local is not None:
            return local
        try:
            with grpc.insecure_channel(f'{address}:{port}',
                                       options=GRPC_CHANNEL_OPTIONS) as ch:
                stub = gfs_pb2_grpc.ChunkServerServiceStub(ch)
                resp = stub.ReadShard(gfs_pb2.ReadShardRequest(
                    chunk_handle=gfs_pb2.ChunkHandle(handle=handle),
                    shard_index=index), timeout=120)
                return resp.data if resp.success else None
        except grpc.RpcError:
            return None

    def _handle_ec_encode_tasks(self, tasks):
        for task in tasks:
            handle = task.chunk_handle.handle
            with self._ec_lock:
                if handle in self._ec_in_progress:
                    continue
                self._ec_in_progress.add(handle)
            targets = [{'server_id': t.server_id, 'address': t.address,
                        'port': t.port} for t in task.shard_targets]
            threading.Thread(target=self._run_ec_encode, daemon=True,
                             args=(handle, task.data_shards, task.parity_shards,
                                   task.chunk_version, targets)).start()

    def _run_ec_encode(self, handle, k, m, version, targets):
        try:
            data = self.read_chunk_data(handle, 0, CHUNK_SIZE)
            if data is None:
                self.logger.warning(f'EC encode skipped: {handle} not held here')
                return
            ok, detail = self.ec_worker.encode_and_distribute(
                handle, data, k, m, version, targets)
            if ok:
                # The master drops the replicas only after it sees all k+m
                # shards reported, so a partial encode degrades to "wasted
                # work" rather than "data loss".
                self.logger.info(f'🧮 EC encoded {handle}: {detail}')
            else:
                self.logger.error(f'EC encode of {handle} incomplete: {detail}')
        except Exception as e:
            self.logger.error(f'EC encode of {handle} raised: {e}')
        finally:
            with self._ec_lock:
                self._ec_in_progress.discard(handle)

    def _handle_ec_repair_tasks(self, tasks):
        for task in tasks:
            handle = task.chunk_handle.handle
            key = f'{handle}#repair{task.shard_index}'
            with self._ec_lock:
                if key in self._ec_in_progress:
                    continue
                self._ec_in_progress.add(key)
            sources = [{'server_id': s.server_id, 'address': s.address,
                        'port': s.port, 'shard_index': s.shard_index}
                       for s in task.sources]
            threading.Thread(target=self._run_ec_repair, daemon=True,
                             args=(key, handle, task.shard_index, task.data_shards,
                                   task.parity_shards, task.original_length,
                                   sources)).start()

    def _run_ec_repair(self, key, handle, index, k, m, original_length, sources):
        try:
            ok, detail = self.ec_worker.reconstruct_shard(
                handle, index, k, m, original_length, sources)
            if ok:
                self.logger.info(f'🔧 EC repair {handle}#{index}: {detail}')
            else:
                self.logger.error(f'EC repair {handle}#{index} failed: {detail}')
        except Exception as e:
            self.logger.error(f'EC repair {handle}#{index} raised: {e}')
        finally:
            with self._ec_lock:
                self._ec_in_progress.discard(key)

    def heartbeat_loop(self):
        """Background heartbeat loop with persistent connection attempts"""
        self.logger.info("🚀 Starting heartbeat loop...")
        
        loop_count = 0
        while self.running:
            loop_count += 1
            self.logger.debug(f"Heartbeat loop iteration #{loop_count}")
            
            try:
                # Try to send heartbeat - this will keep retrying until successful
                self.logger.debug("Calling send_heartbeat()...")
                self.send_heartbeat()
                
                # If successful, wait for the normal heartbeat interval
                if self.running:  # Check if still running after send_heartbeat returns
                    self.logger.info(f"😴 Heartbeat successful, sleeping for {HEARTBEAT_INTERVAL} seconds...")
                    time.sleep(HEARTBEAT_INTERVAL)
                    
            except Exception as e:
                self.logger.error(f"💥 Unexpected error in heartbeat loop: {type(e).__name__}: {e}")
                self.logger.info("⏳ Waiting 5 seconds before retry...")
                time.sleep(5)  # Wait before retrying

    def start_background_tasks(self):
        """Start heartbeat and leader discovery threads"""
        self.running = True
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        # Start leader discovery thread
        self.leader_discovery_thread = threading.Thread(target=self.leader_discovery_loop)
        self.leader_discovery_thread.daemon = True
        self.leader_discovery_thread.start()

    def stop_background_tasks(self):
        """Stop all background threads"""
        self.running = False
        
        if self.heartbeat_thread:
            self.heartbeat_thread.join()
            
        if self.leader_discovery_thread:
            self.leader_discovery_thread.join()
            
        # Close all master connections
        for channel in self.master_channels.values():
            try:
                channel.close()
            except:
                pass

    def start_heartbeat(self):
        """Start heartbeat thread"""
        self.logger.info(f"🎯 Starting heartbeat thread for server {self.server_id}")
        self.running = True
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop, name="HeartbeatThread")
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        self.logger.info("✅ Heartbeat thread started successfully")

    def stop_heartbeat(self):
        """Stop heartbeat thread"""
        self.running = False
        if self.heartbeat_thread:
            self.heartbeat_thread.join()
        
        # Close master connection
        with self.master_lock:
            if self.master_channel:
                try:
                    self.master_channel.close()
                except:
                    pass

    def create_chunk(self, chunk_handle: str) -> bool:
        """Create a new empty chunk"""
        try:
            chunk_path = self.get_chunk_path(chunk_handle)
            
            # Create empty file
            with open(chunk_path, 'wb') as f:
                pass
            
            with self.chunk_lock:
                self.chunks[chunk_handle] = ChunkInfo(
                    handle=chunk_handle,
                    size=0,
                    checksum=self.calculate_checksum(chunk_path)
                )
            
            self.logger.info(f"Created chunk: {chunk_handle}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create chunk {chunk_handle}: {e}")
            return False

    def read_chunk_data(self, chunk_handle: str, offset: int, length: int) -> Optional[bytes]:
        """Read data from a chunk"""
        try:
            chunk_path = self.get_chunk_path(chunk_handle)
            
            if not os.path.exists(chunk_path):
                return None
            
            with open(chunk_path, 'rb') as f:
                f.seek(offset)
                data = f.read(length)
            
            return data
        except Exception as e:
            self.logger.error(f"Failed to read chunk {chunk_handle}: {e}")
            return None

    def write_chunk_data(self, chunk_handle: str, offset: int, data: bytes) -> bool:
        """Write data to a chunk"""
        try:
            chunk_path = self.get_chunk_path(chunk_handle)
            
            # Create chunk if it doesn't exist
            if not os.path.exists(chunk_path):
                self.create_chunk(chunk_handle)
            
            # Write data
            with open(chunk_path, 'r+b') as f:
                f.seek(offset)
                f.write(data)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
            
            # Update chunk info
            with self.chunk_lock:
                new_size = os.path.getsize(chunk_path)
                new_checksum = self.calculate_checksum(chunk_path)
                
                if chunk_handle in self.chunks:
                    self.chunks[chunk_handle].size = new_size
                    self.chunks[chunk_handle].checksum = new_checksum
                else:
                    self.chunks[chunk_handle] = ChunkInfo(
                        handle=chunk_handle,
                        size=new_size,
                        checksum=new_checksum
                    )
            
            self.logger.debug(f"Wrote {len(data)} bytes to chunk {chunk_handle} at offset {offset}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write chunk {chunk_handle}: {e}")
            return False

    def append_chunk_data(self, chunk_handle: str, data: bytes) -> Optional[int]:
        """Append data to a chunk and return the offset"""
        try:
            chunk_path = self.get_chunk_path(chunk_handle)
            
            # Create chunk if it doesn't exist
            if not os.path.exists(chunk_path):
                self.create_chunk(chunk_handle)
            
            # Get current size (append offset)
            current_size = os.path.getsize(chunk_path)
            
            # Check if append would exceed chunk size limit
            if current_size + len(data) > CHUNK_SIZE:
                self.logger.warning(f"Append would exceed chunk size limit for {chunk_handle}")
                return None
            
            # Append data
            with open(chunk_path, 'ab') as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            
            # Update chunk info
            with self.chunk_lock:
                new_size = os.path.getsize(chunk_path)
                new_checksum = self.calculate_checksum(chunk_path)
                
                if chunk_handle in self.chunks:
                    self.chunks[chunk_handle].size = new_size
                    self.chunks[chunk_handle].checksum = new_checksum
                else:
                    self.chunks[chunk_handle] = ChunkInfo(
                        handle=chunk_handle,
                        size=new_size,
                        checksum=new_checksum
                    )
            
            self.logger.debug(f"Appended {len(data)} bytes to chunk {chunk_handle} at offset {current_size}")
            return current_size
        except Exception as e:
            self.logger.error(f"Failed to append to chunk {chunk_handle}: {e}")
            return None

    def is_primary_for_chunk(self, chunk_handle: str) -> bool:
        """Check if this server is primary for the given chunk"""
        with self.chunk_lock:
            # Check if we have an active lease for this chunk
            if chunk_handle in self.primary_leases:
                lease_expiry = self.primary_leases[chunk_handle]
                is_valid = time.time() < lease_expiry
                self.logger.debug(f"Lease check for {chunk_handle}: server={self.server_id}, expiry={lease_expiry}, valid={is_valid}")
                if is_valid:
                    return True
                else:
                    # Lease expired, remove it
                    self.logger.info(f"Lease expired for chunk {chunk_handle} on {self.server_id}")
                    del self.primary_leases[chunk_handle]
            
            # Fallback: If no lease system, assume first chunk server (chunk1) is primary
            # AND the chunk must exist or be creatable
            fallback_primary = (chunk_handle in self.chunks or self.server_id == "chunk1") and self.server_id == "chunk1"
            
            self.logger.debug(f"Primary check for {chunk_handle}: server={self.server_id}, has_lease={chunk_handle in self.primary_leases}, fallback={fallback_primary}")
            return fallback_primary
    
    def set_primary_lease(self, chunk_handle: str, lease_duration: float = 60.0):
        """Set this server as primary for a chunk"""
        with self.chunk_lock:
            self.primary_leases[chunk_handle] = time.time() + lease_duration
            self.logger.info(f"Set as primary for chunk {chunk_handle}")
    
    def get_next_serial(self) -> int:
        """Get next serial number for mutation ordering"""
        with self.serial_lock:
            self.mutation_serial += 1
            return self.mutation_serial
    
    def cache_data(self, data_id: str, data: bytes):
        """Cache data for later write operation"""
        with self.data_cache_lock:
            self.cached_data[data_id] = data
            self.logger.debug(f"Cached data {data_id}, size: {len(data)} bytes")
    
    def get_cached_data(self, data_id: str) -> Optional[bytes]:
        """Retrieve cached data"""
        with self.data_cache_lock:
            return self.cached_data.get(data_id)
    
    def clear_cached_data(self, data_id: str):
        """Clear cached data after use"""
        with self.data_cache_lock:
            if data_id in self.cached_data:
                del self.cached_data[data_id]
                
    def forward_write_to_secondary(self, secondary_address: str, secondary_port: int, 
                                  chunk_handle: str, offset: int, data: bytes, serial: int, version: int = 0) -> bool:
        """Forward write operation to a secondary replica"""
        try:
            channel = grpc.insecure_channel(f"{secondary_address}:{secondary_port}", options=GRPC_CHANNEL_OPTIONS)
            stub = gfs_pb2_grpc.ChunkServerServiceStub(channel)
            
            response = stub.WriteChunk(
                gfs_pb2.WriteChunkRequest(
                    chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                    offset=offset,
                    data=data,
                    lease_id=f"secondary_write_{serial}",
                    chunk_version=version  # ✅ Forward version to secondary
                ),
                timeout=10.0
            )
            
            channel.close()
            return response.success
        except Exception as e:
            self.logger.warning(f"Failed to forward write to {secondary_address}:{secondary_port}: {e}")
            return False
        
class ChunkServerServiceServicer(gfs_pb2_grpc.ChunkServerServiceServicer):
    def __init__(self, chunk_server: GFSChunkServer):
        self.chunk_server = chunk_server
    
    def ReadChunk(self, request, context):
        """Handle chunk read requests with detailed logging"""
        self.chunk_server.logger.info(f"📖 READ CHUNK REQUEST RECEIVED")
        self.chunk_server.logger.info(f"   🔑 Chunk Handle: {request.chunk_handle.handle}")
        self.chunk_server.logger.info(f"   📍 Offset: {request.offset}")
        self.chunk_server.logger.info(f"   📏 Length: {request.length}")
        self.chunk_server.logger.info(f"   👤 Client Address: {context.peer()}")
        
        chunk_handle = request.chunk_handle.handle
        offset = request.offset
        length = request.length
        
        try:
            data = self.chunk_server.read_chunk_data(chunk_handle, offset, length)
            
            if data is not None:
                self.chunk_server.logger.info(f"   ✅ Read {len(data)} bytes successfully")
                return gfs_pb2.ReadChunkResponse(
                    success=True,
                    data=data,
                    message="Chunk read successfully"
                )
            else:
                self.chunk_server.logger.warning(f"   ⚠️  Chunk not found: {chunk_handle}")
                return gfs_pb2.ReadChunkResponse(
                    success=False,
                    data=b'',
                    message="Chunk not found"
                )
            
        except Exception as e:
            self.chunk_server.logger.error(f"   ❌ Read failed: {str(e)}")
            return gfs_pb2.ReadChunkResponse(
                success=False,
                data=b'',
                message=f"Read failed: {str(e)}"
            )
        
    # def WriteChunk(self, request, context):
    #     """Handle chunk write requests with detailed logging"""
    #     self.chunk_server.logger.info(f"📝 WRITE CHUNK REQUEST RECEIVED")
    #     self.chunk_server.logger.info(f"   🔑 Chunk Handle: {request.chunk_handle.handle}")
    #     self.chunk_server.logger.info(f"   📍 Offset: {request.offset}")
    #     self.chunk_server.logger.info(f"   📏 Data Length: {len(request.data)} bytes")
    #     self.chunk_server.logger.info(f"   👤 Client Address: {context.peer()}")
        
    #     chunk_handle = request.chunk_handle.handle
    #     offset = request.offset
    #     data = request.data
        
    #     try:
    #         success = self.chunk_server.write_chunk_data(chunk_handle, offset, data)
            
    #         if success:
    #             self.chunk_server.logger.info(f"   ✅ Chunk written successfully")
    #             return gfs_pb2.WriteChunkResponse(
    #                 success=True,
    #                 message="Chunk written successfully"
    #             )
    #         else:
    #             self.chunk_server.logger.error(f"   ❌ Write failed")
    #             return gfs_pb2.WriteChunkResponse(
    #                 success=False,
    #                 message="Write failed"
    #             )
            
    #     except Exception as e:
    #         self.chunk_server.logger.error(f"   ❌ Write failed: {str(e)}")
    #         return gfs_pb2.WriteChunkResponse(
    #             success=False,
    #             message=f"Write failed: {str(e)}"
    #         )

    def WriteChunk(self, request, context):
        """Write data to a chunk (GFS Protocol Step 4-7)
        
        GFS Protocol Implementation:
        Step 4: Client sends write request to primary (with data_id, no data)
        Step 5: Primary assigns serial numbers and applies mutation locally
        Step 6: Primary forwards write request to all secondaries  
        Step 7: Secondaries reply to primary, primary replies to client
        """
        chunk_handle = request.chunk_handle.handle
        offset = request.offset
        data = request.data
        lease_id = request.lease_id
        secondary_locations = list(request.secondary_locations) if hasattr(request, 'secondary_locations') else []
        chunk_version = request.chunk_version if hasattr(request, 'chunk_version') else 0  # ✅ Get expected version
        
        self.chunk_server.logger.debug(f"Write request: chunk={chunk_handle}, offset={offset}, data_len={len(data)}, lease={lease_id}, secondaries={len(secondary_locations)}, version={chunk_version}")
        
        try:
            # Check if this is a primary write (has data) or secondary write (forwarded)
            is_primary = self.chunk_server.is_primary_for_chunk(chunk_handle)
            is_secondary_write = lease_id.startswith("secondary_write_")
            
            self.chunk_server.logger.info(f"WRITE REQUEST: chunk={chunk_handle}, server={self.chunk_server.server_id}, is_primary={is_primary}, data_len={len(data)}, lease_id={lease_id}")
            
            # If this is a write command (no data) and we're getting it, the master thinks we're primary
            # So grant ourselves the lease temporarily
            if len(data) == 0 and not is_secondary_write:
                self.chunk_server.logger.info(f"Master sent write command to {self.chunk_server.server_id} - assuming primary lease")
                self.chunk_server.set_primary_lease(chunk_handle, 60.0)  # 60 second lease
                is_primary = True  # Update the flag
            
            if is_primary and len(data) == 0:
                # This is a primary receiving write command (GFS Step 4)
                # Data should have been pushed earlier via PushData
                self.chunk_server.logger.info(f"PRIMARY WRITE: chunk={chunk_handle}, server={self.chunk_server.server_id}")
                
                # ✅ UPDATE VERSION NUMBER FIRST (Master is authoritative)
                # When master grants a new lease, it increments version and sends it
                # Primary must accept this new version
                with self.chunk_server.chunk_lock:
                    if chunk_handle in self.chunk_server.chunks:
                        current_version = self.chunk_server.chunks[chunk_handle].version
                        if chunk_version > 0 and current_version != chunk_version:
                            self.chunk_server.logger.warning(f"Version update: {current_version} → {chunk_version}")
                        # Always update to master's version (master is authoritative)
                        self.chunk_server.chunks[chunk_handle].version = chunk_version
                
                # Create chunk if it doesn't exist
                if chunk_handle not in self.chunk_server.chunks:
                    self.chunk_server.logger.info(f"Creating missing chunk {chunk_handle} on primary {self.chunk_server.server_id}")
                    if not self.chunk_server.create_chunk(chunk_handle):
                        return gfs_pb2.WriteChunkResponse(
                            success=False,
                            message=f"Failed to create chunk {chunk_handle}"
                        )
                    # Set version for newly created chunk
                    with self.chunk_server.chunk_lock:
                        if chunk_handle in self.chunk_server.chunks:
                            self.chunk_server.chunks[chunk_handle].version = chunk_version
                            self.chunk_server.logger.info(f"Set version {chunk_version} for newly created chunk {chunk_handle}")
                
                # Extract data_id from lease_id (client should send it)
                data_id = lease_id
                cached_data = self.chunk_server.get_cached_data(data_id)
                
                self.chunk_server.logger.info(f"LOOKING FOR CACHED DATA: data_id={data_id}, found={cached_data is not None}, server={self.chunk_server.server_id}")
                
                if cached_data is None:
                    # Debug: List all cached data IDs
                    with self.chunk_server.data_cache_lock:
                        cached_ids = list(self.chunk_server.cached_data.keys())
                    self.chunk_server.logger.error(f"CACHED DATA NOT FOUND! Requested: {data_id}, Available: {cached_ids}")
                    
                    # FALLBACK: If data is provided directly in request (not typical GFS but helps debugging)
                    if len(data) > 0:
                        self.chunk_server.logger.warning(f"Using fallback: data provided directly in WriteChunk request ({len(data)} bytes)")
                        cached_data = data
                    else:
                        return gfs_pb2.WriteChunkResponse(
                            success=False,
                            message=f"No cached data found for data_id: {data_id}"
                        )
                
                # Step 5: Assign serial number for global ordering
                serial = self.chunk_server.get_next_serial()
                self.chunk_server.logger.info(f"✅ Assigned serial number: {serial} for mutation")
                
                # Step 5: Apply write locally first
                success = self.chunk_server.write_chunk_data(chunk_handle, offset, cached_data)
                if not success:
                    return gfs_pb2.WriteChunkResponse(
                        success=False,
                        message="Failed to write to primary"
                    )
                
                # Step 6: Forward to all secondaries using locations from request
                secondary_acks = 0
                total_secondaries = len(secondary_locations)
                
                if total_secondaries > 0:
                    self.chunk_server.logger.info(f"📤 Forwarding write to {total_secondaries} secondaries with serial={serial}")
                    
                    for secondary_loc in secondary_locations:
                        if self.chunk_server.forward_write_to_secondary(
                            secondary_loc.address, 
                            secondary_loc.port, 
                            chunk_handle, 
                            offset, 
                            cached_data, 
                            serial,
                            chunk_version  # ✅ Forward version to secondaries
                        ):
                            secondary_acks += 1
                            self.chunk_server.logger.info(f"✅ Secondary {secondary_loc.server_id} acknowledged write")
                        else:
                            self.chunk_server.logger.warning(f"❌ Secondary {secondary_loc.server_id} failed to acknowledge")
                
                # Step 7: Reply to client after majority success
                required_acks = (total_secondaries + 1) // 2  # Majority of secondaries
                if secondary_acks >= required_acks:
                    # Clear cached data
                    self.chunk_server.clear_cached_data(data_id)
                    
                    # Update chunk size
                    new_size = offset + len(cached_data)
                    with self.chunk_server.chunk_lock:
                        if chunk_handle in self.chunk_server.chunks:
                            self.chunk_server.chunks[chunk_handle].size = max(
                                self.chunk_server.chunks[chunk_handle].size,
                                new_size
                            )
                    
                    self.chunk_server.logger.info(f"✅ WRITE COMPLETE: serial={serial}, primary + {secondary_acks}/{total_secondaries} secondaries")
                    return gfs_pb2.WriteChunkResponse(
                        success=True,
                        message=f"Write successful, serial={serial}, {secondary_acks} secondaries acked"
                    )
                else:
                    return gfs_pb2.WriteChunkResponse(
                        success=False,
                        message=f"Insufficient secondary acks: {secondary_acks}/{total_secondaries} (required: {required_acks})"
                    )
                    
            elif is_secondary_write or len(data) > 0:
                # This is a secondary receiving forwarded write (GFS Step 6)
                self.chunk_server.logger.debug(f"Secondary write for chunk {chunk_handle}")
                
                # ✅ UPDATE VERSION ON SECONDARY
                with self.chunk_server.chunk_lock:
                    if chunk_handle in self.chunk_server.chunks and chunk_version > 0:
                        self.chunk_server.chunks[chunk_handle].version = chunk_version
                
                # Apply the mutation in the same serial order
                success = self.chunk_server.write_chunk_data(chunk_handle, offset, data)
                
                # Update chunk size
                if success:
                    new_size = offset + len(data)
                    with self.chunk_server.chunk_lock:
                        if chunk_handle in self.chunk_server.chunks:
                            self.chunk_server.chunks[chunk_handle].size = max(
                                self.chunk_server.chunks[chunk_handle].size,
                                new_size
                            )
                
                return gfs_pb2.WriteChunkResponse(
                    success=success,
                    message="Secondary write successful" if success else "Secondary write failed"
                )
            
            else:
                # Invalid write request
                return gfs_pb2.WriteChunkResponse(
                    success=False,
                    message="Invalid write request - not primary and no data provided"
                )
                
        except Exception as e:
            self.chunk_server.logger.error(f"Write error: {e}")
            return gfs_pb2.WriteChunkResponse(
                success=False,
                message=f"Write failed: {e}"
            )

    def AppendChunk(self, request, context):
        """Append data to a chunk (GFS Atomic Record Append - Section 3.3)
        
        GFS Record Append Protocol (follows same control flow as write):
        1. Client sends append request to primary (after pushing data to all replicas)
        2. Primary checks if record fits in current chunk:
           a. If would exceed max size (64MB): pad chunk, tell secondaries to pad, return failure
           b. If fits: choose append offset
        3. Primary appends data at chosen offset
        4. Primary tells secondaries to write at EXACT same offset
        5. All secondaries apply at same offset (atomic)
        6. Primary returns actual offset to client
        
        Key properties (from paper):
        - Record append restricted to at most 1/4 of max chunk size (16MB)
        - At-least-once semantics (may have duplicates on retry)
        - Primary chooses offset, secondaries use exact same offset
        """
        chunk_handle = request.chunk_handle.handle
        data = request.data
        lease_id = request.lease_id
        data_id = request.data_id if hasattr(request, 'data_id') else lease_id
        secondary_locations = list(request.secondary_locations) if hasattr(request, 'secondary_locations') else []
        chunk_version = request.version if hasattr(request, 'version') else 0
        
        self.chunk_server.logger.debug(f"Append request: chunk={chunk_handle}, data_len={len(data)}, lease={lease_id}")
        
        # Check if this server is primary for this chunk
        is_primary = self.chunk_server.is_primary_for_chunk(chunk_handle)
        is_secondary_append = lease_id.startswith("secondary_append_")
        
        self.chunk_server.logger.info(f"APPEND REQUEST: chunk={chunk_handle}, server={self.chunk_server.server_id}, is_primary={is_primary}, data_len={len(data)}, lease_id={lease_id}")
        
        # If master sent us an append command (no data) and we're getting it, master thinks we're primary
        # So grant ourselves the lease temporarily
        if len(data) == 0 and not is_secondary_append:
            self.chunk_server.logger.info(f"Master sent append command to {self.chunk_server.server_id} - assuming primary lease")
            self.chunk_server.set_primary_lease(chunk_handle, 60.0)  # 60 second lease
            is_primary = True  # Update the flag
        
        # Extract offset from lease_id if this is a secondary append
        specified_offset = None
        if is_secondary_append:
            try:
                parts = lease_id.split('_')
                if len(parts) >= 4:
                    specified_offset = int(parts[3])  # "secondary_append_serial_offset"
            except:
                pass
        
        if is_primary and not is_secondary_append:
            # ===== PRIMARY PROCESSING (GFS Section 3.3) =====
            self.chunk_server.logger.info(f"PRIMARY APPEND: chunk={chunk_handle}, server={self.chunk_server.server_id}")
            
            # Get cached data (client pushed it earlier)
            # Use data_id field if available, otherwise fall back to lease_id
            cached_data = self.chunk_server.get_cached_data(data_id)
            
            if cached_data is None:
                self.chunk_server.logger.error(f"No cached data found for data_id: {data_id}")
                # Debug: List all cached data IDs
                with self.chunk_server.data_cache_lock:
                    cached_ids = list(self.chunk_server.cached_data.keys())
                self.chunk_server.logger.error(f"Available cached data IDs: {cached_ids}")
                return gfs_pb2.AppendChunkResponse(
                    success=False,
                    offset=-1,
                    message=f"No cached data found for data_id: {data_id}"
                )
            
            # Validate version
            with self.chunk_server.chunk_lock:
                if chunk_handle in self.chunk_server.chunks:
                    current_version = self.chunk_server.chunks[chunk_handle].version
                    if chunk_version > 0 and current_version != chunk_version:
                        self.chunk_server.logger.warning(f"Version update in append: {current_version} → {chunk_version}")
                    # Always update to master's version (master is authoritative)
                    self.chunk_server.chunks[chunk_handle].version = chunk_version
            
            # Create chunk if doesn't exist
            if chunk_handle not in self.chunk_server.chunks:
                self.chunk_server.logger.info(f"Creating chunk {chunk_handle} on primary")
                if not self.chunk_server.create_chunk(chunk_handle):
                    return gfs_pb2.AppendChunkResponse(
                        success=False,
                        offset=-1,
                        message=f"Failed to create chunk {chunk_handle}"
                    )
                # Set version for newly created chunk
                with self.chunk_server.chunk_lock:
                    if chunk_handle in self.chunk_server.chunks:
                        self.chunk_server.chunks[chunk_handle].version = chunk_version
                        self.chunk_server.logger.info(f"Set version {chunk_version} for newly created chunk {chunk_handle}")
            
            # Get current chunk size to determine append offset
            with self.chunk_server.chunk_lock:
                chunk_path = self.chunk_server.get_chunk_path(chunk_handle)
                append_offset = os.path.getsize(chunk_path)
                
                # CHECK: Would record fit? (GFS Paper Section 3.3)
                if append_offset + len(cached_data) > CHUNK_SIZE:
                    # Record doesn't fit - pad chunk to max size
                    self.chunk_server.logger.warning(f"⚠️  Append would exceed chunk size (offset={append_offset}, data={len(cached_data)}, max={CHUNK_SIZE})")
                    self.chunk_server.logger.info(f"Padding chunk {chunk_handle} to max size and telling secondaries to do same")
                    
                    # Pad this chunk to max size
                    padding_size = CHUNK_SIZE - append_offset
                    if padding_size > 0:
                        padding = b'\x00' * padding_size
                        self.chunk_server.write_chunk_data(chunk_handle, append_offset, padding)
                    
                    # Tell secondaries to pad as well
                    for secondary_loc in secondary_locations:
                        try:
                            # secondary_loc is now a string "address:port"
                            channel = grpc.insecure_channel(secondary_loc, options=GRPC_CHANNEL_OPTIONS)
                            stub = gfs_pb2_grpc.ChunkServerServiceStub(channel)
                            
                            # Send padding command
                            stub.AppendChunk(
                                gfs_pb2.AppendChunkRequest(
                                    chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                                    data=padding if padding_size > 0 else b'',
                                    lease_id=f"secondary_append_pad_{append_offset}",
                                    data_id=data_id,
                                    version=chunk_version
                                ),
                                timeout=10.0
                            )
                            channel.close()
                            self.chunk_server.logger.info(f"✅ Told secondary {secondary_loc} to pad")
                        except Exception as e:
                            self.chunk_server.logger.error(f"Failed to tell secondary to pad: {e}")
                    
                    # Clear cached data
                    self.chunk_server.clear_cached_data(data_id)
                    
                    # Return failure - client should retry with next chunk
                    return gfs_pb2.AppendChunkResponse(
                        success=False,
                        offset=-1,
                        message="Chunk full - padded to max size, retry with next chunk"
                    )
            
            # Record FITS - proceed with append
            serial = self.chunk_server.get_next_serial()
            self.chunk_server.logger.info(f"✅ Assigned serial {serial}, appending at offset {append_offset}")
            
            # Apply append locally at chosen offset
            success = self.chunk_server.write_chunk_data(chunk_handle, append_offset, cached_data)
            if not success:
                return gfs_pb2.AppendChunkResponse(
                    success=False,
                    offset=-1,
                    message="Failed to append on primary"
                )
            
            # Forward append to all secondaries with SAME offset
            secondary_acks = 0
            total_secondaries = len(secondary_locations)
            
            if total_secondaries > 0:
                self.chunk_server.logger.info(f"📤 Forwarding append to {total_secondaries} secondaries at offset {append_offset}")
                
                for secondary_loc in secondary_locations:
                    try:
                        # secondary_loc is now a string "address:port"
                        channel = grpc.insecure_channel(secondary_loc, options=GRPC_CHANNEL_OPTIONS)
                        stub = gfs_pb2_grpc.ChunkServerServiceStub(channel)
                        
                        response = stub.AppendChunk(
                            gfs_pb2.AppendChunkRequest(
                                chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                                data=cached_data,  # Send actual data
                                lease_id=f"secondary_append_{serial}_{append_offset}",  # Encode offset
                                data_id=data_id,
                                version=chunk_version
                            ),
                            timeout=10.0
                        )
                        
                        channel.close()
                        
                        if response.success and response.offset == append_offset:
                            secondary_acks += 1
                            self.chunk_server.logger.info(f"✅ Secondary {secondary_loc} appended at offset {append_offset}")
                        else:
                            self.chunk_server.logger.warning(f"❌ Secondary {secondary_loc} failed: {response.message}")
                    except Exception as e:
                        self.chunk_server.logger.error(f"Failed to forward append to {secondary_loc}: {e}")
            
            # Require majority acknowledgment
            required_acks = (total_secondaries + 1) // 2
            if secondary_acks >= required_acks:
                # Clear cached data
                self.chunk_server.clear_cached_data(data_id)
                
                # Update chunk size
                new_size = append_offset + len(cached_data)
                with self.chunk_server.chunk_lock:
                    if chunk_handle in self.chunk_server.chunks:
                        self.chunk_server.chunks[chunk_handle].size = max(
                            self.chunk_server.chunks[chunk_handle].size,
                            new_size
                        )
                
                self.chunk_server.logger.info(f"✅ APPEND COMPLETE: offset={append_offset}, size={len(cached_data)}, acks={secondary_acks}/{total_secondaries}")
                
                # Return actual offset to client
                return gfs_pb2.AppendChunkResponse(
                    success=True,
                    offset=append_offset,
                    message=f"Appended at offset {append_offset}"
                )
            else:
                return gfs_pb2.AppendChunkResponse(
                    success=False,
                    offset=-1,
                    message=f"Insufficient secondary acks: {secondary_acks}/{total_secondaries} (required: {required_acks})"
                )
        
        elif is_secondary_append:
            # ===== SECONDARY PROCESSING =====
            self.chunk_server.logger.info(f"SECONDARY APPEND: chunk={chunk_handle}, server={self.chunk_server.server_id}")
            
            # Update version on secondary
            with self.chunk_server.chunk_lock:
                if chunk_handle in self.chunk_server.chunks and chunk_version > 0:
                    self.chunk_server.chunks[chunk_handle].version = chunk_version
            
            # Check if this is a padding command
            if "pad" in lease_id:
                # Apply padding at specified offset
                if specified_offset is not None and len(data) > 0:
                    success = self.chunk_server.write_chunk_data(chunk_handle, specified_offset, data)
                    return gfs_pb2.AppendChunkResponse(
                        success=success,
                        offset=specified_offset if success else -1,
                        message="Padding applied" if success else "Padding failed"
                    )
                else:
                    return gfs_pb2.AppendChunkResponse(success=True, offset=0, message="No padding needed")
            
            # Regular append: apply at the EXACT offset specified by primary
            if specified_offset is None:
                self.chunk_server.logger.error(f"Secondary append missing offset in lease_id: {lease_id}")
                return gfs_pb2.AppendChunkResponse(
                    success=False,
                    offset=-1,
                    message="Missing offset for secondary append"
                )
            
            # Apply append at exact offset
            success = self.chunk_server.write_chunk_data(chunk_handle, specified_offset, data)
            
            # Update chunk size
            if success:
                new_size = specified_offset + len(data)
                with self.chunk_server.chunk_lock:
                    if chunk_handle in self.chunk_server.chunks:
                        self.chunk_server.chunks[chunk_handle].size = max(
                            self.chunk_server.chunks[chunk_handle].size,
                            new_size
                        )
            
            return gfs_pb2.AppendChunkResponse(
                success=success,
                offset=specified_offset if success else -1,
                message=f"Secondary append at offset {specified_offset}" if success else "Secondary append failed"
            )
        
        else:
            # Invalid request
            return gfs_pb2.AppendChunkResponse(
                success=False,
                offset=-1,
                message="Invalid append request - not primary and no secondary append marker"
            )

    def PushData(self, request, context):
        """Handle data push from client (part of write pipeline)"""
        data = request.data
        data_id = request.data_id
        next_server = request.next_server
        remaining_chain = list(request.remaining_chain) if hasattr(request, 'remaining_chain') else []
        
        self.chunk_server.logger.info(f"📥 PUSH DATA RECEIVED: data_id={data_id}, size={len(data)} bytes, server={self.chunk_server.server_id}")
        if next_server:
            self.chunk_server.logger.info(f"   ⏭️  Next server in chain: {next_server}")
        
        try:
            # Validate input
            if not data_id:
                return gfs_pb2.PushDataResponse(
                    success=False,
                    message="Missing data_id in push request"
                )
            
            if len(data) == 0:
                self.chunk_server.logger.warning(f"⚠️  Empty data received for data_id: {data_id}")
                return gfs_pb2.PushDataResponse(
                    success=False,
                    message="Empty data received"
                )
            
            # Cache the data for later write operation (GFS protocol step 3)
            self.chunk_server.cache_data(data_id, data)
            self.chunk_server.logger.info(f"✅ CACHED DATA: data_id={data_id}, size={len(data)} bytes, server={self.chunk_server.server_id}")
            
            # Forward data to next server in pipeline (GFS pipelined data flow)
            # IMPORTANT: We wait for the next server's ACK before returning our own ACK
            # This ensures ACKs flow back through the chain: Server3→Server2→Server1→Client
            if next_server and next_server.strip():
                try:
                    self.chunk_server.logger.info(f"🔄 Forwarding {len(data)} bytes to next server: {next_server}")
                    
                    # Parse next server address
                    if ':' in next_server:
                        addr, port = next_server.split(':')
                        port = int(port)
                    else:
                        addr = next_server
                        port = 9011  # Default chunk server port
                    
                    # Determine the server after next_server for continued pipelining
                    next_next_server = ""
                    if remaining_chain and len(remaining_chain) > 1:
                        # Skip the first one (which is next_server) and get the second
                        next_next_server = remaining_chain[1]
                    
                    # Forward to next server and WAIT for its acknowledgment
                    channel = grpc.insecure_channel(f"{addr}:{port}", options=GRPC_CHANNEL_OPTIONS)
                    stub = gfs_pb2_grpc.ChunkServerServiceStub(channel)
                    
                    forward_response = stub.PushData(
                        gfs_pb2.PushDataRequest(
                            data=data,  # ✅ Forward the actual data!
                            data_id=data_id,
                            next_server=next_next_server,  # ✅ Continue the chain
                            remaining_chain=remaining_chain[1:] if len(remaining_chain) > 1 else []
                        ),
                        timeout=10.0  # Increased timeout to wait for downstream ACKs
                    )
                    
                    channel.close()
                    
                    if not forward_response.success:
                        self.chunk_server.logger.error(f"❌ Failed to forward data to {next_server}: {forward_response.message}")
                        return gfs_pb2.PushDataResponse(
                            success=False,
                            message=f"Forward failed: {forward_response.message}",
                            pipeline_complete=False
                        )
                    else:
                        self.chunk_server.logger.info(f"✅ Successfully forwarded data to {next_server} and received ACK")
                        # Return success with the pipeline_complete status from downstream
                        return gfs_pb2.PushDataResponse(
                            success=True,
                            message=f"Data cached on {self.chunk_server.server_id} and forwarded to {next_server}",
                            pipeline_complete=forward_response.pipeline_complete
                        )
                        
                except Exception as e:
                    self.chunk_server.logger.error(f"❌ Failed to forward data to {next_server}: {e}")
                    return gfs_pb2.PushDataResponse(
                        success=False,
                        message=f"Forward exception: {str(e)}",
                        pipeline_complete=False
                    )
            else:
                # This is the last server in the chain
                self.chunk_server.logger.info(f"🏁 End of pipeline reached at {self.chunk_server.server_id}")
                self.chunk_server.logger.info(f"✅ ALL SERVERS in pipeline have cached data_id: {data_id}")
                
                # Return success with pipeline_complete=True since we're the last server
                return gfs_pb2.PushDataResponse(
                    success=True,
                    message=f"Data cached on {self.chunk_server.server_id} (last in chain)",
                    pipeline_complete=True  # Mark as complete since this is the end
                )
        except Exception as e:
            self.chunk_server.logger.error(f"❌ PUSH DATA FAILED: {e}")
            return gfs_pb2.PushDataResponse(
                success=False,
                message=f"Failed to push data: {e}"
            )
  
    # def AppendChunk(self, request, context):
    #     """Append data to a chunk - implements GFS record append data flow"""
    #     chunk_handle = request.chunk_handle.handle
    #     data = request.data
    #     lease_id = request.lease_id
        
    #     self.chunk_server.logger.debug(f"Append request: chunk={chunk_handle}, length={len(data)}")
        
    #     # TODO: Verify lease_id with master
    #     # In GFS, only the primary replica can handle append operations
        
    #     # Validate data size
    #     if len(data) > CHUNK_SIZE // 4:  # GFS recommendation: record size should be much smaller than chunk size
    #         return gfs_pb2.AppendChunkResponse(
    #             success=False,
    #             offset=-1,
    #             message=f"Record too large: {len(data)} bytes (max recommended: {CHUNK_SIZE // 4})"
    #         )
        
    #     offset = self.chunk_server.append_chunk_data(chunk_handle, data)
        
    #     if offset is not None:
    #         return gfs_pb2.AppendChunkResponse(
    #             success=True,
    #             offset=offset,
    #             message="Append successful"
    #         )
    #     else:
    #         return gfs_pb2.AppendChunkResponse(
    #             success=False,
    #             offset=-1,
    #             message=f"Failed to append to chunk {chunk_handle}"
    #         )

    # ------------------------------------------------------------------
    # Erasure-coded shard RPCs (ADR-005)
    # ------------------------------------------------------------------
    def StoreShard(self, request, context):
        handle = request.chunk_handle.handle
        try:
            self.chunk_server.shard_store.put(
                handle, request.shard_index, request.data,
                request.data_shards, request.parity_shards,
                request.original_length, request.chunk_version)
            self.chunk_server.logger.info(
                f'stored shard {handle}#{request.shard_index} '
                f'({len(request.data)} bytes)')
            return gfs_pb2.StoreShardResponse(success=True, message='stored')
        except OSError as e:
            return gfs_pb2.StoreShardResponse(success=False, message=str(e))

    def ReadShard(self, request, context):
        handle = request.chunk_handle.handle
        data = self.chunk_server.shard_store.get(handle, request.shard_index)
        if data is None:
            return gfs_pb2.ReadShardResponse(
                success=False, shard_index=request.shard_index,
                message=f'shard {handle}#{request.shard_index} not held here')
        return gfs_pb2.ReadShardResponse(
            success=True, data=data, shard_index=request.shard_index)

    def DeleteShard(self, request, context):
        existed = self.chunk_server.shard_store.delete(
            request.chunk_handle.handle, request.shard_index)
        return gfs_pb2.DeleteShardResponse(
            success=True, message='deleted' if existed else 'not present')

    def GetChunkChecksum(self, request, context):
        """Get checksum of a chunk"""
        chunk_handle = request.chunk_handle.handle
        
        with self.chunk_server.chunk_lock:
            if chunk_handle in self.chunk_server.chunks:
                chunk_info = self.chunk_server.chunks[chunk_handle]
                return gfs_pb2.GetChunkChecksumResponse(
                    success=True,
                    checksum=chunk_info.checksum
                )
            else:
                return gfs_pb2.GetChunkChecksumResponse(
                    success=False,
                    message=f"Chunk {chunk_handle} not found"
                )

def serve(server_id: str = None, port: int = 9001, master_addresses: List[str] = None,
          external_address: str = None, external_port: int = None,
          storage_dir: str = "chunks", max_workers: int = DEFAULT_MAX_WORKERS,
          log_level: str = "INFO"):
    """Start the GFS Chunk Server"""
    chunk_server = GFSChunkServer(
        server_id=server_id,
        port=port,
        master_addresses=master_addresses,
        external_address=external_address,
        external_port=external_port,
        storage_dir=storage_dir,
        log_level=log_level
    )
    chunk_server.start_background_tasks()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers),
                         options=GRPC_SERVER_OPTIONS)
    gfs_pb2_grpc.add_ChunkServerServiceServicer_to_server(
        ChunkServerServiceServicer(chunk_server), server
    )
    
    listen_addr = f'[::]:{port}'
    server.add_insecure_port(listen_addr)
    
    server.start()
    logging.info(f"GFS Chunk Server {chunk_server.server_id} started on port {port}")
    
    try:
        while True:
            time.sleep(86400)  # Sleep for a day
    except KeyboardInterrupt:
        logging.info("Shutting down GFS Chunk Server")
        chunk_server.stop_background_tasks()
        server.stop(0)

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='GFS Chunk Server')
    parser.add_argument('--server-id', type=str, help='Server ID')
    parser.add_argument('--port', type=int, default=9001, help='Port to listen on')
    parser.add_argument('--masters', type=str, nargs='+', default=['localhost:9000'], help='Master server addresses')
    parser.add_argument('--external-address', type=str, help='External address for client connections (for Docker networking)')
    parser.add_argument('--external-port', type=int, help='External port for client connections (for Docker networking)')
    parser.add_argument('--storage-dir', type=str, default='chunks',
                        help='Directory this chunk server stores chunk files in')
    parser.add_argument('--max-workers', type=int, default=DEFAULT_MAX_WORKERS,
                        help='gRPC server thread pool size')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    args = parser.parse_args()

    serve(args.server_id, args.port, args.masters, args.external_address, args.external_port,
          storage_dir=args.storage_dir, max_workers=args.max_workers, log_level=args.log_level)
#!/usr/bin/env python3

import grpc
import logging
from typing import List, Optional, Tuple, Dict
import time
import random
import argparse
import sys

import gfs_pb2
import gfs_pb2_grpc
from metacache import MetadataCache
from ec_storage import decode_chunk

# Configuration constants -- see gfs_config.py and docs/adr/
from gfs_config import (
    CHUNK_SIZE,
    GRPC_CHANNEL_OPTIONS,
    CLIENT_CACHE_MODE,
    CLIENT_CACHE_SIZE,
    CLIENT_CACHE_TTL,
)

# Path constants for client-side normalization
PATH_SEPARATOR = '/'
ROOT_PATH = '/'

class MultiMasterGFSClient:
    def __init__(self, master_addresses: List[str]):
        """
        Initialize GFS client with multiple master addresses
        Args:
            master_addresses: List of master addresses in format "host:port"
        """
        
        self.master_addresses = master_addresses
        self.current_leader = None
        self.master_stubs = {}  # Cache for master connections
        self.chunk_server_stubs = {}  # Cache for chunk server connections

        # Metadata cache (ADR-006). Without it every read costs a GetFileInfo
        # plus a GetChunkLocations per chunk before any data moves.
        # Shard maps for erasure-coded chunks, learned from
        # GetChunkLocations. Held beside MetadataCache rather than inside it,
        # so `_forget_chunk` below keeps the two in lockstep -- a shard map
        # that outlives its location entry is exactly the stale-read bug the
        # cache invalidation rules exist to prevent.
        self._ec_info = {}

        self.cache = MetadataCache(mode=CLIENT_CACHE_MODE,
                                   maxsize=CLIENT_CACHE_SIZE,
                                   ttl=CLIENT_CACHE_TTL)
        
        # Initialize logging first
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Try to discover the current leader
        self.discover_leader()

    def normalize_path(self, path: str) -> str:
        """Normalize a file path to match server-side normalization"""
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

    def get_master_stub(self, address: str):
        """Get or create a master stub"""
        if address not in self.master_stubs:
            try:
                channel = grpc.insecure_channel(address, options=GRPC_CHANNEL_OPTIONS)
                stub = gfs_pb2_grpc.MasterServiceStub(channel)
                self.master_stubs[address] = stub
                self.logger.debug(f"Connected to master at {address}")
            except Exception as e:
                self.logger.error(f"Failed to connect to master {address}: {e}")
                return None
        
        return self.master_stubs[address]

    def discover_leader(self, max_retries: int = None, retry_interval: float = 1.0) -> bool:
        """
        Discover the current leader master with continuous retry
        Args:
            max_retries: Maximum number of retry attempts (None for infinite retries)
            retry_interval: Time to wait between retries in seconds
        """
        retry_count = 0
        
        while max_retries is None or retry_count < max_retries:
            if retry_count > 0:
                print(f"🔄 Retrying to find leader master... (attempt {retry_count + 1})")
                time.sleep(retry_interval)
            
            # Shuffle addresses to avoid always hitting the same master first
            addresses = self.master_addresses.copy()
            random.shuffle(addresses)
            
            for address in addresses:
                try:
                    stub = self.get_master_stub(address)
                    if not stub:
                        continue
                    
                    request = gfs_pb2.GetPrimaryMasterRequest(requester_id="client")
                    response = stub.GetPrimaryMaster(request, timeout=5.0)
                    
                    if response.success and response.primary_master:
                        # Get the leader info from response
                        leader_id = response.primary_master.master_id
                        leader_address = response.primary_master.address
                        leader_port = response.primary_master.port
                        
                        # FIXED: Map the discovered leader back to our provided addresses
                        # Instead of using the address from response (which might be localhost),
                        # find which of our provided addresses corresponds to this leader
                        discovered_leader = None
                        
                        # Check if the leader is the master we just contacted
                        if (leader_address in ['localhost', '127.0.0.1', '0.0.0.0'] or 
                            address.startswith(leader_address.split(':')[0] if ':' in leader_address else leader_address)):
                            discovered_leader = address
                        else:
                            # Try to find the leader among our provided addresses
                            for candidate_addr in self.master_addresses:
                                try:
                                    candidate_stub = self.get_master_stub(candidate_addr)
                                    if not candidate_stub:
                                        continue
                                    
                                    # Ask this candidate if it's the primary
                                    candidate_request = gfs_pb2.GetPrimaryMasterRequest(requester_id="client")
                                    candidate_response = candidate_stub.GetPrimaryMaster(candidate_request, timeout=2.0)
                                    
                                    if (candidate_response.success and 
                                        candidate_response.primary_master and
                                        candidate_response.primary_master.master_id == leader_id and
                                        candidate_response.primary_master.is_primary):
                                        discovered_leader = candidate_addr
                                        break
                                        
                                except Exception as e:
                                    self.logger.debug(f"Failed to verify leader with {candidate_addr}: {e}")
                                    continue
                        
                        if discovered_leader:
                            self.current_leader = discovered_leader
                            if retry_count > 0:
                                print(f"✅ Found leader master: {discovered_leader} (after {retry_count + 1} attempts)")
                            else:
                                self.logger.info(f"Discovered leader master: {discovered_leader}")
                            return True
                        else:
                            self.logger.warning(f"Could not map discovered leader {leader_id} to provided addresses")
                            
                except Exception as e:
                    self.logger.debug(f"Failed to contact master {address}: {e}")
                    continue
            
            # If we reach here, no leader was found in this attempt
            if retry_count == 0:
                print("❌ No leader master found in the cluster")
                print("🔄 Will keep retrying to find a leader...")
            
            retry_count += 1
        
        # This will only be reached if max_retries is set and exceeded
        self.logger.error(f"Failed to find leader after {max_retries} attempts")
        return False

    def discover_leader_with_timeout(self, timeout_seconds: float = 30.0) -> bool:
        """
        Discover leader with a timeout instead of retry count
        Args:
            timeout_seconds: Maximum time to spend looking for leader
        """
        start_time = time.time()
        retry_count = 0
        
        while time.time() - start_time < timeout_seconds:
            if retry_count > 0:
                elapsed = time.time() - start_time
                remaining = timeout_seconds - elapsed
                print(f"🔄 Retrying to find leader master... (attempt {retry_count + 1}, {remaining:.1f}s remaining)")
                time.sleep(min(1.0, remaining))
            
            # Shuffle addresses to avoid always hitting the same master first
            addresses = self.master_addresses.copy()
            random.shuffle(addresses)
            
            for address in addresses:
                try:
                    stub = self.get_master_stub(address)
                    if not stub:
                        continue
                    
                    request = gfs_pb2.GetPrimaryMasterRequest(requester_id="client")
                    response = stub.GetPrimaryMaster(request, timeout=5.0)
                    
                    if response.success and response.primary_master:
                        leader_addr = f"{response.primary_master.address}:{response.primary_master.port}"
                        self.current_leader = leader_addr
                        if retry_count > 0:
                            print(f"✅ Found leader master: {leader_addr} (after {retry_count + 1} attempts)")
                        else:
                            self.logger.info(f"Discovered leader master: {leader_addr}")
                        return True
                        
                except Exception as e:
                    self.logger.debug(f"Failed to contact master {address}: {e}")
                    continue
            
            # If we reach here, no leader was found in this attempt
            if retry_count == 0:
                print("❌ No leader master found in the cluster")
                print(f"🔄 Will keep retrying to find a leader for {timeout_seconds}s...")
            
            retry_count += 1
        
        print(f"❌ Failed to find leader master within {timeout_seconds} seconds")
        return False

    def _execute_with_leader_failover(self, operation, *args, **kwargs):
        """Execute an operation with automatic leader failover and continuous retry"""
        max_retries = 3  # Maximum attempts per leader discovery
        
        for attempt in range(max_retries):
            if not self.current_leader:
                print("🔍 No current leader, discovering leader...")
                if not self.discover_leader(max_retries=10, retry_interval=1.0):  # Try 10 times with 1s intervals
                    # If still no leader found after retries, keep trying indefinitely
                    print("⚠️  Still no leader found, will keep retrying indefinitely...")
                    if not self.discover_leader():  # Infinite retries
                        raise Exception("Failed to find any leader master - all masters may be down")
            
            try:
                stub = self.get_master_stub(self.current_leader)
                if not stub:
                    print(f"❌ Cannot connect to leader {self.current_leader}, finding new leader...")
                    self.current_leader = None
                    continue
                
                return operation(stub, *args, **kwargs)
                
            except grpc.RpcError as e:
                error_msg = str(e)
                if "Not the leader master" in error_msg or e.code() == grpc.StatusCode.UNAVAILABLE:
                    print(f"⚠️  Master {self.current_leader} is not leader or unavailable, discovering new leader...")
                    self.current_leader = None
                    # Don't retry immediately, let the loop handle leader discovery
                    continue
                else:
                    raise
            except Exception as e:
                self.logger.error(f"Error communicating with master {self.current_leader}: {e}")
                self.current_leader = None
                continue
        
        raise Exception("Failed to execute operation after multiple leader discovery attempts")

    def get_chunk_server_stub(self, address: str, port: int):
        """Get or create a chunk server stub"""
        server_addr = f"{address}:{port}"
        
        if server_addr not in self.chunk_server_stubs:
            try:
                channel = grpc.insecure_channel(server_addr, options=GRPC_CHANNEL_OPTIONS)
                stub = gfs_pb2_grpc.ChunkServerServiceStub(channel)
                self.chunk_server_stubs[server_addr] = stub
                self.logger.debug(f"Connected to chunk server at {server_addr}")
            except Exception as e:
                self.logger.error(f"Failed to connect to chunk server {server_addr}: {e}")
                return None
        
        return self.chunk_server_stubs[server_addr]

    def create_file(self, filename: str) -> bool:
        """Create a new file"""
        filename = self.normalize_path(filename)

        def _create_file_op(stub, filename):
            request = gfs_pb2.CreateFileRequest(filename=filename)
            response = stub.CreateFile(request, timeout=10.0)
            if response.success:
                self.logger.info(f"Created file: {filename}")
                return True
            else:
                self.logger.error(f"Failed to create file {filename}: {response.message}")
                return False
        
        try:
            ok = self._execute_with_leader_failover(_create_file_op, filename)
            if ok:
                # A negative result may be cached from before it existed.
                self.cache.invalidate_file(filename)
            return ok
        except Exception as e:
            self.logger.error(f"Error creating file {filename}: {e}")
            return False

    def delete_file(self, filename: str) -> bool:
        """Delete a file"""
        filename = self.normalize_path(filename)

        def _delete_file_op(stub, filename):
            request = gfs_pb2.DeleteFileRequest(filename=filename)
            response = stub.DeleteFile(request, timeout=10.0)
            if response.success:
                self.logger.info(f"Deleted file: {filename}")
                return True
            else:
                self.logger.error(f"Failed to delete file {filename}: {response.message}")
                return False
        
        try:
            ok = self._execute_with_leader_failover(_delete_file_op, filename)
            if ok:
                self.cache.invalidate_file(filename)
            return ok
        except Exception as e:
            self.logger.error(f"Error deleting file {filename}: {e}")
            return False

    def get_file_info(self, filename: str) -> Optional[dict]:
        """Get file information"""
        filename = self.normalize_path(filename)

        def _get_file_info_op(stub, filename):
            request = gfs_pb2.GetFileInfoRequest(filename=filename)
            response = stub.GetFileInfo(request, timeout=10.0)
            if response.success:
                return {
                    'filename': response.file_info.filename,
                    'size': response.file_info.size,
                    'chunk_size': response.file_info.chunk_size,
                    'chunks': [chunk.handle for chunk in response.file_info.chunks]
                }
            else:
                self.logger.error(f"Failed to get file info for {filename}: {response.message}")
                return None
        
        cached = self.cache.get_file_info(filename)
        if cached is not None:
            return cached

        try:
            info = self._execute_with_leader_failover(_get_file_info_op, filename)
            if info is not None:
                self.cache.put_file_info(filename, info)
            return info
        except Exception as e:
            self.logger.error(f"Error getting file info for {filename}: {e}")
            return None

    def get_chunk_locations(self, chunk_handle: str) -> List[dict]:
        """Get locations of a chunk"""
        def _get_chunk_locations_op(stub, chunk_handle):
            request = gfs_pb2.GetChunkLocationsRequest(
                chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle)
            )
            response = stub.GetChunkLocations(request, timeout=10.0)
            if response.success:
                locations = []
                for location in response.locations:
                    locations.append({
                        'server_id': location.server_id,
                        'address': location.address,
                        'port': location.port
                    })
                if response.ec.is_ec:
                    # Stash the shard map beside the replica list. Kept on
                    # the client rather than resolved by the master because
                    # the paper's rule holds for EC too: the client talks to
                    # chunk servers directly and the master only says where.
                    self._ec_info[chunk_handle] = {
                        'k': response.ec.data_shards,
                        'm': response.ec.parity_shards,
                        'original_length': response.ec.original_length,
                        'shards': [
                            {'server_id': sl.server_id, 'address': sl.address,
                             'port': sl.port, 'shard_index': sl.shard_index}
                            for sl in response.ec.shards
                        ],
                    }
                return locations
            else:
                self.logger.error(f"Failed to get chunk locations for {chunk_handle}: {response.message}")
                return []
        
        cached = self.cache.get_locations(chunk_handle)
        if cached is not None:
            return cached

        try:
            locations = self._execute_with_leader_failover(
                _get_chunk_locations_op, chunk_handle)
            if locations:
                self.cache.put_locations(chunk_handle, locations)
            return locations
        except Exception as e:
            self.logger.error(f"Error getting chunk locations for {chunk_handle}: {e}")
            return []

    def allocate_chunk(self, filename: str, chunk_index: int) -> Optional[Tuple[str, List[dict]]]:
        """Allocate a new chunk for a file"""
        filename = self.normalize_path(filename)

        def _allocate_chunk_op(stub, filename, chunk_index):
            request = gfs_pb2.AllocateChunkRequest(
                filename=filename,
                chunk_index=chunk_index
            )
            response = stub.AllocateChunk(request, timeout=10.0)
            if response.success:
                locations = []
                for location in response.locations:
                    locations.append({
                        'server_id': location.server_id,
                        'address': location.address,
                        'port': location.port
                    })
                return response.chunk_handle.handle, locations
            else:
                self.logger.error(f"Failed to allocate chunk for {filename}: {response.message}")
                return None
        
        try:
            result = self._execute_with_leader_failover(
                _allocate_chunk_op, filename, chunk_index)
            if result:
                handle, locations = result
                # The response already carries authoritative locations, so
                # seed them rather than making the next read ask again.
                self.cache.put_locations(handle, locations)
                # The file's chunk list just changed.
                self.cache.invalidate_file(filename)
            return result
        except Exception as e:
            self.logger.error(f"Error allocating chunk for {filename}: {e}")
            return None

    def calculate_network_proximity(self, address: str, port: int) -> float:
        """
        Calculate network proximity to a chunk server based on various factors:
        1. Localhost/same machine gets highest priority (lowest latency)
        2. Same subnet gets medium priority 
        3. External addresses get lowest priority
        4. Could be enhanced with actual ping measurements
        
        Returns a score where lower values indicate closer proximity
        """
        try:
            import socket
            import ipaddress
            
            # Get local IP address
            local_ip = socket.gethostbyname(socket.gethostname())
            
            # Handle localhost addresses
            if address in ['localhost', '127.0.0.1', '0.0.0.0']:
                return 0.1  # Highest priority (lowest score)
            
            # Handle same machine
            if address == local_ip:
                return 0.2
            
            # Check if in same subnet (simple check for common subnets)
            try:
                local_network = ipaddress.IPv4Network(f"{local_ip}/24", strict=False)
                target_ip = ipaddress.IPv4Address(address)
                
                if target_ip in local_network:
                    return 1.0  # Same subnet
                else:
                    return 2.0  # Different subnet/external
                    
            except (ipaddress.AddressValueError, ValueError):
                # If IP parsing fails, assume external
                return 3.0
                
        except Exception as e:
            self.logger.debug(f"Error calculating proximity to {address}:{port}: {e}")
            # Return a default score if calculation fails
            return 5.0

    def sort_locations_by_proximity(self, locations: List[dict]) -> List[dict]:
        """
        Sort chunk server locations by network proximity.
        Closer servers are preferred for better read performance.
        """
        if not locations:
            return locations
        
        # Calculate proximity scores for each location
        scored_locations = []
        for location in locations:
            proximity_score = self.calculate_network_proximity(
                location['address'], 
                location['port']
            )
            scored_locations.append((proximity_score, location))
        
        # Sort by proximity score (lower is better)
        scored_locations.sort(key=lambda x: x[0])
        
        # Return sorted locations without scores
        sorted_locations = [location for score, location in scored_locations]
        
        # Log the sorting for debugging
        if len(sorted_locations) > 1:
            self.logger.debug(f"Sorted {len(sorted_locations)} chunk servers by proximity:")
            for i, location in enumerate(sorted_locations):
                score = scored_locations[i][0]
                self.logger.debug(f"  {i+1}. {location['server_id']} at {location['address']}:{location['port']} (score: {score})")
        
        return sorted_locations

    def read_chunk(self, chunk_handle: str, offset: int, length: int,
                   retried_fresh: bool = False) -> Optional[bytes]:
        """
        Read data from a chunk - implements GFS read data flow with network proximity optimization:
        1. Client contacts master to get chunk locations
        2. Client sorts replicas by network proximity (closest first)
        3. Client reads directly from replicas in order of proximity
        4. No master involvement in data transfer
        """
        locations = self.get_chunk_locations(chunk_handle)
        if not locations:
            # Could be a genuinely missing chunk, a stale empty cache entry,
            # or a chunk that has been erasure-coded and no longer has whole
            # replicas at all. Refresh once to find out which.
            self._forget_chunk(chunk_handle)
            locations = self.get_chunk_locations(chunk_handle)

        if not locations and chunk_handle in self._ec_info:
            return self._read_chunk_ec(chunk_handle, offset, length)

        if not locations:
            raise Exception(f"No locations found for chunk {chunk_handle}")

        # GFS optimization: sort replicas by network proximity
        sorted_locations = self.sort_locations_by_proximity(locations)
        
        # Try replicas in order of proximity (closest first)
        connection_errors = []
        read_errors = []
        
        for i, location in enumerate(sorted_locations):
            try:
                stub = self.get_chunk_server_stub(location['address'], location['port'])
                if not stub:
                    connection_errors.append(f"Could not connect to chunk server {location['server_id']} at {location['address']}:{location['port']}")
                    continue

                request = gfs_pb2.ReadChunkRequest(
                    chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                    offset=offset,
                    length=length
                )
                response = stub.ReadChunk(request, timeout=60.0)
                
                if response.success:
                    proximity_info = f" (replica {i+1}/{len(sorted_locations)})"
                    self.logger.debug(f"Read {len(response.data)} bytes from chunk {chunk_handle} at {location['server_id']}{proximity_info}")
                    return response.data
                else:
                    read_errors.append(f"Failed to read from {location['server_id']}: {response.message}")
            except Exception as e:
                connection_errors.append(f"Error reading from {location['server_id']} at {location['address']}:{location['port']}: {str(e)}")

        # If we reach here, all chunk servers failed
        error_details = []
        if connection_errors:
            error_details.append(f"Connection errors: {'; '.join(connection_errors)}")
        if read_errors:
            error_details.append(f"Read errors: {'; '.join(read_errors)}")
        
        # Every replica we knew about failed. If those came from the cache,
        # the most likely explanation is that the chunk moved -- a server
        # died and re-replication put it somewhere else. Drop the entry and
        # ask the master once before giving up.
        #
        # This is the half of caching that makes it safe rather than merely
        # fast: without it the client keeps hammering dead replicas until the
        # TTL happens to expire. ADR-006.
        if not retried_fresh and self._forget_chunk(chunk_handle):
            self.logger.info(
                f"all cached replicas for {chunk_handle} failed; refreshing from master")
            # The chunk may have been erasure-coded since this map was
            # cached, in which case the refreshed answer has no replicas at
            # all and the read continues down the shard path.
            return self.read_chunk(chunk_handle, offset, length, retried_fresh=True)

        full_error_msg = f"Unable to reach any chunk server for chunk {chunk_handle}. Tried {len(sorted_locations)} replicas. " + "; ".join(error_details)

        self.logger.error(full_error_msg)
        raise Exception(full_error_msg)


    # ------------------------------------------------------------------
    # Erasure-coded reads (ADR-005)
    # ------------------------------------------------------------------
    def _forget_chunk(self, chunk_handle: str) -> bool:
        """Drop everything cached about one chunk's placement.

        Returns whether anything was actually dropped, so a caller can tell
        'our cache was wrong, retry' from 'we had nothing cached, so this
        answer is already fresh' -- without which the retry recurses.
        """
        dropped = self.cache.invalidate_chunk(chunk_handle)
        if self._ec_info.pop(chunk_handle, None) is not None:
            dropped = True
        return dropped

    def _read_chunk_ec(self, chunk_handle: str, offset: int, length: int,
                       retried_fresh: bool = False):
        """Reconstruct a chunk from its shards.

        Fetches shards until k are in hand, then decodes. Data shards are
        tried first: the code is systematic, so if all k data shards are
        present the "decode" is a concatenation and costs nothing. Only when
        a data shard is actually missing does this pay a matrix inversion --
        which is why a healthy EC read is nearly as cheap as a replica read
        and a degraded one is not.
        """
        info = self._ec_info.get(chunk_handle)
        if not info:
            raise Exception(f"No erasure-coding info for chunk {chunk_handle}")

        k, m = info['k'], info['m']
        shards = sorted(info['shards'], key=lambda s: s['shard_index'])
        data_first = ([s for s in shards if s['shard_index'] < k]
                      + [s for s in shards if s['shard_index'] >= k])

        collected, failures = {}, []
        for shard in data_first:
            if len(collected) >= k:
                break
            try:
                with grpc.insecure_channel(f"{shard['address']}:{shard['port']}",
                                           options=GRPC_CHANNEL_OPTIONS) as ch:
                    stub = gfs_pb2_grpc.ChunkServerServiceStub(ch)
                    resp = stub.ReadShard(gfs_pb2.ReadShardRequest(
                        chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                        shard_index=shard['shard_index']), timeout=60.0)
                    if resp.success:
                        collected[shard['shard_index']] = resp.data
                    else:
                        failures.append(f"{shard['server_id']}: {resp.message}")
            except grpc.RpcError as e:
                failures.append(f"{shard['server_id']}: {e.code()}")

        if len(collected) < k:
            # A cached shard map goes stale for two ordinary reasons: a
            # server died, or repair rebuilt the shard somewhere else. Drop
            # it and ask the master once before declaring the chunk
            # unreadable -- the same invalidate-on-contradiction rule the
            # replica path uses (ADR-006).
            self.cache.invalidate_chunk(chunk_handle)
            had_map = self._ec_info.pop(chunk_handle, None) is not None
            if had_map and not retried_fresh:
                self.logger.info(
                    f"stale shard map for {chunk_handle} "
                    f"({len(collected)}/{k} reachable); refreshing from master")
                self.get_chunk_locations(chunk_handle)
                if chunk_handle in self._ec_info:
                    return self._read_chunk_ec(chunk_handle, offset, length,
                                               retried_fresh=True)
            raise Exception(
                f"chunk {chunk_handle}: only {len(collected)} of {k} shards "
                f"reachable; {'; '.join(failures[:4])}")

        degraded = any(i not in collected for i in range(k))
        data = decode_chunk(collected, k, m, info['original_length'])
        if degraded:
            self.logger.info(
                f"degraded read of {chunk_handle}: reconstructed from "
                f"{len(collected)} shards")
        return data[offset:offset + length] if (offset or length) else data

    def write_chunk_with_lease(self, chunk_handle: str, offset: int, data: bytes, locations: List[dict]) -> bool:
        """
        Write data to a chunk following GFS write data flow:
        1. Client gets lease from master (primary replica)
        2. Client pushes data to ALL replicas in any order
        3. Client sends write request to primary
        4. Primary forwards write request to all secondaries
        5. Primary replies to client after all secondaries respond
        
        Note: This simplified version pushes data and sends write request together
        """
        if not locations:
            self.logger.error(f"No locations provided for chunk {chunk_handle}")
            return False

        # In GFS, the master designates a primary replica and grants it a lease
        # For simplicity, we assume the first location is the primary
        primary_location = locations[0]
        secondary_locations = locations[1:]

        # Step 1 & 2: Push data to all replicas (including primary)
        # In the real GFS, this is done in parallel and efficiently using a chain
        successful_replicas = []
        
        for location in locations:
            try:
                stub = self.get_chunk_server_stub(location['address'], location['port'])
                if not stub:
                    self.logger.warning(f"Could not connect to {location['server_id']}")
                    continue

                # For now, we send write request directly to each replica
                # In real GFS, data is pushed separately and then write command is sent
                request = gfs_pb2.WriteChunkRequest(
                    chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                    offset=offset,
                    data=data,
                    lease_id="temp_lease"  # TODO: Get actual lease from master
                )
                
                response = stub.WriteChunk(request, timeout=30.0)
                
                if response.success:
                    successful_replicas.append(location['server_id'])
                    self.logger.debug(f"Successfully wrote to {location['server_id']}")
                else:
                    self.logger.warning(f"Failed to write to {location['server_id']}: {response.message}")
                    
            except Exception as e:
                self.logger.warning(f"Error writing to {location['server_id']}: {e}")

        # Consider write successful if we wrote to at least the majority of replicas
        min_successful = len(locations) // 2 + 1
        success = len(successful_replicas) >= min_successful
        
        if success:
            self.logger.debug(f"Wrote {len(data)} bytes to chunk {chunk_handle} ({len(successful_replicas)}/{len(locations)} replicas)")
        else:
            self.logger.error(f"Write failed: only {len(successful_replicas)}/{len(locations)} replicas succeeded (needed {min_successful})")
            
        return success

    def write_chunk(self, chunk_handle: str, offset: int, data: bytes, locations: List[dict]) -> bool:
        """Legacy method - delegates to the proper lease-based write"""
        return self.write_chunk_with_lease(chunk_handle, offset, data, locations)

    def append_chunk_atomic(self, chunk_handle: str, data: bytes, locations: List[dict]) -> Optional[int]:
        """
        Perform atomic record append following GFS data flow:
        1. Client pushes data to all replicas
        2. Client sends append request to primary
        3. Primary checks if record fits in current chunk
        4. If yes: primary appends locally and tells secondaries to append at same offset
        5. If no: primary tells client chunk is full, client tries next chunk
        6. Primary responds with the offset where data was appended
        """
        if not locations:
            self.logger.error(f"No locations provided for chunk {chunk_handle}")
            return None

        # Primary is the first location (designated by master)
        primary_location = locations[0]
        
        try:
            primary_stub = self.get_chunk_server_stub(primary_location['address'], primary_location['port'])
            if not primary_stub:
                self.logger.error(f"Could not connect to primary {primary_location['server_id']}")
                return None

            # Send append request to primary - it will coordinate with secondaries
            request = gfs_pb2.AppendChunkRequest(
                chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                data=data,
                lease_id="temp_lease"  # TODO: Get actual lease from master
            )
            
            response = primary_stub.AppendChunk(request, timeout=30.0)
            
            if response.success:
                self.logger.debug(f"Appended {len(data)} bytes to chunk {chunk_handle} at offset {response.offset}")
                return response.offset
            else:
                self.logger.warning(f"Append failed at primary {primary_location['server_id']}: {response.message}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error during append to {primary_location['server_id']}: {e}")
            return None

    def read_file(self, filename: str, offset: int = 0, length: int = None) -> Optional[bytes]:
        """
        Read data from a file following GFS read data flow:
        1. Client asks master for file metadata and chunk locations
        2. Client reads directly from chunk servers
        3. No master involvement in actual data transfer
        """
        filename = self.normalize_path(filename)

        file_info = self.get_file_info(filename)
        if not file_info:
            return None

        file_size = file_info['size']
        if length is None:
            length = file_size - offset

        if offset + length > file_size:
            length = file_size - offset

        if length <= 0:
            return b''

        # Calculate which chunks to read
        start_chunk = offset // CHUNK_SIZE
        end_chunk = (offset + length - 1) // CHUNK_SIZE
        
        result_data = b''
        current_offset = offset

        for chunk_index in range(start_chunk, end_chunk + 1):
            if chunk_index >= len(file_info['chunks']):
                break

            chunk_handle = file_info['chunks'][chunk_index]
            if not chunk_handle:
                # Sparse file - fill with zeros
                chunk_data = b'\x00' * min(CHUNK_SIZE, length - len(result_data))
            else:
                # Calculate read parameters for this chunk
                chunk_start_offset = chunk_index * CHUNK_SIZE
                read_offset = max(0, current_offset - chunk_start_offset)
                read_length = min(CHUNK_SIZE - read_offset, length - len(result_data))

                chunk_data = self.read_chunk(chunk_handle, read_offset, read_length)
                if chunk_data is None:
                    self.logger.error(f"Failed to read chunk {chunk_index} of file {filename}")
                    return None

            result_data += chunk_data
            current_offset += len(chunk_data)

            if len(result_data) >= length:
                break

        return result_data[:length]

    def _write_file_with_shifting_impl(self, filename: str, data: bytes, offset: int = 0) -> bool:
        """
        Write data to a file with proper handling of data shifting across chunks.
        This handles the complex case where writing to the middle of a file can affect multiple chunks.
        
        Algorithm:
        1. Read all existing data from affected chunks
        2. Merge existing data with new data at the specified offset
        3. Redistribute the merged data across chunks
        4. Write all affected chunks back to storage
        5. Update file size metadata in master
        """
        filename = self.normalize_path(filename)

        if not data:
            return True

        # Ensure file exists
        file_info = self.get_file_info(filename)
        if not file_info:
            if not self.create_file(filename):
                return False
            file_info = self.get_file_info(filename)
            if not file_info:
                return False

        original_file_size = file_info['size']
        new_file_size = max(original_file_size, offset + len(data))
        
        # Calculate which chunks might be affected
        start_chunk = offset // CHUNK_SIZE
        end_chunk = (offset + len(data) - 1) // CHUNK_SIZE
        
        # If writing beyond current file size, we might need to extend to more chunks
        if new_file_size > original_file_size:
            end_chunk = max(end_chunk, (new_file_size - 1) // CHUNK_SIZE)
        
        self.logger.info(f"Writing {len(data)} bytes at offset {offset} affects chunks {start_chunk}-{end_chunk}")
        
        # Step 1: Read all existing data from affected chunk range
        existing_data = b''
        chunk_start_offset = start_chunk * CHUNK_SIZE
        
        for chunk_index in range(start_chunk, end_chunk + 1):
            if chunk_index < len(file_info['chunks']) and file_info['chunks'][chunk_index]:
                # Chunk exists, read its data
                chunk_handle = file_info['chunks'][chunk_index]
                try:
                    chunk_data = self.read_chunk(chunk_handle, 0, CHUNK_SIZE)
                    if chunk_data is None:
                        self.logger.error(f"Failed to read existing chunk {chunk_index}")
                        return False
                    existing_data += chunk_data
                except Exception as e:
                    self.logger.error(f"Error reading chunk {chunk_index}: {e}")
                    return False
            else:
                # Chunk doesn't exist or is sparse, fill with zeros
                existing_data += b'\x00' * CHUNK_SIZE
        
        # Trim existing_data to actual file content (remove padding zeros at end)
        file_end_in_chunk_range = min(len(existing_data), original_file_size - chunk_start_offset)
        if file_end_in_chunk_range > 0:
            existing_data = existing_data[:file_end_in_chunk_range]
        else:
            existing_data = b''
        
        # Step 2: Merge new data with existing data
        write_offset_in_range = offset - chunk_start_offset
        
        # Create the new merged data
        merged_data = existing_data[:write_offset_in_range] + data
        
        # Add any existing data that comes after the write
        if write_offset_in_range + len(data) < len(existing_data):
            merged_data += existing_data[write_offset_in_range + len(data):]
        
        self.logger.debug(f"Original data length: {len(existing_data)}, merged data length: {len(merged_data)}")
        
        # Step 3: Write merged data back to chunks
        current_data_offset = 0
        
        for chunk_index in range(start_chunk, end_chunk + 1):
            # Calculate how much data goes in this chunk
            remaining_data = len(merged_data) - current_data_offset
            if remaining_data <= 0:
                # No more data to write, but we might need to clear this chunk if it existed
                if chunk_index < len(file_info['chunks']) and file_info['chunks'][chunk_index]:
                    self.logger.info(f"Chunk {chunk_index} no longer needed, should be cleared")
                break
                
            chunk_data_length = min(CHUNK_SIZE, remaining_data)
            chunk_data = merged_data[current_data_offset:current_data_offset + chunk_data_length]
            
            # Allocate chunk if it doesn't exist
            if chunk_index >= len(file_info['chunks']) or not file_info['chunks'][chunk_index]:
                result = self.allocate_chunk(filename, chunk_index)
                if not result:
                    self.logger.error(f"Failed to allocate chunk {chunk_index} for file {filename}")
                    return False
                
                chunk_handle, locations = result
                
                # Refresh file info to get updated chunk list
                file_info = self.get_file_info(filename)
                if not file_info:
                    return False
            else:
                chunk_handle = file_info['chunks'][chunk_index]
                locations = self.get_chunk_locations(chunk_handle)
            
            # Write the chunk data (always at offset 0 since we're rewriting entire chunks)
            if not self.write_chunk_with_lease(chunk_handle, 0, chunk_data, locations):
                self.logger.error(f"Failed to write chunk {chunk_index} of file {filename}")
                return False
            
            current_data_offset += chunk_data_length
            self.logger.debug(f"Wrote chunk {chunk_index} with {len(chunk_data)} bytes")

        # Step 4: CRITICAL FIX - Update file size metadata in master
        if not self.update_file_size(filename, new_file_size, offset, len(data)):
            self.logger.warning(f"Failed to update file size metadata for {filename}")
            # Don't fail the write operation just because metadata update failed

        self.logger.info(f"Successfully wrote {len(data)} bytes to file {filename} with data shifting")
        return True

    
    def _write_file_impl(self, filename: str, data: bytes, offset: int = 0) -> bool:
        """Write data to a file with proper GFS lease protocol"""
        filename = self.normalize_path(filename)

        if not data:
            return True

        try:
            # Ensure file exists
            file_info = self.get_file_info(filename)
            if not file_info:
                if not self.create_file(filename):
                    return False
                file_info = self.get_file_info(filename)
                if not file_info:
                    return False

            # Calculate which chunks need to be written
            start_chunk = offset // CHUNK_SIZE
            end_chunk = (offset + len(data) - 1) // CHUNK_SIZE

            current_data_offset = 0

            for chunk_index in range(start_chunk, end_chunk + 1):
                # Calculate write parameters for this chunk
                chunk_start_offset = chunk_index * CHUNK_SIZE
                write_offset_in_chunk = max(0, offset - chunk_start_offset)
                remaining_data = len(data) - current_data_offset
                bytes_to_write = min(remaining_data, CHUNK_SIZE - write_offset_in_chunk)
                
                # Get the data slice for this chunk
                chunk_data = data[current_data_offset:current_data_offset + bytes_to_write]
                
                # Allocate chunk if it doesn't exist
                if chunk_index >= len(file_info['chunks']) or not file_info['chunks'][chunk_index]:
                    result = self.allocate_chunk(filename, chunk_index)
                    if not result:
                        self.logger.error(f"Failed to allocate chunk {chunk_index} for file {filename}")
                        return False
                    
                    chunk_handle, locations = result
                    
                    # Refresh file info
                    file_info = self.get_file_info(filename)
                    if not file_info:
                        return False
                else:
                    chunk_handle = file_info['chunks'][chunk_index]
                    locations = self.get_chunk_locations(chunk_handle)

                if not locations:
                    self.logger.error(f"No locations available for chunk {chunk_index}")
                    return False

                # Write to this chunk using proper GFS protocol
                if not self._write_chunk_with_lease(chunk_handle, write_offset_in_chunk, chunk_data, locations):
                    self.logger.error(f"Failed to write chunk {chunk_index} of file {filename}")
                    return False

                current_data_offset += bytes_to_write

                if current_data_offset >= len(data):
                    break

            self.logger.info(f"Wrote {len(data)} bytes to file {filename}")
            return True
        
        except Exception as e:
            self.logger.error(f"Write failed: {e}")
            return False

    
    def _write_file_simple_impl(self, filename: str, data: bytes, offset: int = 0) -> bool:
        """
        Simple write implementation for writes to end of file (original algorithm)
        """
        # Ensure file exists
        file_info = self.get_file_info(filename)
        if not file_info:
            if not self.create_file(filename):
                return False
            file_info = self.get_file_info(filename)
            if not file_info:
                return False

        original_file_size = file_info['size']
        new_file_size = max(original_file_size, offset + len(data))

        # Calculate which chunks need to be written
        start_chunk = offset // CHUNK_SIZE
        end_chunk = (offset + len(data) - 1) // CHUNK_SIZE

        current_data_offset = 0
        current_file_offset = offset

        for chunk_index in range(start_chunk, end_chunk + 1):
            # Allocate chunk if it doesn't exist
            if chunk_index >= len(file_info['chunks']) or not file_info['chunks'][chunk_index]:
                result = self.allocate_chunk(filename, chunk_index)
                if not result:
                    self.logger.error(f"Failed to allocate chunk {chunk_index} for file {filename}")
                    return False
                
                chunk_handle, locations = result
                
                # Update file info
                file_info = self.get_file_info(filename)
                if not file_info:
                    return False
            else:
                chunk_handle = file_info['chunks'][chunk_index]
                locations = self.get_chunk_locations(chunk_handle)

            # Calculate write parameters for this chunk
            chunk_start_offset = chunk_index * CHUNK_SIZE
            write_offset = current_file_offset - chunk_start_offset
            remaining_data = len(data) - current_data_offset
            write_length = min(CHUNK_SIZE - write_offset, remaining_data)

            chunk_data = data[current_data_offset:current_data_offset + write_length]

            # Use lease-based write protocol
            if not self.write_chunk_with_lease(chunk_handle, write_offset, chunk_data, locations):
                self.logger.error(f"Failed to write chunk {chunk_index} of file {filename}")
                return False

            current_data_offset += write_length
            current_file_offset += write_length

            if current_data_offset >= len(data):
                break

        # CRITICAL FIX - Update file size metadata in master
        if not self.update_file_size(filename, new_file_size, offset, len(data)):
            self.logger.warning(f"Failed to update file size metadata for {filename}")
            # Don't fail the write operation just because metadata update failed

        self.logger.info(f"Wrote {len(data)} bytes to file {filename}")
        return True

    # def append_file(self, filename: str, data: bytes) -> bool:
    #     """
    #     Append data to a file using GFS record append:
    #     1. Client asks master for the last chunk of the file
    #     2. Client attempts atomic record append to that chunk
    #     3. If chunk is full, master allocates new chunk and client retries
    #     """
    #     filename = self.normalize_path(filename)

    #     file_info = self.get_file_info(filename)
    #     if not file_info:
    #         if not self.create_file(filename):
    #             return False
    #         file_info = self.get_file_info(filename)
    #         if not file_info:
    #             return False

    #     # If file has no chunks, allocate the first one
    #     if not file_info['chunks']:
    #         result = self.allocate_chunk(filename, 0)
    #         if not result:
    #             self.logger.error(f"Failed to allocate initial chunk for file {filename}")
    #             return False
            
    #         chunk_handle, locations = result
    #         chunk_index = 0
    #     else:
    #         # Use the last chunk for append
    #         chunk_index = len(file_info['chunks']) - 1
    #         chunk_handle = file_info['chunks'][chunk_index]
    #         if not chunk_handle:
    #             # Last chunk is sparse, allocate it
    #             result = self.allocate_chunk(filename, chunk_index)
    #             if not result:
    #                 return False
    #             chunk_handle, locations = result
    #         else:
    #             locations = self.get_chunk_locations(chunk_handle)

    #     # Attempt atomic record append
    #     max_retries = 3
    #     for retry in range(max_retries):
    #         append_offset = self.append_chunk_atomic(chunk_handle, data, locations)
            
    #         if append_offset is not None:
    #             self.logger.info(f"Appended {len(data)} bytes to file {filename} at offset {append_offset}")
    #             return True
            
    #         # If append failed, it might be because chunk is full
    #         # Allocate a new chunk and try again
    #         self.logger.info(f"Chunk {chunk_index} full, allocating new chunk for append (retry {retry + 1})")
            
    #         result = self.allocate_chunk(filename, chunk_index + 1)
    #         if not result:
    #             self.logger.error(f"Failed to allocate new chunk for append to {filename}")
    #             return False
            
    #         chunk_handle, locations = result
    #         chunk_index += 1

    #     self.logger.error(f"Failed to append to file {filename} after {max_retries} retries")
    #     return False

    def _append_file_impl(self, filename: str, data: bytes) -> bool:
        """Append data to a file using GFS atomic record append
        
        GFS Record Append (Section 3.3):
        - Client specifies only the data (no offset)
        - GFS appends at least once atomically
        - Returns the offset where data was written
        - Follows same control flow as write with extra logic at primary
        - Record append restricted to at most 1/4 of max chunk size (16MB)
        """
        filename = self.normalize_path(filename)
        
        # Validate record size (max 16MB = 1/4 of 64MB chunk)
        MAX_RECORD_SIZE = CHUNK_SIZE // 4  # 16MB
        if len(data) > MAX_RECORD_SIZE:
            self.logger.error(f"Record too large: {len(data)} bytes (max {MAX_RECORD_SIZE})")
            return False
        
        # Get or create file
        file_info = self.get_file_info(filename)
        if not file_info:
            if not self.create_file(filename):
                return False
            file_info = self.get_file_info(filename)
        
        # Use atomic record append to last chunk
        return self._append_to_last_chunk(filename, file_info, data)
    
    def _append_to_last_chunk(self, filename: str, file_info: Dict, data: bytes) -> bool:
        """Append data to the last chunk of a file using GFS record append protocol"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Get the last chunk (or allocate if none exists)
                chunks = file_info.get('chunks', [])
                if not chunks or chunks[-1] is None:
                    # Allocate new chunk
                    chunk_index = len(chunks)
                    chunk_handle, locations = self.allocate_chunk(filename, chunk_index)
                    if not chunk_handle:
                        self.logger.error("Failed to allocate chunk for append")
                        return False
                else:
                    # Use last chunk
                    chunk_handle = chunks[-1]
                    locations = self.get_chunk_locations(chunk_handle)
                    if not locations:
                        self.logger.error(f"No locations for chunk {chunk_handle}")
                        return False
                
                # Convert locations to dict format
                locations_dict = []
                for loc in locations:
                    if hasattr(loc, 'server_id'):
                        locations_dict.append({
                            'server_id': loc.server_id,
                            'address': loc.address,
                            'port': loc.port
                        })
                    else:
                        locations_dict.append(loc)
                
                # Perform atomic record append
                result_offset = self._record_append_with_lease(
                    chunk_handle, data, locations_dict, filename, len(chunks) - 1
                )
                
                if result_offset >= 0:
                    self.logger.info(f"✅ Record append successful at offset {result_offset}")
                    return True
                elif result_offset == -2:
                    # Chunk is full, need to retry with next chunk
                    self.logger.info(f"Chunk full, allocating next chunk for append")
                    # Allocate next chunk and retry
                    chunk_index = len(chunks)
                    chunk_handle, locations = self.allocate_chunk(filename, chunk_index)
                    if not chunk_handle:
                        self.logger.error("Failed to allocate next chunk")
                        return False
                    # Refresh file info and continue to next attempt
                    file_info = self.get_file_info(filename)
                    continue
                else:
                    self.logger.error(f"Record append failed with offset {result_offset}")
                    time.sleep(0.1 * (2 ** attempt))
                    continue
                    
            except Exception as e:
                self.logger.error(f"Append attempt {attempt + 1} failed: {e}")
                time.sleep(0.1 * (2 ** attempt))
        
        return False


    # ------------------------------------------------------------------
    # Cache-invalidating wrappers around the mutating paths (ADR-006)
    # ------------------------------------------------------------------
    # Invalidating inside the implementations is not enough: they call
    # get_file_info() again on the way out, which re-caches the size the
    # master had *before* the write committed. The next read then truncates
    # to the stale length -- silently, and only for the client that did the
    # write. Dropping the entry on exit is the only ordering that holds
    # regardless of what the implementation does internally.

    def _after_mutation(self, filename: str, result):
        self.cache.invalidate_file(self.normalize_path(filename))
        return result

    def write_file(self, filename: str, data: bytes, offset: int = 0) -> bool:
        return self._after_mutation(
            filename, self._write_file_impl(filename, data, offset))

    def write_file_simple(self, filename: str, data: bytes, offset: int = 0) -> bool:
        return self._after_mutation(
            filename, self._write_file_simple_impl(filename, data, offset))

    def write_file_with_shifting(self, filename: str, data: bytes, offset: int = 0) -> bool:
        return self._after_mutation(
            filename, self._write_file_with_shifting_impl(filename, data, offset))

    def append_file(self, filename: str, data: bytes) -> bool:
        return self._after_mutation(
            filename, self._append_file_impl(filename, data))

    def get_cluster_status(self) -> Dict:
        """Get status of the GFS cluster"""
        def _get_cluster_status_op(stub):
            request = gfs_pb2.GetPrimaryMasterRequest(requester_id="client")
            response = stub.GetPrimaryMaster(request, timeout=10.0)
            if response.success:
                # FIXED: Map internal addresses back to client-side addresses
                primary_master_info = {
                    'master_id': response.primary_master.master_id,
                    'address': response.primary_master.address,  # Keep internal address for reference
                    'port': response.primary_master.port,
                    'term': response.primary_master.term,
                    'is_primary': response.primary_master.is_primary
                }
                
                # Find the client-side address for the primary master
                primary_client_address = self.current_leader  # We know this is the leader
                if primary_client_address:
                    primary_master_info['client_address'] = primary_client_address
                
                all_masters_info = []
                for master in response.all_masters:
                    master_info = {
                        'master_id': master.master_id,
                        'address': master.address,  # Internal address
                        'port': master.port,
                        'term': master.term,
                        'is_primary': master.is_primary,
                        'last_heartbeat': master.last_heartbeat
                    }
                    
                    # Try to find the client-side address for this master
                    if master.is_primary and self.current_leader:
                        master_info['client_address'] = self.current_leader
                    else:
                        # For non-primary masters, we need to map them to our provided addresses
                        # We can't be 100% sure without more info, but we can make educated guesses
                        master_info['client_address'] = 'unknown'
                        for addr in self.master_addresses:
                            if addr != self.current_leader:  # Not the current leader
                                master_info['client_address'] = addr
                                break
                    
                    all_masters_info.append(master_info)
                
                return {
                    'primary_master': primary_master_info,
                    'all_masters': all_masters_info
                }
            return None
        
        try:
            return self._execute_with_leader_failover(_get_cluster_status_op)
        except Exception as e:
            self.logger.error(f"Error getting cluster status: {e}")
            return None

    def update_file_size(self, filename: str, new_size: int, offset: int, length: int) -> bool:
        """Update file size metadata in master after successful write"""
        filename = self.normalize_path(filename)

        def _update_file_size_op(stub, filename, new_size, offset, length):
            request = gfs_pb2.UpdateFileSizeRequest(
                filename=filename,
                new_size=new_size,
                offset=offset,
                length=length
            )
            response = stub.UpdateFileSize(request, timeout=10.0)
            if response.success:
                self.logger.debug(f"Updated file size for {filename}: {new_size} bytes")
                return True
            else:
                self.logger.error(f"Failed to update file size for {filename}: {response.message}")
                return False
        
        try:
            ok = self._execute_with_leader_failover(
                _update_file_size_op, filename, new_size, offset, length)
            if ok:
                # Size changed; a cached GetFileInfo would now under-report
                # and reads would silently truncate.
                self.cache.invalidate_file(filename)
            return ok
        except Exception as e:
            self.logger.error(f"Error updating file size for {filename}: {e}")
            return False

    # Directory Operations
    def create_directory(self, directory_path: str) -> bool:
        """Create a new directory"""
        directory_path = self.normalize_path(directory_path)

        def _create_directory_op(stub, directory_path):
            request = gfs_pb2.CreateDirectoryRequest(directory_path=directory_path)
            response = stub.CreateDirectory(request, timeout=10.0)
            if response.success:
                self.logger.info(f"Created directory: {directory_path}")
                return True
            else:
                self.logger.error(f"Failed to create directory {directory_path}: {response.message}")
                return False
        
        try:
            ok = self._execute_with_leader_failover(_create_directory_op, directory_path)
            if ok:
                self.cache.invalidate_tree(directory_path)
            return ok
        except Exception as e:
            self.logger.error(f"Error creating directory {directory_path}: {e}")
            return False

    def delete_directory(self, directory_path: str, recursive: bool = False) -> bool:
        """Delete a directory"""
        directory_path = self.normalize_path(directory_path)

        def _delete_directory_op(stub, directory_path, recursive):
            request = gfs_pb2.DeleteDirectoryRequest(directory_path=directory_path, recursive=recursive)
            response = stub.DeleteDirectory(request, timeout=10.0)
            if response.success:
                self.logger.info(f"Deleted directory: {directory_path} ({response.items_deleted} items)")
                return True
            else:
                self.logger.error(f"Failed to delete directory {directory_path}: {response.message}")
                return False
        
        try:
            ok = self._execute_with_leader_failover(
                _delete_directory_op, directory_path, recursive)
            if ok:
                # A recursive delete removes files the client never named,
                # so the whole subtree has to go -- there is no way to
                # enumerate what the master considered to be under it.
                self.cache.invalidate_tree(directory_path)
            return ok
        except Exception as e:
            self.logger.error(f"Error deleting directory {directory_path}: {e}")
            return False

    def list_directory(self, directory_path: str = '/') -> Optional[Dict[str, str]]:
        """List directory contents with types"""
        directory_path = self.normalize_path(directory_path)

        def _list_directory_op(stub, directory_path):
            request = gfs_pb2.ListDirectoryRequest(directory_path=directory_path)
            response = stub.ListDirectory(request, timeout=10.0)
            if response.success:
                entries = {}
                for entry in response.entries:
                    entries[entry.name] = entry.type
                return entries
            else:
                self.logger.error(f"Failed to list directory {directory_path}: {response.message}")
                return None
        
        try:
            return self._execute_with_leader_failover(_list_directory_op, directory_path)
        except Exception as e:
            self.logger.error(f"Error listing directory {directory_path}: {e}")
            return None

    def directory_exists(self, directory_path: str) -> bool:
        """Check if a directory exists"""
        directory_path = self.normalize_path(directory_path)
        entries = self.list_directory(directory_path)
        return entries is not None

    def list_all_files(self) -> Optional[Dict[str, Dict]]:
        """List all files and directories in the entire filesystem"""
        def _list_all_files_op(stub):
            request = gfs_pb2.ListAllFilesRequest()
            response = stub.ListAllFiles(request, timeout=10.0)
            if response.success:
                all_items = {}
                for entry in response.entries:
                    all_items[entry.path] = {
                        'type': entry.type,
                        'size': entry.size,
                        'created_time': entry.created_time,
                        'modified_time': entry.modified_time
                    }
                return all_items
            else:
                self.logger.error(f"Failed to list all files: {response.message}")
                return None
        
        try:
            return self._execute_with_leader_failover(_list_all_files_op)
        except Exception as e:
            self.logger.error(f"Error listing all files: {e}")
            return None


    def get_leader_stub(self):
        """Get the leader master stub"""
        if not self.current_leader:
            return None
        return self.get_master_stub(self.current_leader)

    def _ip_prefix_distance(self, ip1: str, ip2: str) -> int:
        """Return a distance score based on IP prefix matching (lower is closer)."""
        # Normalize localhost
        if ip1 in ['localhost', '127.0.0.1', '0.0.0.0', '::1', '']:
            ip1 = '127.0.0.1'
        if ip2 in ['localhost', '127.0.0.1', '0.0.0.0', '::1', '']:
            ip2 = '127.0.0.1'
        if ip1 == ip2:
            return 0
        try:
            o1 = ip1.split('.')
            o2 = ip2.split('.')
            if len(o1) != 4 or len(o2) != 4:
                return 100  # Not IPv4, treat as far
            for i in range(4):
                if o1[i] != o2[i]:
                    return 4 - i  # 0: same, 1: /24, 2: /16, 3: /8, 4: none
            return 0
        except Exception:
            return 100

    def _build_pipeline(self, locations: List[dict], client_ip: str = '127.0.0.1') -> List[dict]:
        """Sort chunkserver locations by proximity to client IP for pipelined push.
        
        Uses IP prefix distance as primary sort key, with port as tiebreaker.
        This ensures deterministic pipeline order on localhost (all IPs same)
        while preserving network topology awareness in production.
        """
        # Remove duplicates (by address:port)
        unique = {(loc['address'], loc['port']): loc for loc in locations}
        locations = list(unique.values())
        
        # Sort by IP distance (primary), then port (secondary tiebreaker)
        return sorted(
            locations, 
            key=lambda loc: (
                self._ip_prefix_distance(client_ip, loc['address']),
                loc['port']  # ✅ Tiebreaker for equal IP distances (localhost scenario)
            )
        )

    def _write_chunk_with_lease(self, chunk_handle: str, offset: int, data: bytes, locations: List[dict]) -> bool:
        """Write data to chunk following proper GFS protocol with pipelined data push.
        
        GFS Write Protocol (from paper):
        1. Client asks master for lease holder (primary) and replica locations
        2. Master replies with primary identity and replica locations
        3. Client pushes data to ALL replicas (we use pipelined chain for efficiency)
        4. Once all replicas acknowledge, client sends write request to primary
        5. Primary assigns serial numbers and applies mutation locally
        6. Primary forwards write request to all secondaries
        7. Secondaries reply to primary, primary replies to client
        
        Our Implementation:
        - Steps 1-2: RequestLease RPC to master
        - Step 3: PushData to first server, which forwards through pipeline chain
                  (optimization: instead of client→all, we do client→server1→server2→server3)
        - Step 4: Wait for pipeline completion, then send WriteChunk to primary
        - Steps 5-7: Handled by chunk servers (primary coordinates with secondaries)
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # === GFS STEP 1: Request lease from master ===
                leader_stub = self.get_leader_stub()
                if not leader_stub:
                    self.logger.error("No leader available for lease request")
                    continue
                    
                lease_response = leader_stub.RequestLease(
                    gfs_pb2.RequestLeaseRequest(
                        chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle)
                    )
                )
                
                if not lease_response.success:
                    self.logger.warning(f"Failed to get lease: {lease_response.message}")
                    time.sleep(0.1 * (2 ** attempt))
                    continue
                
                # === GFS STEP 2: Receive primary identity and replica locations ===
                primary_server = lease_response.primary_server
                all_locations = lease_response.locations
                chunk_version = lease_response.chunk_version  # ✅ Get version for validation
                self.logger.info(f"📝 LEASE GRANTED: Primary={primary_server}, Replicas={len(all_locations)}, Version={chunk_version}")
                
                # === GFS STEP 3: Push data to all replicas (via pipeline) ===
                # "The client pushes the data to all the replicas. A client can do so 
                # in any order... By decoupling the data flow from the control flow, 
                # we can improve performance by scheduling the expensive data flow 
                # based on the network topology."
                
                # --- Pipeline logic ---
                # Get client IP (best effort, fallback to localhost)
                import socket
                try:
                    client_ip = socket.gethostbyname(socket.gethostname())
                except Exception:
                    client_ip = '127.0.0.1'
                # Build pipeline order
                pipeline = self._build_pipeline([
                    {'server_id': loc.server_id, 'address': loc.address, 'port': loc.port}
                    for loc in all_locations
                ], client_ip)
                if not pipeline:
                    self.logger.error("No pipeline could be built for data push")
                    continue
                data_id = f"write_{chunk_handle}_{int(time.time() * 1000000)}"
                
                # Build the chain for pipelined push
                # Create a list of remaining servers for the pipeline
                remaining_chain = [f"{loc['address']}:{loc['port']}" for loc in pipeline[1:]]
                
                self.logger.info(f"📋 PIPELINE ORDER: {' → '.join([loc['server_id'] for loc in pipeline])}")
                self.logger.info(f"🚀 Starting pipelined data push with data_id: {data_id}")
                
                # Push data ONLY to the FIRST server in the pipeline
                # It will forward to the rest automatically
                first_server = pipeline[0]
                stub = self.get_chunk_server_stub(first_server['address'], first_server['port'])
                if not stub:
                    self.logger.error(f"Failed to connect to first chunkserver {first_server['address']}:{first_server['port']}")
                    continue
                
                # Determine next server in chain (second server in pipeline)
                next_server = ''
                if len(pipeline) > 1:
                    next_loc = pipeline[1]
                    next_server = f"{next_loc['address']}:{next_loc['port']}"
                
                self.logger.info(f"📤 Pushing {len(data)} bytes to {first_server['server_id']} ({first_server['address']}:{first_server['port']})")
                if next_server:
                    self.logger.info(f"   ⏭️  Next in chain: {next_server}")
                
                # GFS Step 3: Push data to all replicas via pipelined chain
                push_response = stub.PushData(
                    gfs_pb2.PushDataRequest(
                        data=data,
                        data_id=data_id,
                        next_server=next_server,
                        remaining_chain=remaining_chain  # Send full chain for visibility
                    ),
                    timeout=15.0  # Increased timeout since we're waiting for entire chain
                )
                
                if not push_response.success:
                    self.logger.error(f"❌ PushData failed: {push_response.message}")
                    continue
                
                # GFS Step 4: Wait for acknowledgment from ALL replicas
                # The pipeline_complete flag tells us if data reached all servers
                if not push_response.pipeline_complete:
                    self.logger.error(f"❌ Pipeline did not complete - not all replicas acknowledged")
                    continue
                
                self.logger.info(f"✅ ALL REPLICAS acknowledged receiving data (via pipeline)")
                self.logger.info(f"   Pipeline: {' → '.join([loc['server_id'] for loc in pipeline])}")
                self.logger.info(f"   ACK chain: {' ← '.join(reversed([loc['server_id'] for loc in pipeline]))}")
                
                # GFS Step 4: Send write request to primary
                # "Once all the replicas have acknowledged receiving the data,
                # the client sends a write request to the primary."
                # Find primary location
                primary_location = None
                for loc in pipeline:
                    if loc['server_id'] == primary_server:
                        primary_location = loc
                        break
                if not primary_location:
                    self.logger.error("Primary server not found in pipeline")
                    continue
                    
                # Build list of secondary locations (all except primary)
                secondary_locations = []
                for loc in all_locations:
                    if loc.server_id != primary_server:
                        secondary_locations.append(loc)
                
                primary_stub = self.get_chunk_server_stub(primary_location['address'], primary_location['port'])
                if not primary_stub:
                    self.logger.error(f"Failed to connect to primary server: {primary_server}")
                    continue
                    
                write_response = primary_stub.WriteChunk(
                    gfs_pb2.WriteChunkRequest(
                        chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                        offset=offset,
                        data=b"",  # Empty - data already cached
                        lease_id=data_id,
                        secondary_locations=secondary_locations,  # ✅ Send secondary locations to primary
                        chunk_version=chunk_version  # ✅ Send version for validation
                    ),
                    timeout=15.0
                )
                if write_response.success:
                    self.logger.info(f"✅ WRITE SUCCESS: Primary {primary_server} applied write using data_id: {data_id}")
                    new_size = offset + len(data)
                    try:
                        size_response = leader_stub.UpdateChunkSize(
                            gfs_pb2.UpdateChunkSizeRequest(
                                chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                                size=new_size
                            )
                        )
                        if size_response.success:
                            self.logger.info(f"✅ SIZE UPDATE: Chunk {chunk_handle} size updated to {new_size} bytes")
                            return True
                        else:
                            self.logger.warning(f"Write succeeded but size update failed: {size_response.message}")
                            return True
                    except Exception as e:
                        self.logger.warning(f"Failed to update chunk size: {e}")
                        return True
                else:
                    self.logger.error(f"❌ WRITE FAILED: Primary {primary_server} rejected write: {write_response.message}")
                    if "No cached data found" in write_response.message:
                        self.logger.warning(f"Cache miss detected, retrying attempt {attempt + 1}")
                        continue
            except Exception as e:
                self.logger.error(f"❌ Write attempt {attempt + 1} failed with exception: {e}")
                time.sleep(0.1 * (2 ** attempt))
        self.logger.error(f"❌ WRITE COMPLETELY FAILED after {max_retries} attempts")
        return False

    def _record_append_with_lease(self, chunk_handle: str, data: bytes, locations: List[dict], 
                                  filename: str, chunk_index: int) -> int:
        """Perform atomic record append following GFS protocol (Section 3.3)
        
        Same control flow as write (Section 3.1) with extra logic at primary:
        1. Request lease from master
        2. Master replies with primary and locations
        3. Client pushes data to ALL replicas (pipelined)
        4. Wait for all replicas to ACK
        5. Send append request to primary
        6. Primary checks if record fits:
           - If doesn't fit: pad chunk, tell secondaries to pad, return -2 (retry next chunk)
           - If fits: append at chosen offset, tell secondaries exact offset
        7. Primary replies with actual offset to client
        
        Returns:
            offset >= 0: Success, data appended at this offset
            -1: Failure, should retry
            -2: Chunk full, should retry with next chunk
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # === STEP 1: Request lease from master ===
                leader_stub = self.get_leader_stub()
                if not leader_stub:
                    self.logger.error("No leader available for lease request")
                    continue
                
                lease_response = leader_stub.RequestLease(
                    gfs_pb2.RequestLeaseRequest(
                        chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle)
                    )
                )
                
                if not lease_response.success:
                    self.logger.warning(f"Failed to get lease: {lease_response.message}")
                    time.sleep(0.1 * (2 ** attempt))
                    continue
                
                # === STEP 2: Receive primary and locations ===
                primary_server = lease_response.primary_server
                all_locations = lease_response.locations
                chunk_version = lease_response.chunk_version
                self.logger.info(f"📝 APPEND LEASE GRANTED: Primary={primary_server}, Version={chunk_version}")
                
                # === STEP 3: Push data to all replicas (same as write) ===
                import socket
                try:
                    client_ip = socket.gethostbyname(socket.gethostname())
                except Exception:
                    client_ip = '127.0.0.1'
                
                pipeline = self._build_pipeline([
                    {'server_id': loc.server_id, 'address': loc.address, 'port': loc.port}
                    for loc in all_locations
                ], client_ip)
                
                if not pipeline:
                    self.logger.error("No pipeline could be built")
                    continue
                
                data_id = f"append_{chunk_handle}_{int(time.time() * 1000000)}"
                remaining_chain = [f"{loc['address']}:{loc['port']}" for loc in pipeline[1:]]
                
                self.logger.info(f"📋 APPEND PIPELINE: {' → '.join([loc['server_id'] for loc in pipeline])}")
                
                # Push to first server
                first_server = pipeline[0]
                stub = self.get_chunk_server_stub(first_server['address'], first_server['port'])
                if not stub:
                    self.logger.error(f"Failed to connect to first server {first_server['address']}:{first_server['port']}")
                    continue
                
                next_server = ''
                if len(pipeline) > 1:
                    next_loc = pipeline[1]
                    next_server = f"{next_loc['address']}:{next_loc['port']}"
                
                self.logger.info(f"📤 Pushing append data ({len(data)} bytes) with data_id: {data_id}")
                
                push_response = stub.PushData(
                    gfs_pb2.PushDataRequest(
                        data=data,
                        data_id=data_id,
                        next_server=next_server,
                        remaining_chain=remaining_chain
                    ),
                    timeout=15.0
                )
                
                if not push_response.success:
                    self.logger.error(f"❌ PushData failed: {push_response.message}")
                    continue
                
                # === STEP 4: Wait for all replicas to ACK ===
                if not push_response.pipeline_complete:
                    self.logger.error(f"❌ Pipeline did not complete")
                    continue
                
                self.logger.info(f"✅ ALL REPLICAS cached append data")
                
                # === STEP 5: Send append request to primary ===
                primary_location = None
                for loc in pipeline:
                    if loc['server_id'] == primary_server:
                        primary_location = loc
                        break
                
                if not primary_location:
                    self.logger.error("Primary server not found in pipeline")
                    continue
                
                # Build secondary locations for primary
                secondary_locations = []
                for loc in all_locations:
                    if loc.server_id != primary_server:
                        secondary_locations.append(loc)
                
                primary_stub = self.get_chunk_server_stub(primary_location['address'], primary_location['port'])
                if not primary_stub:
                    self.logger.error(f"Failed to connect to primary: {primary_server}")
                    continue
                
                # Send AppendChunk request to primary
                # Build secondary_locations as strings (server addresses)
                secondary_addrs = [f"{loc.address}:{loc.port}" for loc in secondary_locations]
                
                append_response = primary_stub.AppendChunk(
                    gfs_pb2.AppendChunkRequest(
                        chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                        data=b"",  # Empty - data already cached
                        lease_id=data_id,  # Reference to cached data
                        data_id=data_id,  # For cached data lookup
                        version=chunk_version,  # Note: field is 'version', not 'chunk_version'
                        secondary_locations=secondary_addrs
                    ),
                    timeout=15.0
                )
                
                # === STEP 6 & 7: Primary processes and replies ===
                if append_response.success:
                    append_offset = append_response.offset
                    self.logger.info(f"✅ RECORD APPEND SUCCESS: offset={append_offset}")
                    
                    # Update master with new chunk size
                    new_size = append_offset + len(data)
                    try:
                        size_response = leader_stub.UpdateChunkSize(
                            gfs_pb2.UpdateChunkSizeRequest(
                                chunk_handle=gfs_pb2.ChunkHandle(handle=chunk_handle),
                                size=new_size
                            )
                        )
                        if size_response.success:
                            self.logger.info(f"✅ Chunk size updated to {new_size}")
                    except Exception as e:
                        self.logger.warning(f"Failed to update chunk size: {e}")
                    
                    return append_offset
                else:
                    # Check if chunk is full (need to retry with next chunk)
                    if "full" in append_response.message.lower() or "exceed" in append_response.message.lower():
                        self.logger.info(f"⚠️  Chunk full, need next chunk: {append_response.message}")
                        return -2  # Special code for chunk full
                    else:
                        self.logger.error(f"❌ APPEND FAILED: {append_response.message}")
                        return -1
                        
            except Exception as e:
                self.logger.error(f"❌ Append attempt {attempt + 1} failed: {e}")
                time.sleep(0.1 * (2 ** attempt))
        
        return -1  # All retries failed


# Backward compatibility - alias for existing code
GFSClient = MultiMasterGFSClient

def print_help():
    """Print available commands"""
    print("\n=== Available Commands ===")
    print("help, h                     - Show this help message")
    print("status                      - Show cluster status")
    print("create <filename>           - Create a new file")
    print("delete <filename>           - Delete a file")
    print("info <filename>             - Get file information")
    print("read <filename> [offset] [length] - Read data from file")
    print("write <filename> <data>     - Write data to file (overwrites)")
    print("append <filename> <data>    - Append data to file")
    print("chunks <chunk_handle>       - Get chunk locations")
    print("")
    print("=== Directory Commands ===")
    print("mkdir <directory>           - Create a new directory")
    print("rmdir <directory> [-r]      - Delete a directory (use -r for recursive)")
    print("ls [directory]              - List directory contents")
    print("ls -l [directory]           - List directory contents with details")
    print("find, list-all              - List all files and directories in the filesystem")
    print("pwd                         - Show current working directory")
    print("cd <directory>              - Change current directory")
    print("")
    print("clear                       - Clear screen")
    print("quit, exit, q               - Exit the client")
    print("========================\n")

def parse_command(command_line):
    """Parse command line input into command and arguments"""
    parts = command_line.strip().split()
    if not parts:
        return None, []
    
    command = parts[0].lower()
    args = parts[1:]
    return command, args

def format_file_size(size_bytes):
    """Format file size in human readable format"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes/(1024**2):.1f} MB"
    else:
        return f"{size_bytes/(1024**3):.1f} GB"

def main():
    """Interactive GFS client"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='GFS Multi-Master Interactive Client')
    parser.add_argument('masters', nargs='+', 
                       help='Master addresses in format ip:port (space separated). Required.')
    
    try:
        args = parser.parse_args()
    except SystemExit:
        print("Error: Master addresses are required!")
        print("Usage: python client.py <master1_ip:port> <master2_ip:port> ...")
        print("Example: python client.py localhost:9000 localhost:9001 localhost:9002")
        sys.exit(1)
    
    master_addresses = args.masters
    print(f"Connecting to GFS masters: {master_addresses}")
    
    # Initialize client
    try:
        client = MultiMasterGFSClient(master_addresses)
        print("✅ Successfully connected to GFS cluster")
    except Exception as e:
        print(f"❌ Failed to connect to GFS cluster: {e}")
        sys.exit(1)
    
    # Show initial cluster status
    print("\n=== Initial Cluster Status ===")
    status = client.get_cluster_status()
    if status:
        primary = status['primary_master']
        # Show client-side address if available, otherwise show internal address
        display_address = primary.get('client_address', f"{primary['address']}:{primary['port']}")
        print(f"Primary Master: {primary['master_id']} at {display_address}")
        print(f"Total Masters: {len(status['all_masters'])}")
    else:
        print("Warning: Could not retrieve cluster status")
    
    print_help()
    
    # Interactive command loop
    while True:
        try:
            command_line = input("gfs> ").strip()
            if not command_line:
                continue
            
            command, args = parse_command(command_line)
            if not command:
                continue
            
            # Handle commands
            if command in ['help', 'h']:
                print_help()
            
            elif command == 'status':
                print("\n=== Cluster Status ===")
                status = client.get_cluster_status()
                if status:
                    primary = status['primary_master']
                    # Show client-side address if available
                    primary_display = primary.get('client_address', f"{primary['address']}:{primary['port']}")
                    print(f"Primary Master: {primary['master_id']} at {primary_display} (term: {primary['term']})")
                    print(f"\nAll Masters:")
                    for master in status['all_masters']:
                        role = "PRIMARY" if master['is_primary'] else "BACKUP"
                        # Show client-side address if available
                        master_display = master.get('client_address', f"{master['address']}:{master['port']}")
                        if master_display == 'unknown':
                            master_display = f"{master['address']}:{master['port']} (internal)"
                        print(f"  - {master['master_id']} ({role}) at {master_display} (term: {master['term']})")
                else:
                    print("❌ Could not retrieve cluster status")
            
            elif command == 'create':
                if len(args) != 1:
                    print("Usage: create <filename>")
                    continue
                
                filename = args[0]
                print(f"Creating file: {filename}")
                if client.create_file(filename):
                    print(f"✅ File '{filename}' created successfully")
                else:
                    print(f"❌ Failed to create file '{filename}'")
            
            elif command == 'delete':
                if len(args) != 1:
                    print("Usage: delete <filename>")
                    continue
                
                filename = args[0]
                confirm = input(f"Are you sure you want to delete '{filename}'? (y/N): ")
                if confirm.lower() in ['y', 'yes']:
                    print(f"Deleting file: {filename}")
                    if client.delete_file(filename):
                        print(f"✅ File '{filename}' deleted successfully")
                    else:
                        print(f"❌ Failed to delete file '{filename}'")
                else:
                    print("Delete cancelled")
            
            elif command == 'info':
                if len(args) != 1:
                    print("Usage: info <filename>")
                    continue
                
                filename = args[0]
                print(f"Getting info for file: {filename}")
                file_info = client.get_file_info(filename)
                if file_info:
                    print(f"✅ File Information:")
                    print(f"   Filename: {file_info['filename']}")
                    print(f"   Size: {format_file_size(file_info['size'])} ({file_info['size']} bytes)")
                    print(f"   Chunk Size: {format_file_size(file_info['chunk_size'])}")
                    print(f"   Number of Chunks: {len(file_info['chunks'])}")
                    if file_info['chunks']:
                        print(f"   Chunk Handles:")
                        for i, chunk in enumerate(file_info['chunks']):
                            if chunk:
                                print(f"     [{i}]: {chunk}")
                            else:
                                print(f"     [{i}]: <sparse/empty>")
                else:
                    print(f"❌ File '{filename}' not found or error retrieving info")
            
            elif command == 'read':
                if len(args) < 1 or len(args) > 3:
                    print("Usage: read <filename> [offset] [length]")
                    continue
                
                filename = args[0]
                offset = int(args[1]) if len(args) > 1 else 0
                length = int(args[2]) if len(args) > 2 else None
                
                print(f"Reading file: {filename}")
                if offset > 0 or length is not None:
                    print(f"  Offset: {offset}, Length: {length if length else 'full file'}")
                
                data = client.read_file(filename, offset, length)
                if data is not None:
                    print(f"✅ Read {len(data)} bytes:")
                    try:
                        # Try to decode as text
                        text = data.decode('utf-8')
                        print(f"   Content: {repr(text)}")
                        if len(text) <= 200:
                            print(f"   Text: {text}")
                        else:
                            print(f"   Text (first 200 chars): {text[:200]}...")
                    except UnicodeDecodeError:
                        print(f"   Binary data: {data[:100]}{'...' if len(data) > 100 else ''}")
                else:
                    print(f"❌ Failed to read file '{filename}'")
            
            elif command == 'write':
                if len(args) < 2:
                    print("Usage: write <filename> <data>")
                    print("Note: Use quotes for data with spaces: write file.txt \"hello world\"")
                    continue
                
                filename = args[0]
                data = ' '.join(args[1:]).encode('utf-8')
                
                print(f"Writing {len(data)} bytes to file: {filename}")
                if client.write_file(filename, data):
                    print(f"✅ Data written successfully to '{filename}'")
                else:
                    print(f"❌ Failed to write data to '{filename}'")
            
            elif command == 'append':
                if len(args) < 2:
                    print("Usage: append <filename> <data>")
                    print("Note: Use quotes for data with spaces: append file.txt \"hello world\"")
                    continue
                
                filename = args[0]
                data = ' '.join(args[1:]).encode('utf-8')
                
                print(f"Appending {len(data)} bytes to file: {filename}")
                if client.append_file(filename, data):
                    print(f"✅ Data appended successfully to '{filename}'")
                else:
                    print(f"❌ Failed to append data to '{filename}'")
            
            elif command == 'chunks':
                if len(args) != 1:
                    print("Usage: chunks <chunk_handle>")
                    continue
                
                chunk_handle = args[0]
                print(f"Getting locations for chunk: {chunk_handle}")
                locations = client.get_chunk_locations(chunk_handle)
                if locations:
                    print(f"✅ Chunk locations ({len(locations)} replicas):")
                    for i, location in enumerate(locations):
                        print(f"   [{i+1}] Server: {location['server_id']} at {location['address']}:{location['port']}")
                else:
                    print(f"❌ No locations found for chunk '{chunk_handle}' or chunk doesn't exist")
            
            # Directory Commands
            elif command == 'mkdir':
                if len(args) != 1:
                    print("Usage: mkdir <directory>")
                    continue
                
                directory = args[0]
                print(f"Creating directory: {directory}")
                if client.create_directory(directory):
                    print(f"✅ Directory '{directory}' created successfully")
                else:
                    print(f"❌ Failed to create directory '{directory}'")
            
            elif command == 'rmdir':
                if len(args) < 1 or len(args) > 2:
                    print("Usage: rmdir <directory> [-r]")
                    continue
                
                directory = args[0]
                recursive = len(args) > 1 and args[1] == '-r'
                
                if recursive:
                    confirm = input(f"Are you sure you want to recursively delete '{directory}' and all its contents? (y/N): ")
                    if confirm.lower() not in ['y', 'yes']:
                        print("Delete cancelled")
                        continue
                else:
                    confirm = input(f"Are you sure you want to delete directory '{directory}'? (y/N): ")
                    if confirm.lower() not in ['y', 'yes']:
                        print("Delete cancelled")
                        continue
                
                print(f"Deleting directory: {directory} ({'recursive' if recursive else 'non-recursive'})")
                if client.delete_directory(directory, recursive):
                    print(f"✅ Directory '{directory}' deleted successfully")
                else:
                    print(f"❌ Failed to delete directory '{directory}'")
            
            elif command == 'ls':
                directory = '/'
                show_details = False
                
                # Parse arguments
                if len(args) == 1:
                    if args[0] == '-l':
                        show_details = True
                    else:
                        directory = args[0]
                elif len(args) == 2:
                    if args[0] == '-l':
                        show_details = True
                        directory = args[1]
                    elif args[1] == '-l':
                        directory = args[0]
                        show_details = True
                    else:
                        print("Usage: ls [directory] [-l]")
                        continue
                elif len(args) > 2:
                    print("Usage: ls [directory] [-l]")
                    continue
                
                print(f"Listing directory: {directory}")
                entries = client.list_directory(directory)
                if entries is not None:
                    if not entries:
                        print("📁 Directory is empty")
                    else:
                        print(f"✅ Directory contents ({len(entries)} items):")
                        # Sort entries: directories first, then files
                        sorted_entries = sorted(entries.items(), key=lambda x: (x[1] != 'directory', x[0]))
                        
                        for name, entry_type in sorted_entries:
                            if show_details:
                                icon = "📁" if entry_type == 'directory' else "📄"
                                print(f"   {icon} {name:<30} ({entry_type})")
                            else:
                                icon = "📁" if entry_type == 'directory' else "📄"
                                print(f"   {icon} {name}")
                else:
                    print(f"❌ Failed to list directory '{directory}' (may not exist)")
            
            elif command in ['find', 'list-all']:
                print("📋 Listing all files and directories in the filesystem...")
                all_items = client.list_all_files()
                if all_items is not None:
                    if not all_items:
                        print("📁 Filesystem is empty")
                    else:
                        print(f"✅ Found {len(all_items)} items in filesystem:")
                        
                        # Separate files and directories for better display
                        files = {}
                        directories = {}
                        
                        for path, info in all_items.items():
                            if info['type'] == 'directory':
                                directories[path] = info
                            else:
                                files[path] = info
                        
                        # Display directories first
                        if directories:
                            print(f"\n📁 Directories ({len(directories)}):")
                            for path in sorted(directories.keys()):
                                info = directories[path]
                                print(f"   📁 {path}")
                        
                        # Display files
                        if files:
                            print(f"\n📄 Files ({len(files)}):")
                            for path in sorted(files.keys()):
                                info = files[path]
                                size_str = format_file_size(info['size'])
                                print(f"   📄 {path:<50} ({size_str})")
                else:
                    print("❌ Failed to list filesystem contents")
            
            elif command == 'pwd':
                # For now, we don't maintain current directory state in the client
                # This could be enhanced to track the current working directory
                print("📍 Current directory: / (root)")
                print("Note: Current directory tracking not yet implemented")
            
            elif command == 'cd':
                # For now, we don't maintain current directory state in the client
                # This could be enhanced to change the current working directory
                if len(args) != 1:
                    print("Usage: cd <directory>")
                    continue
                
                directory = args[0]
                print(f"📍 Change directory requested: {directory}")
                print("Note: Directory navigation not yet implemented in this client")
                print("      You can use absolute paths in all commands for now")
            
            elif command == 'clear':
                import os
                os.system('clear' if os.name == 'posix' else 'cls')
            
            elif command in ['quit', 'exit', 'q']:
                print("Goodbye!")
                break
            
            else:
                print(f"❌ Unknown command: {command}")
                print("Type 'help' for available commands")
        
        except KeyboardInterrupt:
            print("\nUse 'quit' or 'exit' to leave the client")
        except EOFError:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                traceback.print_exc()

if __name__ == '__main__':
    main()
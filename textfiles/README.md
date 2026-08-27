# Google File System (GFS) Implementation with Multi-Master Support

A complete Python implementation of the Google File System based on the original research paper, now enhanced with **multi-master support for high availability**. Features Docker deployment, leader election, automatic failover, and distributed consensus.

## 🚀 New Multi-Master Architecture

This enhanced implementation extends the original GFS with **fault-tolerant master nodes** using a Raft-inspired consensus algorithm:

### Multi-Master Features
- **🏛️ Leader Election**: Automatic leader selection among master nodes
- **🔄 Automatic Failover**: Seamless failover when the leader fails  
- **📊 Distributed Consensus**: Raft-like algorithm for consistency
- **🔄 Metadata Synchronization**: Real-time sync between masters
- **🎯 Client Auto-Discovery**: Clients automatically find the current leader
- **⚡ Zero-Downtime**: Operations continue during master failures

## Architecture Overview

### Master Cluster (`master.py`)
- **Leader Election**: Uses randomized timeouts and majority voting
- **Distributed Metadata**: Leader synchronizes metadata to backup masters
- **Term-based Consensus**: Prevents split-brain scenarios
- **Heartbeat Monitoring**: Maintains cluster health and leadership
- **State Machine**: FOLLOWER → CANDIDATE → LEADER transitions
- **Automatic Recovery**: Failed masters rejoin cluster automatically

### Enhanced Client (`client.py`)
- **Multi-Master Awareness**: Connects to multiple master addresses
- **Leader Discovery**: Automatically finds current leader
- **Transparent Failover**: Retries operations with new leader
- **Connection Pooling**: Maintains connections to all masters
- **Cluster Status**: Real-time cluster health monitoring

### Chunk Servers (`chunk_server.py`)
- **Master Resilience**: Continues operation during master changes
- **Adaptive Heartbeats**: Reports to current leader
- **Unchanged Interface**: No modifications needed for multi-master

## Key Features

### From the GFS Paper
- **Large file support**: Files can span multiple 64MB chunks
- **High availability**: 3-way replication of all chunks by default
- **Consistency model**: Supports concurrent writes with defined consistency semantics
- **Scalability**: Master server tracks metadata; chunk servers handle data
- **Fault tolerance**: Handles chunk server failures gracefully
- **Lease-based consistency**: Uses leases to maintain write ordering

### Implementation-Specific
- **gRPC communication**: Modern RPC framework for all inter-component communication
- **Docker deployment**: Complete containerization for easy deployment
- **Persistent metadata**: Master metadata survives restarts
- **Configurable replication**: Adjustable replication factor (default: 3)
- **Health monitoring**: Built-in health checks and monitoring
- **Multi-master support**: Leader election, failover, and distributed consensus

## File Structure

```
gfs/
├── master.py              # Multi-master server with leader election
├── chunk_server.py        # Chunk server (unchanged)
├── client.py              # Multi-master aware client
├── gfs.proto              # Extended protobuf (election messages)
├── gfs_pb2.py             # Generated protobuf code
├── gfs_pb2_grpc.py        # Generated gRPC code
├── requirements.txt       # Dependencies
├── Dockerfile             # Container definition
├── docker-compose.yml     # Multi-master orchestration
├── start_gfs.sh           # Enhanced startup script
└── README.md              # This documentation
```

## Quick Start - Multi-Master Mode

### 1. Start Multi-Master Cluster (3 Masters + 3 Chunk Servers)

```bash
# Start the complete multi-master cluster
./start_gfs.sh start

# Check cluster status and current leader
./start_gfs.sh status
```

### 2. Test Multi-Master Operations

```bash
# Run comprehensive multi-master tests
./start_gfs.sh test

# Test leader failover scenarios
./start_gfs.sh failover-test
```

### 3. Interactive Multi-Master Client

```bash
# Run basic demo
./start_gfs.sh demo

# Or use interactive shell
docker-compose run --rm gfs-client bash

# Multi-master client usage
python -c "
from client import MultiMasterGFSClient
client = MultiMasterGFSClient([
    'gfs-master-1:9000',
    'gfs-master-2:9000', 
    'gfs-master-3:9000'
])

# Show cluster status
status = client.get_cluster_status()
print(f'Leader: {status[\"primary_master\"][\"master_id\"]}')

# File operations work transparently
client.create_file('test.txt')
client.write_file('test.txt', b'Multi-master GFS!')
print(client.read_file('test.txt').decode())
"
```

## Manual Installation and Setup

### Prerequisites
- Python 3.8+
- pip

### Installation

```bash
# Clone or extract the GFS implementation
cd gfs

# Install dependencies
pip install -r requirements.txt

# Generate gRPC code (if not present)
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. gfs.proto
```

### Running Components

#### Start Master Servers
```bash
# Terminal 1
python master.py --master-id master1 --port 9000 --peers master2:9010 master3:9020

# Terminal 2
python master.py --master-id master2 --port 9010 --peers master1:9000 master3:9020

# Terminal 3
python master.py --master-id master3 --port 9020 --peers master1:9000 master2:9010
```

#### Start Chunk Servers
```bash
# Terminal 1
python chunk_server.py --server-id cs1 --port 9001 --master localhost:9000

# Terminal 2  
python chunk_server.py --server-id cs2 --port 9002 --master localhost:9000

# Terminal 3
python chunk_server.py --server-id cs3 --port 9003 --master localhost:9000
```

#### Use Client
```bash
python client.py
```

## API Usage

### Basic Multi-Master Client

```python
from client import MultiMasterGFSClient

# Connect to master cluster
masters = ['master1:9000', 'master2:9000', 'master3:9000']
client = MultiMasterGFSClient(masters)

# Operations are automatically routed to current leader
client.create_file('distributed_file.txt')
client.write_file('distributed_file.txt', b'Fault-tolerant storage!')

# Read operations can use any master
data = client.read_file('distributed_file.txt')
```

### Cluster Monitoring

```python
# Get real-time cluster status
status = client.get_cluster_status()

print(f"Current Leader: {status['primary_master']['master_id']}")
print(f"Term: {status['primary_master']['term']}")

for master in status['all_masters']:
    role = "PRIMARY" if master['is_primary'] else "BACKUP"
    print(f"{master['master_id']}: {role} (Term {master['term']})")
```

### Handling Failover in Applications

```python
def robust_write(client, filename, data):
    """Write with automatic retry on failover"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return client.write_file(filename, data)
        except Exception as e:
            if "Not the leader" in str(e) and attempt < max_retries - 1:
                print(f"Leader changed, retrying... ({attempt + 1}/{max_retries})")
                time.sleep(1)  # Brief pause for election
                continue
            raise
```

## Configuration

### Master Cluster Ports
- **Master 1**: `localhost:9000` (Primary election candidate)
- **Master 2**: `localhost:9010` (Backup)  
- **Master 3**: `localhost:9020` (Backup)
- **Chunk Servers**: `localhost:9001-9003` (Unchanged)

### Consensus Parameters
```python
ELECTION_TIMEOUT = 150-300ms  # Randomized to prevent split votes
MASTER_HEARTBEAT = 50ms       # Leader heartbeat interval  
LEASE_TIMEOUT = 60s           # Write lease duration
TERM_PERSISTENCE = True       # Terms survive restarts
```

### System Constants
- **Chunk size**: 64MB (configurable in code)
- **Replication factor**: 3 (configurable in code)
- **Heartbeat interval**: 30 seconds
- **Lease timeout**: 60 seconds

## Architecture Details

### Leader Election Process

1. **Initialization**: All masters start as FOLLOWERS
2. **Election Timeout**: Random timeout prevents split votes
3. **Candidate Phase**: Request votes from peer masters
4. **Majority Rule**: Requires >50% votes to become leader
5. **Leadership**: Winner sends heartbeats to maintain authority
6. **Failover**: New election triggered if leader fails

### Consistency Guarantees

- **Strong Consistency**: Only leader accepts writes
- **Metadata Sync**: Leader replicates to all followers
- **Version Control**: Operations have monotonic version numbers
- **Split-Brain Prevention**: Term-based consensus prevents conflicts
- **Durability**: Metadata persisted on all masters

### Fault Tolerance Capabilities

| Scenario | Behavior | Recovery Time |
|----------|----------|---------------|
| 1 Master Fails | Automatic failover | 150-300ms |
| 2 Masters Fail | System remains operational | N/A |  
| All Masters Fail | System unavailable | Manual restart |
| Network Partition | Majority partition remains active | 150-300ms |
| Master Restart | Rejoin cluster automatically | 5-10s |

### Performance Characteristics
- **Read throughput**: Scales with number of chunk servers
- **Write throughput**: Limited by network and disk I/O
- **Metadata operations**: Single master bottleneck (as designed)

## Monitoring and Debugging

### Cluster Health Commands

```bash
# Real-time cluster status
./start_gfs.sh status

# View master logs
./start_gfs.sh logs

# Monitor election events
docker-compose logs -f gfs-master-1 | grep -i election

# Check consensus state
docker-compose exec gfs-master-1 python -c "
from master import MultiMasterGFS
# Master internal state inspection
"
```

### Debug Commands
```python
from client import MultiMasterGFSClient
client = MultiMasterGFSClient(['master1:9000', 'master2:9000', 'master3:9000'])

# List file info
info = client.get_file_info('myfile.txt')
print(f"Chunks: {info['chunks']}")

# Check chunk locations
for chunk in info['chunks']:
    if chunk:
        locations = client.get_chunk_locations(chunk)
        print(f"Chunk {chunk}: {len(locations)} replicas")
```

### Data Persistence
- **Master metadata**: Stored in `master_metadata.json`
- **Chunk data**: Stored in `chunks/` directory per server
- **Docker volumes**: Persist data across container restarts

## Limitations and Future Work

### Current Limitations
- **No snapshots**: Snapshot functionality not implemented
- **Basic security**: No authentication or encryption
- **Limited record append**: Simplified implementation
- **No client caching**: No metadata caching in client

### Potential Enhancements
- Master replication with shadow masters
- Client-side metadata caching
- Chunk checksumming and repair
- Snapshot and backup functionality
- Security and access control
- Performance monitoring and metrics
- Web UI for system monitoring

## Comparison to Original GFS

This implementation captures the core concepts from the Google File System paper:

### Implemented Features ✅
- Single master architecture
- Large file support with chunking  
- Chunk replication (configurable factor)
- Lease-based write consistency
- Heartbeat-based failure detection
- Client library for file operations
- Metadata persistence
- Multi-master support with leader election (Raft Algo) and failover

### Simplified or Missing ✅/❌
- ✅ Record append (simplified)
- ❌ Snapshot functionality  
- ❌ Sophisticated garbage collection
- ❌ Client metadata caching
- ❌ Chunk migration/rebalancing

## Testing

### Basic Functionality Test
```bash
# Run the built-in demo
docker-compose --profile demo run --rm gfs-client

# Or manually test
python client.py
```

### Fault Tolerance Test  
```bash
# Stop a chunk server
docker-compose stop gfs-chunkserver-1

# Verify reads still work (from other replicas)
docker-compose run --rm gfs-client python -c "
from client import MultiMasterGFSClient
client = MultiMasterGFSClient(['master1:9000', 'master2:9000', 'master3:9000'])
print(client.read_file('test_file.txt'))
"

# Restart the chunk server
docker-compose start gfs-chunkserver-1
```

## Contributing

This implementation serves as an educational example of the GFS architecture. Areas for contribution:

1. **Performance improvements**: Optimize chunk allocation algorithms
2. **Feature additions**: Implement missing GFS features
3. **Testing**: Add comprehensive test suites
4. **Documentation**: Improve code documentation
5. **Monitoring**: Add metrics and monitoring capabilities

## References

- [The Google File System (2003)](https://research.google/pubs/pub51/) - Original GFS paper
- [Raft Consensus Algorithm](https://raft.github.io/) - Consensus inspiration
- [gRPC Documentation](https://grpc.io/docs/) - RPC framework
- [Docker Compose](https://docs.docker.com/compose/) - Container orchestration

---

This implementation demonstrates the core principles of distributed file systems as described in the seminal GFS paper, providing a foundation for understanding large-scale storage systems.
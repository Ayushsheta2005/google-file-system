# Production Deployment Guide

This guide shows how to deploy the multi-master GFS across multiple machines using just Docker (no docker-compose needed).

## Build and Distribution

### 1. Build the Image
```bash
# Build the GFS image
docker build -t gfs-system:latest .

# Tag for registry (optional)
docker tag gfs-system:latest your-registry.com/gfs-system:latest

# Push to registry (optional)
docker push your-registry.com/gfs-system:latest
```

### 2. Distribute to Machines
```bash
# Save image to tar file
docker save gfs-system:latest > gfs-system.tar

# Copy to other machines and load
scp gfs-system.tar user@machine2:/tmp/
ssh user@machine2 "docker load < /tmp/gfs-system.tar"
```

## Multi-Machine Deployment

### Master Nodes (3 machines for HA)

**Machine 1 (10.0.1.10):**
```bash
docker run -d \
  --name gfs-master-1 \
  --restart unless-stopped \
  -p 9000:9000 \
  -v /data/gfs/master1:/app/metadata \
  --network host \
  gfs-system python master.py \
    --master-id master-1 \
    --port 9000 \
    --peers 10.0.1.11:9000 10.0.1.12:9000
```

**Machine 2 (10.0.1.11):**
```bash
docker run -d \
  --name gfs-master-2 \
  --restart unless-stopped \
  -p 9000:9000 \
  -v /data/gfs/master2:/app/metadata \
  --network host \
  gfs-system python master.py \
    --master-id master-2 \
    --port 9000 \
    --peers 10.0.1.10:9000 10.0.1.12:9000
```

**Machine 3 (10.0.1.12):**
```bash
docker run -d \
  --name gfs-master-3 \
  --restart unless-stopped \
  -p 9000:9000 \
  -v /data/gfs/master3:/app/metadata \
  --network host \
  gfs-system python master.py \
    --master-id master-3 \
    --port 9000 \
    --peers 10.0.1.10:9000 10.0.1.11:9000
```

### Chunk Servers (Multiple machines)

**Chunk Server Machine 1:**
```bash
docker run -d \
  --name gfs-chunkserver-1 \
  --restart unless-stopped \
  -p 9001:9001 \
  -v /data/gfs/chunks1:/app/chunks \
  --network host \
  gfs-system python chunk_server.py \
    --server-id chunkserver-1 \
    --port 9001 \
    --master 10.0.1.10:9000
```

**Chunk Server Machine 2:**
```bash
docker run -d \
  --name gfs-chunkserver-2 \
  --restart unless-stopped \
  -p 9001:9001 \
  -v /data/gfs/chunks2:/app/chunks \
  --network host \
  gfs-system python chunk_server.py \
    --server-id chunkserver-2 \
    --port 9001 \
    --master 10.0.1.10:9000
```

### Client Usage (Any machine)

```bash
# Interactive client
docker run -it --rm \
  --network host \
  gfs-system python -c "
from client import MultiMasterGFSClient
client = MultiMasterGFSClient(['10.0.1.10:9000', '10.0.1.11:9000', '10.0.1.12:9000'])
status = client.get_cluster_status()
print(f'Leader: {status[\"primary_master\"][\"master_id\"]}')
client.create_file('production-test.txt')
client.write_file('production-test.txt', b'Production GFS deployment!')
print(client.read_file('production-test.txt').decode())
"

# Application container
docker run -d \
  --name my-gfs-app \
  --network host \
  gfs-system python my_application.py
```

## Management Commands

### Check Status
```bash
# Check master status
docker exec gfs-master-1 python -c "
from client import MultiMasterGFSClient
client = MultiMasterGFSClient(['10.0.1.10:9000', '10.0.1.11:9000', '10.0.1.12:9000'])
status = client.get_cluster_status()
for master in status['all_masters']:
    role = 'PRIMARY' if master['is_primary'] else 'BACKUP'
    print(f'{master[\"master_id\"]}: {role}')
"

# Check logs
docker logs gfs-master-1
docker logs gfs-chunkserver-1
```

### Scaling
```bash
# Add new chunk server on new machine
docker run -d \
  --name gfs-chunkserver-N \
  --restart unless-stopped \
  -p 9001:9001 \
  -v /data/gfs/chunksN:/app/chunks \
  --network host \
  gfs-system python chunk_server.py \
    --server-id chunkserver-N \
    --port 9001 \
    --master 10.0.1.10:9000
```

### Maintenance
```bash
# Stop services
docker stop gfs-master-1 gfs-chunkserver-1

# Update image
docker pull your-registry.com/gfs-system:latest

# Restart with new image
docker run -d --name gfs-master-1 ... # same parameters as before
```

## Networking Options

### Option 1: Host Network (Simplest)
```bash
--network host
# Containers use host machine's network directly
```

### Option 2: Bridge Network with Port Mapping
```bash
-p 9000:9000  # Map host:container ports
```

### Option 3: Custom Network (Advanced)
```bash
# Create custom network
docker network create --driver bridge gfs-net

# Use custom network
--network gfs-net
```

## Production Considerations

### Security
```bash
# Run as non-root user
--user 1000:1000

# Read-only filesystem
--read-only --tmpfs /tmp

# Limited resources
--memory 2g --cpus 2
```

### Monitoring
```bash
# Health checks
--health-cmd "python -c 'import socket; socket.create_connection((\"localhost\", 9000))'" \
--health-interval 30s \
--health-timeout 10s \
--health-retries 3
```

### Data Persistence
```bash
# Important: Mount persistent volumes
-v /data/gfs/master1:/app/metadata    # Master metadata
-v /data/gfs/chunks1:/app/chunks      # Chunk data
```

## Advantages of This Approach

1. **True Distribution**: Each service runs on separate machines
2. **Resource Isolation**: Each machine dedicated to specific role
3. **Network Flexibility**: Direct machine-to-machine communication
4. **Scaling**: Easy to add new chunk servers
5. **Production Ready**: No docker-compose dependency

## When to Use Docker Compose vs Direct Docker

**Use Docker Compose for:**
- Local development and testing
- Single-machine demos
- Integration testing
- Learning and experimentation

**Use Direct Docker for:**
- Production deployments
- Multi-machine clusters
- Serious performance testing
- Real distributed systems

Your approach with just the Dockerfile is definitely the right way for production!
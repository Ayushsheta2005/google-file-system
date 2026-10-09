#!/usr/bin/env python3
"""Shared tunables for the GFS cluster.

Every knob here can be overridden from the environment, which is how the
benchmark suite A/B tests a change without needing a separate build: run the
same cluster twice with e.g. GFS_CLIENT_CACHE=off and GFS_CLIENT_CACHE=lru and
compare. See docs/adr/ for why each default is what it is.
"""

import os


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default) or default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ('1', 'true', 'yes', 'on')


# --------------------------------------------------------------------------
# Layout
# --------------------------------------------------------------------------
# The paper's 64 MB chunk. Large enough that a 1 GB file is 16 chunks of
# metadata rather than 256k blocks, which is the whole reason GFS can keep its
# namespace in one master's memory.
CHUNK_SIZE = _env_int('GFS_CHUNK_SIZE', 64 * 1024 * 1024)


# --------------------------------------------------------------------------
# gRPC transport
# --------------------------------------------------------------------------
# A 64 MB chunk plus protobuf framing blows straight through gRPC's 4 MB
# default message cap, so both ends have to be raised. See ADR-009.
MAX_MESSAGE_BYTES = _env_int('GFS_MAX_MESSAGE_BYTES', 128 * 1024 * 1024)

# The original code pinned both servers to a 10-thread pool, which capped the
# whole cluster at 10 in-flight RPCs. See ADR-010.
DEFAULT_MAX_WORKERS = _env_int('GFS_MAX_WORKERS', 64)

KEEPALIVE_TIME_MS = _env_int('GFS_KEEPALIVE_TIME_MS', 20000)

# Client and server keepalive settings have to agree or the server treats the
# client as abusive: it answers with GOAWAY/ENHANCE_YOUR_CALM ("too_many_pings")
# and tears the connection down. The server's tolerance window must therefore
# be no larger than the interval the client actually pings at.
GRPC_SERVER_OPTIONS = [
    ('grpc.max_send_message_length', MAX_MESSAGE_BYTES),
    ('grpc.max_receive_message_length', MAX_MESSAGE_BYTES),
    ('grpc.so_reuseport', 0),
    ('grpc.keepalive_time_ms', KEEPALIVE_TIME_MS),
    ('grpc.keepalive_timeout_ms', 10000),
    ('grpc.keepalive_permit_without_calls', 1),
    ('grpc.http2.max_pings_without_data', 0),
    # Accept pings as often as the client sends them, idle or not.
    ('grpc.http2.min_ping_interval_without_data_ms', KEEPALIVE_TIME_MS // 2),
    ('grpc.http2.min_time_between_pings_ms', KEEPALIVE_TIME_MS // 2),
]

GRPC_CHANNEL_OPTIONS = [
    ('grpc.max_send_message_length', MAX_MESSAGE_BYTES),
    ('grpc.max_receive_message_length', MAX_MESSAGE_BYTES),
    ('grpc.keepalive_time_ms', KEEPALIVE_TIME_MS),
    ('grpc.keepalive_timeout_ms', 10000),
    ('grpc.keepalive_permit_without_calls', 1),
    ('grpc.http2.max_pings_without_data', 0),
]


# --------------------------------------------------------------------------
# Durability (Phase 0.1-0.3)
# --------------------------------------------------------------------------
# 'oplog'    - fsync an operation log record before replying to the client
# 'snapshot' - the original behaviour: dump all metadata as JSON every 30s
OPLOG_MODE = _env_str('GFS_OPLOG_MODE', 'oplog')

# Window in milliseconds over which concurrent mutations are coalesced into a
# single fsync. 0 disables group commit (one fsync per mutation).
#
# Defaults to 0 because measurement says so: the master calls commit() while
# holding metadata_lock, so mutations are already serialized and no batch
# ever forms -- the window becomes pure added latency, capping throughput at
# 1/window. Measured 124 ops/s at 5ms versus 377 ops/s at 0. The machinery is
# correct and stays, but it cannot pay off until commit() moves outside the
# global lock. See ADR-002.
OPLOG_GROUP_COMMIT_MS = _env_float('GFS_OPLOG_GROUP_COMMIT_MS', 0.0)

# Roll a checkpoint once the log has this many records since the last one.
CHECKPOINT_EVERY_OPS = _env_int('GFS_CHECKPOINT_EVERY_OPS', 5000)

# Keep this many old checkpoints on disk for forensics.
CHECKPOINT_KEEP = _env_int('GFS_CHECKPOINT_KEEP', 2)


# --------------------------------------------------------------------------
# Client metadata cache (Phase 0.6)
# --------------------------------------------------------------------------
# 'lru' or 'off'
CLIENT_CACHE_MODE = _env_str('GFS_CLIENT_CACHE', 'lru')
CLIENT_CACHE_SIZE = _env_int('GFS_CLIENT_CACHE_SIZE', 4096)
CLIENT_CACHE_TTL = _env_float('GFS_CLIENT_CACHE_TTL', 60.0)


# --------------------------------------------------------------------------
# Replication / placement
# --------------------------------------------------------------------------
REPLICATION_FACTOR = _env_int('GFS_REPLICATION_FACTOR', 3)

# Cap on clone tasks in flight, so recovering from a lost node does not
# saturate the network and tank foreground throughput. See ADR-003.
MAX_CLONES_IN_FLIGHT_PER_SERVER = _env_int('GFS_MAX_CLONES_PER_SERVER', 2)
MAX_CLONES_IN_FLIGHT_CLUSTER = _env_int('GFS_MAX_CLONES_CLUSTER', 8)

# How often the master rescans for under-replicated chunks. The original loop
# ran every 30s, which with 30s heartbeats meant a node failure could go
# unaddressed for over a minute. See ADR-003.
REPLICATION_SCAN_INTERVAL = _env_float('GFS_REPLICATION_SCAN_INTERVAL', 3.0)

# How often a chunk server re-checks who the leader is. The fast variant
# applies while no leader is known -- at start-up, or during a failover --
# where a flat 10s wait dominated cluster boot time.
LEADER_DISCOVERY_INTERVAL = _env_float('GFS_LEADER_DISCOVERY_INTERVAL', 5.0)
LEADER_DISCOVERY_RETRY_INTERVAL = _env_float('GFS_LEADER_DISCOVERY_RETRY', 0.5)

# A leader may grant write leases only while a majority has confirmed its
# leadership within this window. Must be comfortably below
# ELECTION_TIMEOUT_MIN so a partitioned leader stops granting strictly
# before a replacement can be elected -- otherwise both sides hand out
# leases on the same chunk and replicas diverge. See ADR-007.
LEADER_LEASE_WINDOW = _env_float('GFS_LEADER_LEASE_WINDOW', 0.4)


# --------------------------------------------------------------------------
# Erasure coding (Phase 1)
# --------------------------------------------------------------------------
# 'replica' keeps 3x replication forever; 'ec' lets the background converter
# re-encode cold chunks as Reed-Solomon shards. See ADR-005.
STORAGE_MODE = _env_str('GFS_STORAGE_MODE', 'replica')
EC_DATA_SHARDS = _env_int('GFS_EC_DATA_SHARDS', 6)
EC_PARITY_SHARDS = _env_int('GFS_EC_PARITY_SHARDS', 3)
# A chunk untouched for this long is eligible for conversion to EC.
EC_WARM_AFTER_SECONDS = _env_float('GFS_EC_WARM_AFTER', 300.0)


# --------------------------------------------------------------------------
# Timing (Phase 0.8)
# --------------------------------------------------------------------------
# The originals were 30s/60s heartbeats with 5-8s election timeouts, which made
# a node failure take a full minute to even notice. See ADR-008.
HEARTBEAT_INTERVAL = _env_float('GFS_HEARTBEAT_INTERVAL', 5.0)
HEARTBEAT_MISS_LIMIT = _env_float('GFS_HEARTBEAT_MISS_LIMIT', 3.0)
LEASE_TIMEOUT = _env_float('GFS_LEASE_TIMEOUT', 60.0)

ELECTION_TIMEOUT_MIN = _env_int('GFS_ELECTION_TIMEOUT_MIN', 800)     # ms
ELECTION_TIMEOUT_MAX = _env_int('GFS_ELECTION_TIMEOUT_MAX', 1600)    # ms
MASTER_HEARTBEAT_INTERVAL = _env_int('GFS_MASTER_HEARTBEAT_INTERVAL', 150)  # ms
INITIAL_ELECTION_TIMEOUT = _env_int('GFS_INITIAL_ELECTION_TIMEOUT', 3000)   # ms


def summary() -> dict:
    """Snapshot of the active configuration, recorded in benchmark results."""
    return {
        'max_workers': DEFAULT_MAX_WORKERS,
        'max_message_bytes': MAX_MESSAGE_BYTES,
        'oplog_mode': OPLOG_MODE,
        'oplog_group_commit_ms': OPLOG_GROUP_COMMIT_MS,
        'checkpoint_every_ops': CHECKPOINT_EVERY_OPS,
        'client_cache_mode': CLIENT_CACHE_MODE,
        'client_cache_size': CLIENT_CACHE_SIZE,
        'client_cache_ttl': CLIENT_CACHE_TTL,
        'replication_factor': REPLICATION_FACTOR,
        'storage_mode': STORAGE_MODE,
        'ec_data_shards': EC_DATA_SHARDS,
        'ec_parity_shards': EC_PARITY_SHARDS,
        'heartbeat_interval': HEARTBEAT_INTERVAL,
        'election_timeout_ms': [ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX],
    }

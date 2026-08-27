#!/usr/bin/env bash
set -euo pipefail

# start_chunkservers.sh
# Usage: ./start_chunkservers.sh <N> [--clean]
#
# Environment overrides:
#   IMAGE        (default: gfs-system)
#   BASE_PORT    (default: 8010)
#   CONTAINER_PORT (default: 8000)
#   PREFIX       (default: chunkserver)
#   HOST_IP      (default: auto-detected)
#   MASTER_PORTS (default: 9010,9011,9012)
#
# Example:
#   IMAGE=gfs-system ./start_chunkservers.sh 5
#   ./start_chunkservers.sh 3 --clean
#   HOST_IP=192.168.1.100 ./start_chunkservers.sh 4

# --- parse args ---
if [[ "${#}" -lt 1 ]]; then
  echo "Usage: $0 <number_of_chunkservers> [--clean]"
  exit 1
fi

NUM="$1"
shift || true

CLEAN=false
while [[ "${#}" -gt 0 ]]; do
  case "$1" in
    --clean) CLEAN=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if ! [[ "$NUM" =~ ^[0-9]+$ ]] || [[ "$NUM" -le 0 ]]; then
  echo "Error: <number_of_chunkservers> must be a positive integer"
  exit 1
fi

# --- helper: detect host IPv4 (first global non-loopback) ---
detect_host_ip() {
  local ip=""
  if command -v ip >/dev/null 2>&1; then
    ip=$(ip -4 addr show scope global | awk '/inet/ {print $2}' | cut -d/ -f1 | head -n1 || true)
  fi
  if [[ -z "$ip" ]]; then
    ip=$(hostname -I 2>/dev/null | awk '{print $1}' || true)
  fi
  if [[ -z "$ip" ]]; then
    echo "127.0.0.1"
  else
    echo "$ip"
  fi
}

# --- configurable vars (env overrides) ---
IMAGE="${IMAGE:-gfs-system}"
BASE_PORT="${BASE_PORT:-8010}"
CONTAINER_PORT="${CONTAINER_PORT:-8000}"
PREFIX="${PREFIX:-chunkserver}"
SLEEP_BETWEEN_STARTS="${SLEEP_BETWEEN_STARTS:-0.2}"

# Auto-detect host IP if not provided
HOST_IP="${HOST_IP:-$(detect_host_ip)}"
MASTER_PORTS="${MASTER_PORTS:-9010,9011,9012}"

# Build master addresses using detected host IP
IFS=',' read -ra PORT_ARRAY <<< "$MASTER_PORTS"
MASTER_ADDRESSES=""
for port in "${PORT_ARRAY[@]}"; do
    if [[ -z "$MASTER_ADDRESSES" ]]; then
        MASTER_ADDRESSES="${HOST_IP}:${port}"
    else
        MASTER_ADDRESSES="${MASTER_ADDRESSES},${HOST_IP}:${port}"
    fi
done

echo "🚀 STARTING GFS CHUNK SERVERS"
echo "======================================"
echo "Image:             $IMAGE"
echo "Number servers:    $NUM"
echo "Host IP:           $HOST_IP"
echo "Host ports:        $BASE_PORT .. $((BASE_PORT + NUM - 1))"
echo "Container port:    $CONTAINER_PORT"
echo "Volume prefix:     chunkserver-data-<n>"
echo "Master addresses:  $MASTER_ADDRESSES"
echo "Clean mode:        $CLEAN"
echo "======================================"
echo

# --- check docker presence ---
if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker not found in PATH. Install Docker first."
  exit 1
fi

# --- optionally clean existing containers + volumes ---
if [[ "$CLEAN" == "true" ]]; then
  echo "CLEAN MODE: removing containers and volumes for prefix '$PREFIX'..."
  for i in $(seq 1 "$NUM"); do
    name="${PREFIX}${i}"
    volname="chunkserver-data-${i}"
    if docker ps -a --format '{{.Names}}' | grep -wq "${name}"; then
      echo "Stopping & removing container ${name}..."
      docker rm -f "${name}" >/dev/null || true
    fi
    if docker volume ls --format '{{.Name}}' | grep -wq "${volname}"; then
      echo "Removing volume ${volname}..."
      docker volume rm "${volname}" >/dev/null || true
    fi
  done
  echo "Clean done."
  echo
fi

# --- start chunkservers ---
for i in $(seq 1 "$NUM"); do
  name="${PREFIX}${i}"
  volname="chunkserver-data-${i}"
  host_port=$((BASE_PORT + i - 1))

  # Convert comma-separated master addresses to space-separated for command line
  IFS=',' read -ra MASTER_ARRAY <<< "$MASTER_ADDRESSES"

  echo "=== Starting ${name} (host port ${host_port}) ==="
  echo "Server ID:    ${name}"
  echo "Host Port:    ${host_port} -> ${CONTAINER_PORT}"
  echo "Masters:      ${MASTER_ARRAY[*]}"
  echo "Volume:       ${volname} -> /app/chunks"

  # create volume if not exists
  if ! docker volume ls --format '{{.Name}}' | grep -wq "${volname}"; then
    docker volume create "${volname}" >/dev/null
    echo "Created volume ${volname}"
  fi

  # remove existing container if present (keeps volume unless CLEAN was used)
  if docker ps -a --format '{{.Names}}' | grep -wq "${name}"; then
    echo "Removing existing container ${name}..."
    docker rm -f "${name}" >/dev/null || true
  fi

  # Run container: expose host_port -> container CONTAINER_PORT and mount volume
  # We pass master addresses as separate args and external address for client connectivity
  echo "Starting container with command:"
  echo "  python chunk_server.py --server-id '${name}' --port ${CONTAINER_PORT} --external-address '${HOST_IP}' --external-port ${host_port} --masters ${MASTER_ARRAY[*]}"
  
  docker run -d --name "${name}" -p "${host_port}:${CONTAINER_PORT}" \
    -v "${volname}:/app/chunks" \
    "${IMAGE}" \
    python chunk_server.py --server-id "${name}" --port "${CONTAINER_PORT}" \
    --external-address "${HOST_IP}" --external-port "${host_port}" --masters "${MASTER_ARRAY[@]}"

  echo "✅ ${name} started successfully"
  echo

  # short delay so many containers don't hammer each other at once
  sleep "${SLEEP_BETWEEN_STARTS}"
done

echo
echo "🎉 ALL CHUNK SERVERS STARTED"
echo "=============================="
echo "Running chunkservers:"
docker ps --filter "name=${PREFIX}" --format "table {{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Status}}"

echo
echo "📊 USEFUL COMMANDS:"
echo "View logs:           docker logs <container_name>"
echo "Follow logs:         docker logs -f <container_name>"
echo "Stop all:            docker stop \$(docker ps -q --filter name=${PREFIX})"
echo "Remove all:          docker rm -f \$(docker ps -aq --filter name=${PREFIX})"
echo "Inspect volume:      docker run --rm -v chunkserver-data-1:/data alpine ls -la /data"
echo
echo "📡 MASTER ADDRESSES USED: $MASTER_ADDRESSES"
echo "🌍 HOST IP DETECTED: $HOST_IP"
echo
echo "Example usage with custom settings:"
echo "  HOST_IP=192.168.1.100 MASTER_PORTS=9010,9011,9012 ./start_chunkservers.sh 5"
echo "  HOST_IP=10.0.0.50 ./start_chunkservers.sh 3 --clean"
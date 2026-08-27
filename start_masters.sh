#!/usr/bin/env bash
set -euo pipefail

# start_masters.sh
# Usage: ./start_masters.sh <N> [--clean]
#
# Environment overrides:
#   IMAGE        (default: gfs-system)
#   BASE_PORT    (default: 9010)
#   CONTAINER_PORT (default: 9000)
#   PREFIX       (default: master)
#
# Example:
#   IMAGE=gfs-system ./start_masters_with_volumes.sh 3
#   ./start_masters_with_volumes.sh 3 --clean

# --- parse args ---
if [[ "${#}" -lt 1 ]]; then
  echo "Usage: $0 <number_of_masters> [--clean]"
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
  echo "Error: <number_of_masters> must be a positive integer"
  exit 1
fi

# --- configurable vars (env overrides) ---
IMAGE="${IMAGE:-gfs-system}"
BASE_PORT="${BASE_PORT:-9010}"
CONTAINER_PORT="${CONTAINER_PORT:-9000}"
PREFIX="${PREFIX:-master}"
SLEEP_BETWEEN_STARTS="${SLEEP_BETWEEN_STARTS:-0.2}"

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

HOST_IP="${HOST_IP:-$(detect_host_ip)}"

echo "Image:          $IMAGE"
echo "Number masters: $NUM"
echo "Host IP:        $HOST_IP"
echo "Host ports:     $BASE_PORT .. $((BASE_PORT + NUM - 1))"
echo "Container port: $CONTAINER_PORT"
echo "Volume prefix:  master-metadata-<n>"
echo "Clean mode:     $CLEAN"
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
    volname="master-metadata-${i}"
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

# --- start masters ---
for i in $(seq 1 "$NUM"); do
  name="${PREFIX}${i}"
  volname="master-metadata-${i}"
  host_port=$((BASE_PORT + i - 1))

  # Build peers list (other masters)
  peers=()
  for j in $(seq 1 "$NUM"); do
    if [[ "$j" -ne "$i" ]]; then
      peer_port=$((BASE_PORT + j - 1))
      peers+=("${HOST_IP}:${peer_port}")
    fi
  done

  echo "=== Starting ${name} (host port ${host_port})"
  echo "Peers: ${peers[*]}"
  echo "Volume: ${volname} -> /app/metadata"

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
  # We pass peers as separate args (nargs='*' in master.py expected)
  docker run -d --name "${name}" -p "${host_port}:${CONTAINER_PORT}" \
    -v "${volname}:/app/metadata" \
    "${IMAGE}" \
    python master.py --master-id "${name}" --port "${CONTAINER_PORT}" --peers "${peers[@]}"

  # short delay so many containers don't hammer each other at once
  sleep "${SLEEP_BETWEEN_STARTS}"
done

echo
echo "Done. Running masters:"
docker ps --filter "name=${PREFIX}" --format "table {{.Names}}\t{{.Image}}\t{{.Ports}}"

echo
echo "To inspect a volume's contents (example for master1):"
echo "  docker run --rm -v master-metadata-1:/data alpine ls -la /data"
echo
echo "If you used a custom image name, ensure the image contains 'master.py' at the container root or adjust the command."

#!/usr/bin/env bash
# Sourced by every job script.
set -euo pipefail

export PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
export PY="$PROJECT_ROOT/.venv-ada/bin/python"

# Chunk data goes to node-local scratch, never to NFS home. Writing gigabytes
# of replicas to a shared home directory is slow and affects other users.
if [ -n "${SLURM_JOB_ID:-}" ] && [ -d "/scratch" ]; then
    export GFS_DATA_ROOT="/scratch/$USER/gfs-$SLURM_JOB_ID"
else
    export GFS_DATA_ROOT="${TMPDIR:-/tmp}/gfs-$$"
fi
mkdir -p "$GFS_DATA_ROOT"

cleanup() { rm -rf "$GFS_DATA_ROOT" 2>/dev/null || true; }
trap cleanup EXIT

banner() {
    echo
    echo "============================================================"
    echo "  $*"
    echo "  host=$(hostname)  cpus=${SLURM_CPUS_ON_NODE:-$(nproc)}  $(date '+%F %T')"
    echo "============================================================"
}

export PYTHONUNBUFFERED=1
cd "$PROJECT_ROOT"

#!/usr/bin/env bash
# One-time setup on Ada. Idempotent -- safe to re-run.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$PROJECT_ROOT/.venv-ada"

echo "== GFS setup on $(hostname) =="

# A venv rsync'd from a laptop has absolute paths and the wrong interpreter,
# so Ada gets its own, under a distinct name.
module load python/3.9 2>/dev/null || module load python 2>/dev/null || true
PY="$(command -v python3.11 || command -v python3.10 || command -v python3.9 || command -v python3)"
echo "interpreter: $PY ($("$PY" --version 2>&1))"

if [ ! -d "$VENV" ]; then
    "$PY" -m venv "$VENV"
fi
"$VENV/bin/python" -m pip install --quiet --upgrade pip

# Prefer a prebuilt wheel: compiling grpcio from source on a login node takes
# ~20 minutes and will look like a hang.
if ! "$VENV/bin/python" -m pip install --quiet --only-binary=:all: \
        grpcio grpcio-tools protobuf numpy pytest 2>/dev/null; then
    echo "WARNING: no binary wheel available; building from source (slow)."
    "$VENV/bin/python" -m pip install --quiet grpcio grpcio-tools protobuf numpy pytest
fi

cd "$PROJECT_ROOT"
"$VENV/bin/python" -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. gfs.proto
echo "protobuf stubs regenerated"

mkdir -p ada/logs bench/results
"$VENV/bin/python" -c "import grpc, numpy, pytest; print('deps ok, grpcio', grpc.__version__)"

cat <<MSG

Setup complete.

  venv:    $VENV
  next:    sbatch ada/test_suite.slurm

Before submitting, set --partition and --account in the .slurm files
(check 'sinfo' for valid partitions) or the jobs will queue indefinitely.
MSG

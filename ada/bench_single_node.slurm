#!/usr/bin/env bash
#SBATCH --job-name=gfs-bench-1n
#SBATCH --output=ada/logs/bench1n-%j.out
#SBATCH --error=ada/logs/bench1n-%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=02:00:00
##SBATCH --partition=long
##SBATCH --account=<your-account>

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
banner "Single-node benchmark suite"

# Directly comparable to the laptop run, with enough cores that the clients
# are not themselves the bottleneck in the concurrency sweep.
export GFS_BENCH_SIZES=16,64,256,1024
export GFS_BENCH_BUDGET=900

banner "1/4  sequential throughput"
"$PY" bench/bench_throughput.py

banner "2/4  concurrency sweep"
GFS_BENCH_CLIENTS=1,2,4,8,16,32,64 GFS_BENCH_CLIENT_MB=16 \
    "$PY" bench/bench_concurrent.py

banner "3/4  client cache A/B"
GFS_BENCH_FILES=16 GFS_BENCH_FILE_MB=16 GFS_BENCH_PASSES=10 \
    "$PY" bench/bench_cache.py

banner "4/4  durability A/B (oplog vs snapshot, group commit on/off)"
GFS_BENCH_OPS=2000 GFS_BENCH_WRITERS=32 "$PY" bench/bench_metadata.py

banner "done -- results in bench/results/"
ls -la bench/results/

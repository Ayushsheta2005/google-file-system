#!/usr/bin/env bash
#SBATCH --job-name=gfs-bench-4n
#SBATCH --output=ada/logs/bench4n-%j.out
#SBATCH --error=ada/logs/bench4n-%j.err
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=20
#SBATCH --mem=48G
#SBATCH --time=03:00:00
##SBATCH --partition=long
##SBATCH --account=<your-account>

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
banner "Multi-node benchmark -- real network between components"

# These are the numbers worth quoting. On one host every "network" hop is a
# memory copy, so the write pipeline and replication look free; here they
# are not. Expect lower absolute throughput and a more honest figure.
NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
echo "nodes: ${NODES[*]}"

"$PY" ada/run_distributed.py \
    --masters-on "${NODES[0]}" \
    --chunkservers-on "${NODES[1]},${NODES[2]},${NODES[3]}" \
    --chunkservers-per-node 2 \
    --bench throughput,concurrent,cache

banner "done"

#!/usr/bin/env bash
#SBATCH --job-name=gfs-bench-ec
#SBATCH --output=ada/logs/benchec-%j.out
#SBATCH --error=ada/logs/benchec-%j.err
#SBATCH --nodes=9
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=02:00:00
##SBATCH --partition=long
##SBATCH --account=<your-account>

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
banner "Erasure coding on 9 physical hosts"

# One shard per physical host is the only configuration in which RS(6,3)'s
# durability claim is literally true: 9 independent failure domains, any 6
# of which reconstruct. On a single machine that independence is fiction.
NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
echo "nodes: ${NODES[*]}"

export GFS_STORAGE_MODE=ec
export GFS_EC_DATA_SHARDS=6
export GFS_EC_PARITY_SHARDS=3
export GFS_EC_WARM_AFTER=30

"$PY" ada/run_distributed.py \
    --masters-on "${NODES[0]}" \
    --chunkservers-on "$(IFS=,; echo "${NODES[*]:0:9}")" \
    --chunkservers-per-node 1 \
    --bench ec

banner "done"

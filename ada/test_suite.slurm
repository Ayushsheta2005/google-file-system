#!/usr/bin/env bash
#SBATCH --job-name=gfs-tests
#SBATCH --output=ada/logs/tests-%j.out
#SBATCH --error=ada/logs/tests-%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=01:00:00
## Set these before submitting -- check `sinfo`.
##SBATCH --partition=long
##SBATCH --account=<your-account>

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
banner "GFS test suite"

# Correctness first: if these fail the benchmark numbers mean nothing.
"$PY" -m pytest tests/ -v -p no:cacheprovider --tb=short
echo "exit: $?"

#!/usr/bin/env bash
# One-shot: set up, detect the cluster's partition/account, submit everything.
#
# Run this once after ssh'ing in. It is idempotent -- safe to re-run after a
# failure, and it will not resubmit jobs that are already queued or running.
#
#   cd ~/gfs && bash ada/run_all.sh
#
# Then come back later and run:  bash ada/collect.sh

set -uo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
mkdir -p ada/logs bench/results

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
warn() { printf '   !! %s\n' "$*"; }

# ---------------------------------------------------------------- 1. setup
say "1/4  Environment"
if ! bash ada/setup.sh; then
    warn "setup failed -- fix the error above before continuing"
    exit 1
fi

# -------------------------------------------------- 2. partition / account
say "2/4  Scheduler settings"

# The .slurm files ship with --partition and --account commented out, because
# valid values differ per cluster and a wrong one leaves jobs queued forever.
# Detect them rather than making the user read sinfo output.
PARTITION="${GFS_PARTITION:-}"
if [ -z "$PARTITION" ]; then
    # The scheduler's own default partition, marked with '*' in sinfo. An
    # earlier version ranked partitions by idle nodes and time limit and
    # picked a small special-purpose one the account could not submit to.
    # The default is the right answer on every cluster that has one.
    PARTITION="$(sinfo -h -o '%P' 2>/dev/null | grep '\*' | head -1 | tr -d '*')"
fi
if [ -z "$PARTITION" ]; then
    # No default marked: take whichever up partition has the most nodes.
    PARTITION="$(sinfo -h -o '%P %a %D' 2>/dev/null \
        | awk '$2=="up" {gsub(/\*/,"",$1); n[$1]+=$3} END {for (p in n) print n[p], p}' \
        | sort -rn | head -1 | awk '{print $2}')"
fi
if [ -z "$PARTITION" ]; then
    warn "could not detect a partition. Run 'sinfo', then re-run as:"
    warn "    GFS_PARTITION=<name> bash ada/run_all.sh"
    exit 1
fi

ACCOUNT="${GFS_ACCOUNT:-}"
if [ -z "$ACCOUNT" ]; then
    ACCOUNT="$(sacctmgr -nP show assoc user="$USER" format=Account 2>/dev/null | head -1)"
fi

echo "   partition: $PARTITION"
echo "   account:   ${ACCOUNT:-<none; omitting --account>}"

MAX_NODES="$(sinfo -h -p "$PARTITION" -o '%D' 2>/dev/null | awk '{s+=$1} END {print s+0}')"
echo "   nodes available in $PARTITION: ${MAX_NODES:-unknown}"

submit() {
    local script="$1" want_nodes="$2" label="$3"
    if squeue -h -u "$USER" -o '%j' 2>/dev/null | grep -qx "$label"; then
        echo "   $label already queued/running -- skipping"
        return 0
    fi
    if [ -n "${MAX_NODES:-}" ] && [ "$MAX_NODES" -gt 0 ] \
       && [ "$want_nodes" -gt "$MAX_NODES" ]; then
        warn "$label wants $want_nodes nodes, partition has $MAX_NODES -- skipped"
        warn "  (for EC specifically: re-run with GFS_EC_DATA_SHARDS=4"
        warn "   GFS_EC_PARITY_SHARDS=2 and edit --nodes to $MAX_NODES)"
        return 0
    fi
    local args=(--partition="$PARTITION")
    [ -n "$ACCOUNT" ] && args+=(--account="$ACCOUNT")
    local out
    out="$(sbatch "${args[@]}" "$script" 2>&1)"
    if [ $? -eq 0 ]; then
        echo "   $label -> ${out}"
        return 0
    fi

    # sacctmgr can report an account that is not valid for every partition,
    # and many clusters do not need --account at all. Retry without it
    # rather than failing on a flag we guessed.
    if [ -n "$ACCOUNT" ] && echo "$out" | grep -qi 'invalid account'; then
        out="$(sbatch --partition="$PARTITION" "$script" 2>&1)"
        if [ $? -eq 0 ]; then
            echo "   $label -> ${out}  (submitted without --account)"
            return 0
        fi
    fi
    warn "$label failed to submit: $out"
}

# ------------------------------------------------------------ 3. correctness
say "3/4  Submitting correctness job"
# Deliberately submitted alone and first. If the suite does not come back 188
# on this machine -- a different Python or grpcio is the likely cause --
# benchmark numbers from this environment are not worth collecting.
submit ada/test_suite.slurm 1 gfs-tests

# ------------------------------------------------------------ 4. benchmarks
say "4/4  Submitting benchmark jobs"
submit ada/bench_single_node.slurm 1 gfs-bench-1n
submit ada/bench_multi_node.slurm  4 gfs-bench-4n
submit ada/bench_ec.slurm          9 gfs-bench-ec

say "Submitted"
squeue -u "$USER" 2>/dev/null || true

cat <<MSG

Check progress:   squeue -u $USER
Watch a log:      tail -f ada/logs/tests-*.out
When finished:    bash ada/collect.sh

Correctness is the gate. If gfs-tests does not report 188 passed, read
ada/logs/tests-*.err before trusting anything the benchmark jobs produce.
MSG

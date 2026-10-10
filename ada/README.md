# Running on Ada (IIIT-H HPC)

Everything here runs on the cluster side. You run two commands; the scripts
do the rest.

## The short version

Three commands total.

```bash
# 1. from your laptop
rsync -av --exclude '.venv' --exclude '.venv-ada' --exclude '.git' \
      --exclude 'bench/results' \
      google_file_system_with_multiple_masters/ <user>@ada.iiit.ac.in:~/gfs/
```

```bash
# 2. on Ada -- sets up, detects partition/account, submits everything
cd ~/gfs && bash ada/run_all.sh
```

```bash
# 3. once the jobs finish (squeue -u $USER to check)
bash ada/collect.sh
```

`run_all.sh` detects the partition from `sinfo` and the account from
`sacctmgr`, skips any job asking for more nodes than the partition has, and
will not resubmit something already queued. Override detection if needed:

```bash
GFS_PARTITION=long GFS_ACCOUNT=myaccount bash ada/run_all.sh
```

`collect.sh` gathers every log and result JSON into `ada/RESULTS.txt`.

Everything below is the manual path, and what each job actually measures.

## Why run on Ada at all

The laptop numbers are real but they measure a single machine's loopback:
every "network" hop is memory copy, and all masters, chunk servers and
clients contend for the same 8 cores. Ada gives three things the laptop
cannot:

1. **Real network.** `bench_multi_node.slurm` puts masters and chunk servers
   on *different physical hosts*, so replication and the write pipeline pay
   actual network cost. Expect lower absolute throughput and a more honest
   number.
2. **Enough cores to saturate.** The 32-client sweep plateaued at 272 MB/s
   on the laptop, which is as likely to be client-side CPU contention as
   server-side. On a 40-core node the clients stop being the bottleneck.
3. **Enough distinct hosts for erasure coding to mean something.** RS(6,3)
   needs 9 nodes holding one shard each for the durability claim to hold.
   On one machine, "9 independent failure domains" is a fiction.

## What each job does

| Script | Nodes | Walltime | What it measures |
|---|---|---|---|
| `test_suite.slurm` | 1 | 1 h | Correctness. Run this first — if it fails, the benchmarks are meaningless. |
| `bench_single_node.slurm` | 1 | 2 h | Throughput, concurrency sweep, cache A/B, oplog A/B. Directly comparable to the laptop numbers. |
| `bench_multi_node.slurm` | 4 | 3 h | The same suite with the cluster spread over real hosts. These are the numbers worth quoting. |
| `bench_ec.slurm` | 9 | 2 h | RS(6,3) with one shard per physical host: conversion time, degraded-read latency, shard repair, measured storage overhead. |

## Notes specific to Ada

- **Use `$TMPDIR` or scratch for chunk data, not your home directory.** Home
  is NFS-backed and shared; writing 3 GB of chunk replicas there is slow and
  antisocial. `setup.sh` points `GFS_DATA_ROOT` at node-local scratch.
- **Ports.** Multiple jobs on one node will collide if they hardcode ports.
  Everything here uses `free_port()` from `cluster.py`, which asks the OS.
- **Modules.** `setup.sh` loads `python/3.9` (or newer) and builds a venv
  under `~/gfs/.venv-ada`, kept separate from any laptop venv that got
  rsync'd by accident.
- **Partition and account.** The `--partition` and `--account` lines in each
  `.slurm` file are placeholders. Check `sinfo` and set them before
  submitting, or the job sits in the queue forever.
- **grpcio wheels.** If `pip install grpcio` tries to build from source and
  takes 20 minutes, that is normal on a login node without a matching wheel.
  `setup.sh` prefers a binary wheel and falls back with a warning.

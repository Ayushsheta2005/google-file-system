#!/usr/bin/env python3
"""Namespace mutation throughput, and what group commit is worth.

Three configurations:
  snapshot          - the original 30s whole-JSON dump (no per-op durability)
  oplog, no batch   - fsync per mutation
  oplog, 5ms batch  - group commit
"""
import multiprocessing as mp, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bench.harness import Results
from cluster import LocalCluster
import logging
logging.getLogger().setLevel(logging.ERROR)

OPS = int(os.environ.get('GFS_BENCH_OPS', '300'))
WRITERS = int(os.environ.get('GFS_BENCH_WRITERS', '16'))

CONFIGS = [
    ('snapshot (original)', {'GFS_OPLOG_MODE': 'snapshot'}),
    ('oplog, fsync per op', {'GFS_OPLOG_MODE': 'oplog', 'GFS_OPLOG_GROUP_COMMIT_MS': '0'}),
    ('oplog, 5ms group commit', {'GFS_OPLOG_MODE': 'oplog', 'GFS_OPLOG_GROUP_COMMIT_MS': '5'}),
]


def creator(a):
    addrs, wid, n, tag = a
    import logging as lg, sys
    lg.getLogger().setLevel(lg.CRITICAL)
    sys.path.insert(0, '/Users/ayushmsheta/Downloads/graviton/gfs/google_file_system_with_multiple_masters')
    from client import MultiMasterGFSClient
    cl = MultiMasterGFSClient(addrs)
    ok = 0
    for i in range(n):
        if cl.create_file(f'/meta/{tag}/w{wid}_{i}.txt'):
            ok += 1
    return ok


def main():
    res = Results('metadata_ops')
    for label, env in CONFIGS:
        c = LocalCluster(masters=3, chunkservers=3, log_level='ERROR', env=env)
        c.start()
        try:
            per = max(1, OPS // WRITERS)
            tag = label.split()[0].strip('(),')
            jobs = [(c.all_master_addresses, w, per, tag) for w in range(WRITERS)]
            t0 = time.perf_counter()
            with mp.Pool(WRITERS) as pool:
                done = sum(pool.map(creator, jobs))
            wall = time.perf_counter() - t0
            res.add(config=label, writers=WRITERS, creates=done,
                    ops_per_sec=round(done / wall, 1), wall_s=round(wall, 2),
                    durable=('no' if 'snapshot' in label else 'yes'))
        finally:
            c.destroy()
    res.save({'writers': WRITERS, 'target_ops': OPS})


if __name__ == '__main__':
    mp.set_start_method('spawn', force=True)
    main()

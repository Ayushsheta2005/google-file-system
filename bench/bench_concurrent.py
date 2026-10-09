#!/usr/bin/env python3
"""Concurrent client scaling.

Separate *processes*, not threads: under the GIL a thread-based client sweep
measures the client's own contention rather than the cluster's capacity, and
flattens the curve for the wrong reason.
"""
import json, multiprocessing as mp, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bench.harness import Results, throughput_mbps, percentiles
from cluster import LocalCluster
import logging
logging.getLogger().setLevel(logging.ERROR)

PER_CLIENT_MB = int(os.environ.get('GFS_BENCH_CLIENT_MB', '8'))
LEVELS = [int(x) for x in os.environ.get('GFS_BENCH_CLIENTS', '1,2,4,8,16,32').split(',')]


def worker(args):
    """One client process: write its own file, read it back, verify."""
    addrs, idx, size_mb, level = args
    import logging as lg
    lg.getLogger().setLevel(lg.ERROR)
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from client import MultiMasterGFSClient
    try:
        cl = MultiMasterGFSClient(addrs)
        payload = bytes([idx % 256]) * (size_mb * 1024 * 1024)
        # Level must be in the path: reusing a name across levels makes
        # CreateFile fail with 'already exists' and looks like a
        # concurrency failure when it is just the benchmark's own bug.
        path = f'/bench/n{level}/conc_{idx}.bin'
        t0 = time.perf_counter()
        if not cl.create_file(path):
            return {'idx': idx, 'ok': False, 'why': 'create'}
        if not cl.write_file(path, payload, 0):
            return {'idx': idx, 'ok': False, 'why': 'write'}
        w = time.perf_counter() - t0
        t1 = time.perf_counter()
        got = cl.read_file(path)
        r = time.perf_counter() - t1
        return {'idx': idx, 'ok': got == payload, 'write_s': w, 'read_s': r,
                'bytes': len(payload)}
    except Exception as e:
        return {'idx': idx, 'ok': False, 'why': f'{type(e).__name__}: {e}'}


def main():
    res = Results('concurrent')
    c = LocalCluster(masters=3, chunkservers=4, log_level='ERROR')
    c.start()
    addrs = c.all_master_addresses
    try:
        for n in LEVELS:
            jobs = [(addrs, i, PER_CLIENT_MB, n) for i in range(n)]
            t0 = time.perf_counter()
            with mp.Pool(processes=n) as pool:
                out = pool.map(worker, jobs)
            wall = time.perf_counter() - t0

            good = [o for o in out if o.get('ok')]
            total_bytes = sum(o['bytes'] for o in good) * 2   # write + read
            res.add(clients=n,
                    succeeded=f'{len(good)}/{n}',
                    aggregate_mbps=throughput_mbps(total_bytes, wall),
                    wall_s=round(wall, 2),
                    write_p50=round(percentiles([o['write_s'] for o in good]).get('p50', 0), 3),
                    write_p95=round(percentiles([o['write_s'] for o in good]).get('p95', 0), 3),
                    read_p50=round(percentiles([o['read_s'] for o in good]).get('p50', 0), 3),
                    failures=[o.get('why') for o in out if not o.get('ok')][:2])
    finally:
        c.destroy()
    res.save({'per_client_mb': PER_CLIENT_MB})


if __name__ == '__main__':
    mp.set_start_method('spawn', force=True)
    main()

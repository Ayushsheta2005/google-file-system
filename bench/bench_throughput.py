#!/usr/bin/env python3
"""Sequential write and read throughput.

Scales up so a slow configuration reports something useful instead of
hanging: each size only runs if the previous one finished inside the budget.
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bench.harness import Results, throughput_mbps, percentiles
from cluster import LocalCluster

# The client logs a paragraph per chunk at INFO; at 1 GiB that is more output
# than measurement.
import logging
logging.getLogger('client').setLevel(logging.ERROR)
logging.getLogger().setLevel(logging.ERROR)

SIZES_MB = [int(x) for x in os.environ.get('GFS_BENCH_SIZES', '16,64,256,1024').split(',')]
BUDGET_S = float(os.environ.get('GFS_BENCH_BUDGET', '240'))

def main():
    r = Results('throughput')
    c = LocalCluster(masters=3, chunkservers=4, log_level='ERROR')
    c.start()
    try:
        client = c.client()
        for size_mb in SIZES_MB:
            payload = os.urandom(size_mb * 1024 * 1024)
            path = f'/bench/seq_{size_mb}.bin'
            client.create_file(path)

            t = time.perf_counter()
            ok = client.write_file(path, payload, 0)
            w = time.perf_counter() - t
            if not ok:
                r.add(size_mb=size_mb, status='WRITE_FAILED'); break

            client.cache.clear()           # cold read
            t = time.perf_counter()
            got = client.read_file(path)
            rd = time.perf_counter() - t
            cold_ok = (got == payload)

            t = time.perf_counter()        # warm read (cache hit)
            got2 = client.read_file(path)
            warm = time.perf_counter() - t

            r.add(size_mb=size_mb,
                  write_mbps=throughput_mbps(len(payload), w),
                  read_cold_mbps=throughput_mbps(len(payload), rd),
                  read_warm_mbps=throughput_mbps(len(payload), warm),
                  write_s=round(w, 2), read_s=round(rd, 2),
                  verified=cold_ok and got2 == payload)
            client.delete_file(path)
            if w + rd > BUDGET_S:
                print(f'  (stopping: {size_mb}MB took {w+rd:.0f}s, over budget)')
                break
    finally:
        c.destroy()
    r.save()

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""A/B the client metadata cache.

Counts master metadata RPCs directly by wrapping the stub accessor, rather
than inferring from cache counters -- the question is "how many times did we
actually talk to the master", and that should be measured at the wire, not
at the cache.
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bench.harness import Results, percentiles
from cluster import LocalCluster
from metacache import MetadataCache
import logging
logging.getLogger().setLevel(logging.ERROR)

FILES = int(os.environ.get('GFS_BENCH_FILES', '8'))
FILE_MB = int(os.environ.get('GFS_BENCH_FILE_MB', '8'))
PASSES = int(os.environ.get('GFS_BENCH_PASSES', '5'))


def count_master_rpcs(client):
    """Wrap the metadata RPCs so every call to the master is counted."""
    counter = {'n': 0}
    for name in ('get_file_info', 'get_chunk_locations'):
        pass
    orig = client._execute_with_leader_failover

    def counting(op, *a, **kw):
        counter['n'] += 1
        return orig(op, *a, **kw)

    client._execute_with_leader_failover = counting
    return counter


def run(cluster, mode, paths, payloads):
    client = cluster.client()
    client.cache = MetadataCache(mode=mode, maxsize=4096, ttl=60.0)
    counter = count_master_rpcs(client)

    latencies = []
    t0 = time.perf_counter()
    for _ in range(PASSES):
        for path, expected in zip(paths, payloads):
            t = time.perf_counter()
            got = client.read_file(path)
            latencies.append(time.perf_counter() - t)
            assert got == expected, f'{mode}: data mismatch on {path}'
    wall = time.perf_counter() - t0
    return counter['n'], wall, latencies


def main():
    res = Results('cache_ab')
    c = LocalCluster(masters=3, chunkservers=4, log_level='ERROR')
    c.start()
    try:
        setup = c.client()
        paths, payloads = [], []
        for i in range(FILES):
            p = f'/bench/cache/f{i}.bin'
            data = bytes([i % 251]) * (FILE_MB * 1024 * 1024)
            assert setup.create_file(p) and setup.write_file(p, data, 0)
            paths.append(p); payloads.append(data)

        out = {}
        for mode in ('off', 'lru'):
            rpcs, wall, lat = run(c, mode, paths, payloads)
            reads = FILES * PASSES
            pc = percentiles(lat)
            out[mode] = (rpcs, wall, pc)
            res.add(cache=mode,
                    master_rpcs=rpcs,
                    rpcs_per_read=round(rpcs / reads, 2),
                    total_reads=reads,
                    wall_s=round(wall, 3),
                    read_p50_ms=round(pc['p50'] * 1000, 2),
                    read_p95_ms=round(pc['p95'] * 1000, 2))

        off_rpcs, off_wall, _ = out['off']
        lru_rpcs, lru_wall, _ = out['lru']
        res.add(metric='SUMMARY',
                rpc_reduction_pct=round(100 * (off_rpcs - lru_rpcs) / off_rpcs, 1),
                latency_reduction_pct=round(100 * (off_wall - lru_wall) / off_wall, 1),
                speedup=f'{off_wall / lru_wall:.2f}x')
    finally:
        c.destroy()
    res.save({'files': FILES, 'file_mb': FILE_MB, 'passes': PASSES})


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Launch a GFS cluster across several Ada nodes and run benchmarks on it.

`cluster.py` spawns everything as local subprocesses, which is right for
tests and for the laptop but hides the thing the cluster exists to measure:
on one host every "network" hop between a client, a master and three
replicas is a memory copy. The write pipeline and re-replication look nearly
free, and they are not.

This places masters and chunk servers on *different physical hosts* using
`srun`, so the pipeline pays real network cost. Expect lower absolute
throughput than the single-node run -- that is the point, and those are the
numbers worth quoting.

Invoked from the .slurm files; see ada/README.md.
"""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from typing import Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

PY = os.path.join(ROOT, '.venv-ada', 'bin', 'python')
if not os.path.exists(PY):
    PY = sys.executable


def log(msg: str):
    print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)


class RemoteProc:
    """One cluster process pinned to a named node via `srun`."""

    def __init__(self, name: str, kind: str, node: str, port: int,
                 popen: subprocess.Popen, log_path: str):
        self.name = name
        self.kind = kind
        self.node = node
        self.port = port
        self.popen = popen
        self.log_path = log_path

    @property
    def address(self) -> str:
        return f'{self.node}:{self.port}'

    def alive(self) -> bool:
        return self.popen.poll() is None

    def kill(self):
        if self.alive():
            self.popen.send_signal(signal.SIGKILL)
            try:
                self.popen.wait(timeout=15)
            except subprocess.TimeoutExpired:
                pass

    def tail(self, n=30) -> str:
        try:
            with open(self.log_path) as f:
                return ''.join(f.readlines()[-n:])
        except OSError:
            return ''

    def __repr__(self):
        return f'<{self.kind} {self.name}@{self.node}:{self.port} ' \
               f'{"up" if self.alive() else "down"}>'


def pick_ports(n: int, base: int = 21000) -> List[int]:
    """Deterministic non-colliding ports derived from the job id.

    Two jobs landing on the same node would otherwise fight over the same
    range. The OS-assigned ports cluster.py uses are not available here,
    because the port has to be known before the remote process starts.
    """
    job = int(os.environ.get('SLURM_JOB_ID', os.getpid())) % 2000
    start = base + job * 20
    return [start + i for i in range(n)]


class DistributedCluster:
    def __init__(self, master_nodes: List[str], chunk_nodes: List[str],
                 chunkservers_per_node: int = 1, data_root: Optional[str] = None,
                 log_level: str = 'INFO'):
        self.master_nodes = master_nodes
        self.chunk_nodes = chunk_nodes
        self.per_node = chunkservers_per_node
        self.data_root = data_root or os.environ.get(
            'GFS_DATA_ROOT', os.path.join(os.environ.get('TMPDIR', '/tmp'),
                                          f'gfs-{os.getpid()}'))
        self.log_dir = os.path.join(HERE, 'logs')
        self.log_level = log_level
        os.makedirs(self.log_dir, exist_ok=True)
        self.masters: List[RemoteProc] = []
        self.chunkservers: List[RemoteProc] = []
        self.leader_address: Optional[str] = None

    # ------------------------------------------------------------- launch
    def _srun(self, node: str, name: str, kind: str, port: int,
              argv: List[str]) -> RemoteProc:
        log_path = os.path.join(self.log_dir, f'{name}.log')
        cmd = ['srun', '--nodes=1', '--ntasks=1', f'--nodelist={node}',
               '--exclusive=user', '--overlap'] + argv
        env = dict(os.environ)
        env['PYTHONUNBUFFERED'] = '1'
        handle = open(log_path, 'ab', buffering=0)
        popen = subprocess.Popen(cmd, cwd=ROOT, env=env,
                                 stdout=handle, stderr=subprocess.STDOUT,
                                 start_new_session=True)
        return RemoteProc(name, kind, node, port, popen, log_path)

    def start(self, timeout: float = 180.0):
        n_masters = len(self.master_nodes)
        n_chunk = len(self.chunk_nodes) * self.per_node
        ports = pick_ports(n_masters + n_chunk)
        m_ports, c_ports = ports[:n_masters], ports[n_masters:]

        addrs = [f'{node}:{port}'
                 for node, port in zip(self.master_nodes, m_ports)]
        log(f'master addresses: {addrs}')

        for i, (node, port) in enumerate(zip(self.master_nodes, m_ports)):
            name = f'master{i}'
            data_dir = os.path.join(self.data_root, name)
            peers = [a for a in addrs if a != f'{node}:{port}']
            argv = [PY, 'master.py', '--master-id', name, '--port', str(port),
                    '--data-dir', data_dir, '--log-level', self.log_level]
            if peers:
                argv += ['--peers'] + peers
            self.masters.append(self._srun(node, name, 'master', port, argv))

        time.sleep(5)   # let the masters bind before chunk servers dial in

        idx = 0
        for node in self.chunk_nodes:
            for _ in range(self.per_node):
                port = c_ports[idx]
                name = f'cs{idx}'
                data_dir = os.path.join(self.data_root, name)
                argv = [PY, 'chunk_server.py', '--server-id', name,
                        '--port', str(port), '--storage-dir', data_dir,
                        '--log-level', self.log_level,
                        '--external-address', node,
                        '--external-port', str(port),
                        '--masters'] + addrs
                self.chunkservers.append(
                    self._srun(node, name, 'chunkserver', port, argv))
                idx += 1

        self.master_addresses = addrs
        self.await_ready(timeout=timeout)

    # -------------------------------------------------------------- probe
    def status(self) -> Dict:
        import grpc
        import gfs_pb2
        import gfs_pb2_grpc
        from gfs_config import GRPC_CHANNEL_OPTIONS
        for addr in self.master_addresses:
            try:
                with grpc.insecure_channel(addr, options=GRPC_CHANNEL_OPTIONS) as ch:
                    stub = gfs_pb2_grpc.MasterServiceStub(ch)
                    r = stub.GetClusterStatus(
                        gfs_pb2.GetClusterStatusRequest(), timeout=5.0)
                    if r.success and r.is_leader:
                        self.leader_address = addr
                        return {
                            'leader': addr,
                            'live_chunkservers': sum(1 for c in r.chunk_servers if c.alive),
                            'total_chunks': r.total_chunks,
                            'ec_chunks': r.ec_chunks,
                            'under_replicated': r.under_replicated_chunks,
                        }
            except grpc.RpcError:
                continue
        return {}

    def await_ready(self, timeout: float = 180.0):
        want = len(self.chunkservers)
        deadline = time.time() + timeout
        last = {}
        while time.time() < deadline:
            last = self.status()
            if last and last.get('live_chunkservers', 0) >= want:
                log(f'cluster ready: leader={last["leader"]}, '
                    f'{last["live_chunkservers"]}/{want} chunk servers')
                return
            time.sleep(2.0)

        log('CLUSTER DID NOT COME UP')
        log(f'last status: {last}')
        for p in self.masters + self.chunkservers:
            log(f'--- {p!r}')
            print(p.tail(20), flush=True)
        raise RuntimeError('cluster not ready')

    def stop(self):
        for p in self.chunkservers + self.masters:
            p.kill()


# --------------------------------------------------------------------------
# Benchmarks against a distributed cluster
# --------------------------------------------------------------------------
def bench_throughput(cluster, results):
    from client import MultiMasterGFSClient
    for size_mb in (16, 64, 256, 1024):
        payload = os.urandom(size_mb * 1024 * 1024)
        c = MultiMasterGFSClient(cluster.master_addresses)
        path = f'/ada/seq_{size_mb}.bin'
        c.create_file(path)

        t = time.perf_counter()
        ok = c.write_file(path, payload, 0)
        w = time.perf_counter() - t
        if not ok:
            log(f'{size_mb}MB write FAILED')
            break
        c.cache.clear()
        t = time.perf_counter()
        got = c.read_file(path)
        r = time.perf_counter() - t

        row = {'bench': 'throughput', 'size_mb': size_mb,
               'write_mbps': round(size_mb / w, 2),
               'read_mbps': round(size_mb / r, 2),
               'verified': got == payload}
        log(str(row))
        results.append(row)
        c.delete_file(path)


def bench_concurrent(cluster, results):
    import multiprocessing as mp
    from bench.bench_concurrent import worker
    for n in (1, 2, 4, 8, 16, 32, 64):
        jobs = [(cluster.master_addresses, i, 16, n) for i in range(n)]
        t = time.perf_counter()
        with mp.Pool(n) as pool:
            out = pool.map(worker, jobs)
        wall = time.perf_counter() - t
        good = [o for o in out if o.get('ok')]
        row = {'bench': 'concurrent', 'clients': n,
               'succeeded': f'{len(good)}/{n}',
               'aggregate_mbps': round(sum(o['bytes'] for o in good) * 2
                                       / (1024 * 1024) / wall, 2)}
        log(str(row))
        results.append(row)


def bench_cache(cluster, results):
    from client import MultiMasterGFSClient
    from metacache import MetadataCache
    paths, payloads = [], []
    setup = MultiMasterGFSClient(cluster.master_addresses)
    for i in range(8):
        p = f'/ada/cache/f{i}.bin'
        data = bytes([i % 251]) * (16 * 1024 * 1024)
        setup.create_file(p)
        setup.write_file(p, data, 0)
        paths.append(p)
        payloads.append(data)

    for mode in ('off', 'lru'):
        c = MultiMasterGFSClient(cluster.master_addresses)
        c.cache = MetadataCache(mode=mode)
        calls = {'n': 0}
        orig = c._execute_with_leader_failover

        def counting(op, *a, **kw):
            calls['n'] += 1
            return orig(op, *a, **kw)
        c._execute_with_leader_failover = counting

        t = time.perf_counter()
        for _ in range(5):
            for path, want in zip(paths, payloads):
                assert c.read_file(path) == want
        wall = time.perf_counter() - t
        row = {'bench': 'cache', 'mode': mode, 'master_rpcs': calls['n'],
               'wall_s': round(wall, 3)}
        log(str(row))
        results.append(row)


def bench_ec(cluster, results):
    """Conversion, degraded read and repair with one shard per physical host."""
    from client import MultiMasterGFSClient
    c = MultiMasterGFSClient(cluster.master_addresses)
    payload = os.urandom(64 * 1024 * 1024)
    path = '/ada/ec/big.bin'
    c.create_file(path)
    c.write_file(path, payload, 0)

    t = time.perf_counter()
    deadline = time.time() + 300
    while time.time() < deadline:
        st = cluster.status()
        if st.get('ec_chunks', 0) >= 1:
            break
        time.sleep(2.0)
    convert_s = time.perf_counter() - t
    results.append({'bench': 'ec', 'metric': 'convert_s', 'value': round(convert_s, 1)})
    log(f'converted to EC in {convert_s:.1f}s')

    time.sleep(10)
    fresh = MultiMasterGFSClient(cluster.master_addresses)
    t = time.perf_counter()
    got = fresh.read_file(path)
    healthy = time.perf_counter() - t
    results.append({'bench': 'ec', 'metric': 'healthy_read_mbps',
                    'value': round(64 / healthy, 2), 'verified': got == payload})
    log(f'healthy EC read: {64 / healthy:.1f} MB/s, verified={got == payload}')

    parity = int(os.environ.get('GFS_EC_PARITY_SHARDS', '3'))
    for victim in cluster.chunkservers[1:1 + parity]:
        log(f'killing {victim!r}')
        victim.kill()
    time.sleep(5)

    degraded_client = MultiMasterGFSClient(cluster.master_addresses)
    t = time.perf_counter()
    try:
        got = degraded_client.read_file(path)
        degraded = time.perf_counter() - t
        results.append({'bench': 'ec', 'metric': 'degraded_read_mbps',
                        'value': round(64 / degraded, 2),
                        'shards_lost': parity, 'verified': got == payload})
        log(f'degraded read after losing {parity} shards: '
            f'{64 / degraded:.1f} MB/s, verified={got == payload}')
    except Exception as e:
        results.append({'bench': 'ec', 'metric': 'degraded_read', 'error': str(e)})
        log(f'degraded read FAILED: {e}')


BENCHES = {'throughput': bench_throughput, 'concurrent': bench_concurrent,
           'cache': bench_cache, 'ec': bench_ec}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--masters-on', required=True,
                    help='comma-separated node names for masters')
    ap.add_argument('--chunkservers-on', required=True,
                    help='comma-separated node names for chunk servers')
    ap.add_argument('--chunkservers-per-node', type=int, default=1)
    ap.add_argument('--bench', default='throughput',
                    help='comma-separated: ' + ','.join(BENCHES))
    ap.add_argument('--log-level', default='WARNING')
    args = ap.parse_args()

    import logging
    logging.getLogger().setLevel(logging.ERROR)

    cluster = DistributedCluster(
        master_nodes=[n for n in args.masters_on.split(',') if n],
        chunk_nodes=[n for n in args.chunkservers_on.split(',') if n],
        chunkservers_per_node=args.chunkservers_per_node,
        log_level=args.log_level)

    results = []
    try:
        cluster.start()
        for name in args.bench.split(','):
            name = name.strip()
            if name not in BENCHES:
                log(f'unknown benchmark {name!r}, skipping')
                continue
            log(f'=== {name} ===')
            try:
                BENCHES[name](cluster, results)
            except Exception as e:
                import traceback
                traceback.print_exc()
                results.append({'bench': name, 'error': str(e)})
    finally:
        cluster.stop()

    import gfs_config
    out = {
        'job_id': os.environ.get('SLURM_JOB_ID'),
        'nodes': os.environ.get('SLURM_JOB_NODELIST'),
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'topology': {
            'masters_on': args.masters_on,
            'chunkservers_on': args.chunkservers_on,
            'chunkservers_per_node': args.chunkservers_per_node,
        },
        'config': gfs_config.summary(),
        'rows': results,
    }
    path = os.path.join(ROOT, 'bench', 'results',
                        f'ada_{os.environ.get("SLURM_JOB_ID", "local")}.json')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(out, f, indent=2)
    log(f'results -> {path}')


if __name__ == '__main__':
    main()

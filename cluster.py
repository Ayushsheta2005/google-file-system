#!/usr/bin/env python3
"""Local (no-Docker) GFS cluster for development, tests and benchmarks.

Docker works but a container round-trip per edit is too slow to iterate on, and
the benchmark harness needs to kill individual processes to measure failover.
This runs every master and chunk server as a plain subprocess under a scratch
directory, so a whole cluster is `with LocalCluster(...) as c:`.

    python3 cluster.py --masters 3 --chunkservers 3
"""

import atexit
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
PYTHON = os.path.join(HERE, '.venv', 'bin', 'python')
if not os.path.exists(PYTHON):
    PYTHON = sys.executable


_HANDED_OUT_PORTS = set()


def free_port(attempts: int = 50) -> int:
    """Ask the OS for an unused port.

    Two guards beyond the naive version, both learned from flaky tests:

    * A port handed out earlier in this process is never handed out again.
      The OS will happily return the same ephemeral port twice in a row if
      nothing has bound it yet, and the second component then collides with
      the first.
    * The port is re-probed before being returned. A cluster that was just
      torn down can still hold a listening socket for a moment, and binding
      to it fails in a way that surfaces much later as an unrelated timeout.
    """
    for _ in range(attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        if port in _HANDED_OUT_PORTS:
            continue
        if port_open('127.0.0.1', port, timeout=0.05):
            continue
        _HANDED_OUT_PORTS.add(port)
        return port
    raise RuntimeError('could not find a free port')


def port_open(host: str, port: int, timeout: float = 0.25) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


class Proc:
    """One cluster process, with the knobs the harness needs to fault-inject."""

    def __init__(self, name: str, kind: str, port: int, popen: subprocess.Popen,
                 data_dir: str, argv: List[str], log_path: str):
        self.name = name
        self.kind = kind            # 'master' | 'chunkserver'
        self.port = port
        self.popen = popen
        self.data_dir = data_dir
        self.argv = argv
        self.log_path = log_path

    @property
    def address(self) -> str:
        return f'127.0.0.1:{self.port}'

    def alive(self) -> bool:
        return self.popen.poll() is None

    def kill(self, graceful: bool = False):
        """SIGKILL by default: a crash test that lets the process clean up is
        not a crash test."""
        if not self.alive():
            return
        self.popen.send_signal(signal.SIGTERM if graceful else signal.SIGKILL)
        try:
            self.popen.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.popen.kill()
            self.popen.wait(timeout=5)

    def pause(self):
        """SIGSTOP: freeze the process without losing its state. Simulates a
        GC pause or a hung node -- distinct from a crash, and the case that
        actually breaks lease protocols."""
        if self.alive():
            self.popen.send_signal(signal.SIGSTOP)

    def resume(self):
        if self.alive():
            self.popen.send_signal(signal.SIGCONT)

    def tail(self, n: int = 40) -> str:
        try:
            with open(self.log_path) as f:
                return ''.join(f.readlines()[-n:])
        except OSError:
            return ''

    def __repr__(self):
        return f'<{self.kind} {self.name} :{self.port} {"up" if self.alive() else "down"}>'


class LocalCluster:
    def __init__(self, masters: int = 3, chunkservers: int = 3,
                 root: Optional[str] = None, clean: bool = True,
                 log_level: str = 'INFO', env: Optional[Dict[str, str]] = None):
        self.n_masters = masters
        self.n_chunkservers = chunkservers
        self.log_level = log_level
        self.extra_env = env or {}
        self.root = root or tempfile.mkdtemp(prefix='gfs-cluster-')
        self._owns_root = root is None
        if clean and os.path.isdir(self.root):
            shutil.rmtree(self.root, ignore_errors=True)
        os.makedirs(self.root, exist_ok=True)

        self.masters: List[Proc] = []
        self.chunkservers: List[Proc] = []
        self._started = False
        atexit.register(self.stop)

    # ---------------------------------------------------------------- env
    def _env(self) -> Dict[str, str]:
        e = dict(os.environ)
        e['PYTHONUNBUFFERED'] = '1'
        e.update(self.extra_env)
        return e

    # ------------------------------------------------------------- launch
    def _spawn(self, name: str, kind: str, port: int, argv: List[str],
               data_dir: str) -> Proc:
        os.makedirs(data_dir, exist_ok=True)
        log_path = os.path.join(data_dir, f'{name}.out')
        log = open(log_path, 'ab', buffering=0)
        popen = subprocess.Popen(
            argv, cwd=HERE, env=self._env(),
            stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True,   # so SIGKILL hits only this process
        )
        return Proc(name, kind, port, popen, data_dir, argv, log_path)

    def start(self, wait_for_leader: bool = True, timeout: float = 60.0):
        if self._started:
            return self
        master_ports = [free_port() for _ in range(self.n_masters)]
        addrs = [f'127.0.0.1:{p}' for p in master_ports]

        for i, port in enumerate(master_ports):
            name = f'master{i}'
            data_dir = os.path.join(self.root, name)
            peers = [a for a in addrs if a != f'127.0.0.1:{port}']
            argv = [PYTHON, 'master.py',
                    '--master-id', name,
                    '--port', str(port),
                    '--data-dir', data_dir,
                    '--log-level', self.log_level]
            if peers:
                argv += ['--peers'] + peers
            self.masters.append(self._spawn(name, 'master', port, argv, data_dir))

        self._await_ports([m.port for m in self.masters], timeout=20.0)

        for i in range(self.n_chunkservers):
            port = free_port()
            name = f'cs{i}'
            data_dir = os.path.join(self.root, name)
            argv = [PYTHON, 'chunk_server.py',
                    '--server-id', name,
                    '--port', str(port),
                    '--storage-dir', data_dir,
                    '--log-level', self.log_level,
                    '--masters'] + addrs
            self.chunkservers.append(
                self._spawn(name, 'chunkserver', port, argv, data_dir))

        self._await_ports([c.port for c in self.chunkservers], timeout=30.0)
        self._started = True

        if wait_for_leader:
            self.await_leader(timeout=timeout)
        return self

    def _await_ports(self, ports: List[int], timeout: float):
        deadline = time.time() + timeout
        pending = set(ports)
        while pending and time.time() < deadline:
            for p in list(pending):
                if port_open('127.0.0.1', p):
                    pending.discard(p)
            if pending:
                time.sleep(0.1)
        if pending:
            raise RuntimeError(f'ports never opened: {sorted(pending)}\n{self.diagnostics()}')

    # ------------------------------------------------------------- client
    @property
    def master_addresses(self) -> List[str]:
        return [m.address for m in self.masters if m.alive()]

    @property
    def all_master_addresses(self) -> List[str]:
        """Including dead ones -- a client should survive being handed a
        stale address list, and the tests rely on that."""
        return [m.address for m in self.masters]

    def client(self):
        from client import MultiMasterGFSClient
        return MultiMasterGFSClient(self.all_master_addresses)

    def await_leader(self, timeout: float = 60.0):
        """Block until some master reports itself leader AND every live chunk
        server has registered with it. Registration matters: a leader with no
        chunk servers cannot allocate, so returning early just moves the
        failure into the caller."""
        import grpc
        import gfs_pb2
        import gfs_pb2_grpc
        from gfs_config import GRPC_CHANNEL_OPTIONS

        deadline = time.time() + timeout
        want_cs = sum(1 for c in self.chunkservers if c.alive())
        last = 'no attempt made'
        while time.time() < deadline:
            for m in self.masters:
                if not m.alive():
                    continue
                try:
                    with grpc.insecure_channel(m.address, options=GRPC_CHANNEL_OPTIONS) as ch:
                        stub = gfs_pb2_grpc.MasterServiceStub(ch)
                        resp = stub.GetPrimaryMaster(
                            gfs_pb2.GetPrimaryMasterRequest(requester_id='cluster.py'),
                            timeout=2.0)
                        if not resp.success:
                            last = f'{m.name}: no leader yet'
                            continue
                        leader = resp.primary_master.address + ':' + str(resp.primary_master.port)
                        n = self.status(leader).get('live_chunkservers', 0)
                        if n >= want_cs:
                            self.leader_address = leader
                            return leader
                        last = f'leader {leader} sees {n}/{want_cs} chunkservers'
                except grpc.RpcError as e:
                    last = f'{m.name}: {e.code()}'
            time.sleep(0.25)
        raise RuntimeError(f'no leader within {timeout}s ({last})\n{self.diagnostics()}')

    def status(self, address: Optional[str] = None) -> Dict:
        """Replication health from the master's own view.

        An earlier version of this probed by allocating a scratch chunk and
        counting the locations returned -- which silently capped at the
        replication factor, so a 4-node cluster could never report more than
        3 and the wait never finished. GetClusterStatus reports it directly.
        """
        import grpc
        import gfs_pb2
        import gfs_pb2_grpc
        from gfs_config import GRPC_CHANNEL_OPTIONS

        addr = address or getattr(self, 'leader_address', None)
        candidates = [addr] if addr else []
        candidates += [m.address for m in self.masters if m.alive()]

        for cand in candidates:
            if not cand:
                continue
            try:
                with grpc.insecure_channel(cand, options=GRPC_CHANNEL_OPTIONS) as ch:
                    stub = gfs_pb2_grpc.MasterServiceStub(ch)
                    r = stub.GetClusterStatus(
                        gfs_pb2.GetClusterStatusRequest(), timeout=3.0)
                    if not r.success:
                        continue
                    return {
                        'is_leader': r.is_leader,
                        'leader_id': r.leader_id,
                        'term': r.term,
                        'metadata_version': r.metadata_version,
                        'total_files': r.total_files,
                        'total_chunks': r.total_chunks,
                        'under_replicated': r.under_replicated_chunks,
                        'unavailable': r.unavailable_chunks,
                        'clones_in_flight': r.clones_in_flight,
                        'pending_deletions': r.pending_deletions,
                        'ec_chunks': r.ec_chunks,
                        'replicated_chunks': r.replicated_chunks,
                        'ec_chunks_degraded': r.ec_chunks_degraded,
                        'ec_encodes_in_flight': r.ec_encodes_in_flight,
                        'logical_bytes': r.logical_bytes,
                        'physical_bytes': r.physical_bytes,
                        'live_chunkservers': sum(1 for c in r.chunk_servers if c.alive),
                        'chunk_servers': [
                            {'id': c.server_id, 'alive': c.alive,
                             'chunks': c.total_chunks,
                             'heartbeat_age': c.heartbeat_age_seconds}
                            for c in r.chunk_servers
                        ],
                    }
            except grpc.RpcError:
                continue
        return {}

    def await_condition(self, predicate, timeout: float, interval: float = 0.5,
                        describe=None):
        """Poll `predicate(status_dict)` until true. Returns elapsed seconds,
        or raises with the last status seen -- which is what makes a failed
        replication test say *why* it failed."""
        start = time.time()
        last = {}
        while time.time() - start < timeout:
            last = self.status()
            if last and predicate(last):
                return time.time() - start
            time.sleep(interval)
        detail = describe(last) if describe else last
        raise RuntimeError(f'condition not met within {timeout}s; last status: {detail}')

    # --------------------------------------------------------------- fault
    def leader(self) -> Optional[Proc]:
        addr = getattr(self, 'leader_address', None)
        if not addr:
            try:
                addr = self.await_leader(timeout=15)
            except RuntimeError:
                return None
        # Match on port, not the whole string: the master reports itself as
        # whatever hostname it was configured with ('localhost:9000'), which
        # need not equal the '127.0.0.1:9000' we spawned it as.
        try:
            port = int(str(addr).rsplit(':', 1)[1])
        except (IndexError, ValueError):
            return None
        for m in self.masters:
            if m.port == port:
                return m
        return None

    def kill_leader(self) -> Optional[Proc]:
        m = self.leader()
        if m:
            m.kill()
            self.leader_address = None
        return m

    def restart(self, proc: Proc, wait: bool = True):
        """Relaunch a killed process with its original argv and data dir --
        this is what exercises oplog recovery."""
        proc.kill()
        log = open(proc.log_path, 'ab', buffering=0)
        proc.popen = subprocess.Popen(
            proc.argv, cwd=HERE, env=self._env(),
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        if wait:
            self._await_ports([proc.port], timeout=30.0)
        return proc

    # --------------------------------------------------------------- admin
    def diagnostics(self, lines: int = 25) -> str:
        out = ['--- cluster diagnostics ---', f'root: {self.root}']
        for p in self.masters + self.chunkservers:
            out.append(f'\n=== {p!r}')
            out.append(p.tail(lines))
        return '\n'.join(out)

    def stop(self, await_ports: bool = True):
        """Tear down and, by default, do not return until the ports are free.

        Returning early makes tests flaky in a way that is painful to debug:
        the next cluster calls free_port(), the OS hands back a port a dying
        process still holds, and that component silently fails to bind. The
        symptom shows up several tests later as an unrelated timeout.
        """
        ports = [p.port for p in self.chunkservers + self.masters]
        for p in self.chunkservers + self.masters:
            try:
                p.resume()   # a paused process ignores SIGKILL until resumed
                p.kill()
            except Exception:
                pass
        self.chunkservers, self.masters = [], []
        self._started = False

        if await_ports and ports:
            deadline = time.time() + 15.0
            while time.time() < deadline:
                if not any(port_open('127.0.0.1', p, timeout=0.05) for p in ports):
                    break
                time.sleep(0.1)

    def destroy(self):
        self.stop()
        if self._owns_root:
            shutil.rmtree(self.root, ignore_errors=True)

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.destroy()
        return False


def main():
    import argparse
    ap = argparse.ArgumentParser(description='Run a local GFS cluster')
    ap.add_argument('--masters', type=int, default=3)
    ap.add_argument('--chunkservers', type=int, default=3)
    ap.add_argument('--root', type=str, default=None)
    ap.add_argument('--log-level', type=str, default='INFO')
    args = ap.parse_args()

    c = LocalCluster(masters=args.masters, chunkservers=args.chunkservers,
                     root=args.root, log_level=args.log_level)
    c.start()
    print(f'cluster up  root={c.root}')
    print(f'leader      {c.leader_address}')
    print(f'masters     {" ".join(c.all_master_addresses)}')
    print(f'chunkserv   {" ".join(p.address for p in c.chunkservers)}')
    print('\nctrl-c to tear down')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nstopping')
        c.stop()


if __name__ == '__main__':
    main()

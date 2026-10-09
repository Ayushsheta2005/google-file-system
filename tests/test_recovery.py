#!/usr/bin/env python3
"""Master recovery tests.

The operation log records the *effect* of each mutation and replay re-applies
those effects, which means there are two independent code paths producing the
same state: the live handler, and `apply_record`. If they ever disagree, the
master silently resurrects or loses files after a restart and nothing else
catches it.

So the central test here is an equivalence test: run operations live, snapshot
the state, throw the process away, rebuild from checkpoint + log, and assert
the two snapshots are identical.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import master as M  # noqa: E402
from oplog import OperationLog  # noqa: E402


def make_master(tmp_path, master_id='m0'):
    return M.MultiMasterGFS(
        master_id=master_id, port=0,
        metadata_file=str(tmp_path / 'metadata.json'),
        peers=[])


def comparable(m):
    """State a restart must preserve.

    Replica locations are deliberately excluded: they are rebuilt from chunk
    server heartbeats, not from the log, so they are legitimately empty right
    after recovery. See ADR-001.
    """
    return {
        'files': {
            name: (f.size, list(f.chunks))
            for name, f in sorted(m.files.items())
        },
        'directories': sorted(m.directories.keys()),
        'chunks': {
            h: (c.version, c.size)
            for h, c in sorted(m.chunks.items())
        },
    }


def restart(m, tmp_path):
    """Simulate a crash: drop the object, rebuild from disk only."""
    if m.oplog:
        m.oplog.close()
    return make_master(tmp_path, master_id=m.master_id)


# ------------------------------------------------------------------ basics
def test_empty_master_has_root_directory(tmp_path):
    m = make_master(tmp_path)
    assert '/' in m.directories
    assert m.durable, 'oplog mode should be the default'


def test_created_file_survives_restart(tmp_path):
    m = make_master(tmp_path)
    m.files['/a.txt'] = M.FileMetadata('/a.txt', [], 0, 1.0, 1.0)
    m.commit(M.OP_CREATE_FILE, filename='/a.txt', created_time=1.0)

    m2 = restart(m, tmp_path)
    assert '/a.txt' in m2.files


def test_deleted_file_stays_deleted_after_restart(tmp_path):
    """The failure that matters most: a delete that does not survive makes a
    file reappear, and clients then read chunks nothing owns."""
    m = make_master(tmp_path)
    m.files['/gone.txt'] = M.FileMetadata('/gone.txt', [], 0, 1.0, 1.0)
    m.commit(M.OP_CREATE_FILE, filename='/gone.txt', created_time=1.0)
    del m.files['/gone.txt']
    m.commit(M.OP_DELETE_FILE, filenames=['/gone.txt'])

    m2 = restart(m, tmp_path)
    assert '/gone.txt' not in m2.files


def test_chunk_allocation_survives_restart(tmp_path):
    m = make_master(tmp_path)
    m.files['/big.bin'] = M.FileMetadata('/big.bin', [], 0, 1.0, 1.0)
    m.commit(M.OP_CREATE_FILE, filename='/big.bin', created_time=1.0)

    for idx in range(3):
        handle = f'chunk_{idx}'
        m.chunks[handle] = M.ChunkMetadata(handle=handle, version=1,
                                           locations={'cs0', 'cs1'})
        m.files['/big.bin'].chunks.append(handle)
        m.commit(M.OP_ALLOCATE_CHUNK, filename='/big.bin', chunk_index=idx,
                 chunk_handle=handle, chunk_version=1)

    m2 = restart(m, tmp_path)
    assert m2.files['/big.bin'].chunks == ['chunk_0', 'chunk_1', 'chunk_2']
    assert set(m2.chunks) == {'chunk_0', 'chunk_1', 'chunk_2'}


def test_locations_are_not_restored_from_the_log(tmp_path):
    """Locations come from heartbeats, by design. Asserting they are empty
    pins that decision down so nobody 'fixes' it by logging them."""
    m = make_master(tmp_path)
    m.chunks['c1'] = M.ChunkMetadata(handle='c1', version=1,
                                     locations={'cs0', 'cs1', 'cs2'})
    m.files['/f'] = M.FileMetadata('/f', ['c1'], 0, 1.0, 1.0)
    m.commit(M.OP_CREATE_FILE, filename='/f', created_time=1.0)
    m.commit(M.OP_ALLOCATE_CHUNK, filename='/f', chunk_index=0,
             chunk_handle='c1', chunk_version=1)

    m2 = restart(m, tmp_path)
    assert m2.chunks['c1'].locations == set()


def test_chunk_size_and_derived_file_size_survive(tmp_path):
    m = make_master(tmp_path)
    m.files['/f'] = M.FileMetadata('/f', ['c1'], 0, 1.0, 1.0)
    m.chunks['c1'] = M.ChunkMetadata(handle='c1', version=1, locations=set())
    m.commit(M.OP_CREATE_FILE, filename='/f', created_time=1.0)
    m.commit(M.OP_ALLOCATE_CHUNK, filename='/f', chunk_index=0,
             chunk_handle='c1', chunk_version=1)

    m.chunks['c1'].size = 4096
    m.files['/f'].size = 4096
    m.commit(M.OP_UPDATE_CHUNK_SIZE, chunk_handle='c1', size=4096,
             filename='/f', file_size=4096)

    m2 = restart(m, tmp_path)
    assert m2.chunks['c1'].size == 4096
    assert m2.files['/f'].size == 4096, 'file size must not reset to 0'


def test_chunk_version_bump_survives(tmp_path):
    """Version is how stale replicas are detected. If a restart rolls it
    back, the master starts trusting replicas it already invalidated."""
    m = make_master(tmp_path)
    m.chunks['c1'] = M.ChunkMetadata(handle='c1', version=1, locations=set())
    m.files['/f'] = M.FileMetadata('/f', ['c1'], 0, 1.0, 1.0)
    m.commit(M.OP_CREATE_FILE, filename='/f', created_time=1.0)
    m.commit(M.OP_ALLOCATE_CHUNK, filename='/f', chunk_index=0,
             chunk_handle='c1', chunk_version=1)
    for v in (2, 3, 4):
        m.chunks['c1'].version = v
        m.commit(M.OP_BUMP_CHUNK_VERSION, chunk_handle='c1', chunk_version=v)

    m2 = restart(m, tmp_path)
    assert m2.chunks['c1'].version == 4


# ------------------------------------------------- directories and moves
def test_directory_tree_survives_restart(tmp_path):
    m = make_master(tmp_path)
    assert m.create_directory('/a/b/c')
    before = sorted(m.directories.keys())

    m2 = restart(m, tmp_path)
    assert sorted(m2.directories.keys()) == before


def test_recursive_directory_delete_survives(tmp_path):
    m = make_master(tmp_path)
    m.create_directory('/data/logs')
    for name in ('/data/logs/a', '/data/logs/b'):
        m.files[name] = M.FileMetadata(name, [], 0, 1.0, 1.0)
        m.commit(M.OP_CREATE_FILE, filename=name, created_time=1.0)

    ok, _, _ = m.delete_directory('/data', recursive=True)
    assert ok

    m2 = restart(m, tmp_path)
    assert not [d for d in m2.directories if d.startswith('/data')]
    assert not [f for f in m2.files if f.startswith('/data')]


def test_file_move_survives_restart(tmp_path):
    m = make_master(tmp_path)
    m.files['/old.txt'] = M.FileMetadata('/old.txt', ['c1'], 10, 1.0, 1.0)
    m.commit(M.OP_CREATE_FILE, filename='/old.txt', created_time=1.0)
    ok, _ = m.move_path('/old.txt', '/new.txt')
    assert ok

    m2 = restart(m, tmp_path)
    assert '/new.txt' in m2.files and '/old.txt' not in m2.files


def test_directory_move_takes_its_contents(tmp_path):
    m = make_master(tmp_path)
    m.create_directory('/src/inner')
    m.files['/src/inner/f.txt'] = M.FileMetadata('/src/inner/f.txt', [], 0, 1.0, 1.0)
    m.commit(M.OP_CREATE_FILE, filename='/src/inner/f.txt', created_time=1.0)

    ok, _ = m.move_path('/src', '/dst')
    assert ok

    m2 = restart(m, tmp_path)
    assert '/dst/inner/f.txt' in m2.files
    assert '/src/inner/f.txt' not in m2.files
    assert '/dst/inner/' in m2.directories


# ------------------------------------------------------ the equivalence test
def build_workload(m):
    """A mixed workload touching every logged op."""
    m.create_directory('/w/sub')
    for i in range(12):
        name = f'/w/file{i}.bin'
        m.files[name] = M.FileMetadata(name, [], 0, 100.0 + i, 100.0 + i)
        m.commit(M.OP_CREATE_FILE, filename=name, created_time=100.0 + i)

        for idx in range(2):
            handle = f'chunk_{i}_{idx}'
            m.chunks[handle] = M.ChunkMetadata(handle=handle, version=1,
                                               locations={'cs0'})
            m.files[name].chunks.append(handle)
            m.commit(M.OP_ALLOCATE_CHUNK, filename=name, chunk_index=idx,
                     chunk_handle=handle, chunk_version=1)

            size = 1024 * (i + 1)
            m.chunks[handle].size = size
            total = sum(m.chunks[h].size for h in m.files[name].chunks if h)
            m.files[name].size = total
            m.commit(M.OP_UPDATE_CHUNK_SIZE, chunk_handle=handle, size=size,
                     filename=name, file_size=total)

    # delete a few, move a few, bump a version
    for i in (1, 5, 9):
        name = f'/w/file{i}.bin'
        for h in m.files[name].chunks:
            m.chunks.pop(h, None)
        del m.files[name]
        m.commit(M.OP_DELETE_FILE, filenames=[name])

    m.move_path('/w/file2.bin', '/w/renamed.bin')
    m.move_path('/w/sub', '/w/sub2')

    m.chunks['chunk_0_0'].version = 7
    m.commit(M.OP_BUMP_CHUNK_VERSION, chunk_handle='chunk_0_0', chunk_version=7)


def test_replay_reproduces_live_state_exactly(tmp_path):
    """The equivalence test. Two code paths build this state -- the live
    handlers and apply_record -- and they must not drift apart."""
    m = make_master(tmp_path)
    build_workload(m)
    live = comparable(m)

    m2 = restart(m, tmp_path)
    recovered = comparable(m2)

    assert recovered == live


def test_replay_is_idempotent_across_repeated_restarts(tmp_path):
    """Recovering twice must not double-apply anything."""
    m = make_master(tmp_path)
    build_workload(m)
    expected = comparable(m)

    current = m
    for _ in range(3):
        current = restart(current, tmp_path)
        assert comparable(current) == expected


# ----------------------------------------------------------- checkpointing
def test_checkpoint_then_restart_matches(tmp_path):
    m = make_master(tmp_path)
    build_workload(m)
    expected = comparable(m)

    assert m.write_checkpoint()
    assert m.oplog.records_since_checkpoint == 0

    m2 = restart(m, tmp_path)
    assert comparable(m2) == expected


def test_operations_after_a_checkpoint_are_also_recovered(tmp_path):
    """Checkpoint, then write more, then crash -- both halves must come back."""
    m = make_master(tmp_path)
    build_workload(m)
    m.write_checkpoint()

    m.files['/after_ckpt.txt'] = M.FileMetadata('/after_ckpt.txt', [], 0, 1.0, 1.0)
    m.commit(M.OP_CREATE_FILE, filename='/after_ckpt.txt', created_time=1.0)
    expected = comparable(m)

    m2 = restart(m, tmp_path)
    assert comparable(m2) == expected
    assert '/after_ckpt.txt' in m2.files
    assert '/w/file0.bin' in m2.files


def test_torn_log_tail_does_not_block_startup(tmp_path):
    """A crash mid-append must cost the one unacknowledged record, not the
    master's ability to boot."""
    m = make_master(tmp_path)
    build_workload(m)
    m.files['/last.txt'] = M.FileMetadata('/last.txt', [], 0, 1.0, 1.0)
    m.commit(M.OP_CREATE_FILE, filename='/last.txt', created_time=1.0)
    m.oplog.close()

    logpath = os.path.join(str(tmp_path), 'oplog.bin')
    with open(logpath, 'r+b') as f:
        f.truncate(os.path.getsize(logpath) - 7)

    m2 = make_master(tmp_path)
    assert '/w/file0.bin' in m2.files         # everything before survives
    assert '/last.txt' not in m2.files        # the torn record does not


def test_appends_continue_cleanly_after_a_torn_tail(tmp_path):
    m = make_master(tmp_path)
    m.files['/a'] = M.FileMetadata('/a', [], 0, 1.0, 1.0)
    m.commit(M.OP_CREATE_FILE, filename='/a', created_time=1.0)
    m.oplog.close()

    logpath = os.path.join(str(tmp_path), 'oplog.bin')
    with open(logpath, 'ab') as f:
        f.write(b'\x99\x99\x99')     # partial header from a crash

    m2 = make_master(tmp_path)
    m2.files['/b'] = M.FileMetadata('/b', [], 0, 1.0, 1.0)
    m2.commit(M.OP_CREATE_FILE, filename='/b', created_time=1.0)

    m3 = restart(m2, tmp_path)
    assert '/a' in m3.files and '/b' in m3.files


# ------------------------------------------------------------- snapshot mode
def test_snapshot_mode_still_works(tmp_path, monkeypatch):
    """Legacy mode has to keep working: the benchmark suite runs both to
    show what the operation log actually bought."""
    monkeypatch.setattr(M, 'OPLOG_MODE', 'snapshot')
    m = make_master(tmp_path)
    assert not m.durable
    m.files['/s.txt'] = M.FileMetadata('/s.txt', [], 0, 1.0, 1.0)
    m.commit(M.OP_CREATE_FILE, filename='/s.txt', created_time=1.0)

    m2 = make_master(tmp_path)
    assert '/s.txt' in m2.files

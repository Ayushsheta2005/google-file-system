#!/usr/bin/env python3
"""Unit tests for the operation log and checkpoint store.

The interesting cases are all failure cases: a crash halfway through an
append, a corrupt checkpoint, concurrent appenders sharing an fsync. Those are
what the framing and the atomic rename exist for, so those are what get tested.
"""

import json
import os
import struct
import sys
import threading
import zlib

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from oplog import (  # noqa: E402
    CheckpointStore,
    CorruptRecord,
    OperationLog,
    OP_CREATE_FILE,
    OP_ALLOCATE_CHUNK,
    _HEADER,
)


@pytest.fixture
def logpath(tmp_path):
    return str(tmp_path / 'oplog.bin')


# ---------------------------------------------------------------- basics
def test_append_and_replay_roundtrip(logpath):
    log = OperationLog(logpath, group_commit_ms=0)
    for i in range(50):
        log.append(OP_CREATE_FILE, filename=f'/f{i}')
    log.close()

    records = list(OperationLog(logpath).replay())
    assert len(records) == 50
    assert [r['filename'] for r in records] == [f'/f{i}' for i in range(50)]
    assert all(r['op'] == OP_CREATE_FILE for r in records)


def test_append_returns_monotonic_sequence(logpath):
    log = OperationLog(logpath, group_commit_ms=0)
    seqs = [log.append(OP_CREATE_FILE, filename=f'/f{i}') for i in range(10)]
    assert seqs == list(range(1, 11))
    log.close()


def test_rejects_unknown_op(logpath):
    log = OperationLog(logpath, group_commit_ms=0)
    with pytest.raises(ValueError):
        log.append('not_a_real_op', filename='/x')
    log.close()


def test_data_survives_reopen(logpath):
    log = OperationLog(logpath, group_commit_ms=0)
    log.append(OP_CREATE_FILE, filename='/first')
    log.close()

    log2 = OperationLog(logpath, group_commit_ms=0)
    log2.append(OP_CREATE_FILE, filename='/second')
    log2.close()

    names = [r['filename'] for r in OperationLog(logpath).replay()]
    assert names == ['/first', '/second']


def test_fsync_happens_before_append_returns(logpath):
    """The guarantee the whole design rests on: once append() returns, a
    separate reader opening the file sees the record. If this regresses,
    'zero acknowledged-write loss' becomes false."""
    log = OperationLog(logpath, group_commit_ms=0)
    log.append(OP_CREATE_FILE, filename='/durable')
    assert [r['filename'] for r in OperationLog(logpath).replay()] == ['/durable']
    log.close()


# ----------------------------------------------------------- torn tails
def _truncate_by(path, nbytes):
    size = os.path.getsize(path)
    with open(path, 'r+b') as f:
        f.truncate(size - nbytes)


def test_torn_tail_mid_payload_is_tolerated(logpath):
    """Crash while writing the payload: keep every complete record before it."""
    log = OperationLog(logpath, group_commit_ms=0)
    for i in range(5):
        log.append(OP_CREATE_FILE, filename=f'/f{i}')
    log.close()

    _truncate_by(logpath, 10)   # chop into the last record's payload

    records = list(OperationLog(logpath).replay(tolerate_tail=True))
    assert [r['filename'] for r in records] == ['/f0', '/f1', '/f2', '/f3']


def test_torn_tail_mid_header_is_tolerated(logpath):
    log = OperationLog(logpath, group_commit_ms=0)
    for i in range(3):
        log.append(OP_CREATE_FILE, filename=f'/f{i}')
    log.close()

    with open(logpath, 'ab') as f:      # 3 bytes of an 8-byte header
        f.write(b'\x01\x02\x03')

    records = list(OperationLog(logpath).replay(tolerate_tail=True))
    assert len(records) == 3


def test_strict_replay_raises_on_torn_tail(logpath):
    log = OperationLog(logpath, group_commit_ms=0)
    log.append(OP_CREATE_FILE, filename='/f0')
    log.close()
    _truncate_by(logpath, 5)

    with pytest.raises(CorruptRecord):
        list(OperationLog(logpath).replay(tolerate_tail=False))


def test_corruption_in_the_middle_is_not_silently_swallowed(logpath):
    """A bad CRC at the tail is a crash. The same damage mid-file is real
    corruption and must be loud, not quietly truncate the namespace."""
    log = OperationLog(logpath, group_commit_ms=0)
    for i in range(5):
        log.append(OP_CREATE_FILE, filename=f'/f{i}')
    log.close()

    # Flip a byte inside the first record's payload.
    with open(logpath, 'r+b') as f:
        f.seek(_HEADER.size + 4)
        b = f.read(1)
        f.seek(_HEADER.size + 4)
        f.write(bytes([b[0] ^ 0xFF]))

    with pytest.raises(CorruptRecord):
        list(OperationLog(logpath).replay(tolerate_tail=True))


def test_truncate_tail_lets_appends_continue(logpath):
    """Without truncation the next append lands after a torn frame and every
    later replay stops there, silently losing everything written after."""
    log = OperationLog(logpath, group_commit_ms=0)
    for i in range(3):
        log.append(OP_CREATE_FILE, filename=f'/f{i}')
    log.close()
    _truncate_by(logpath, 6)

    log2 = OperationLog(logpath, group_commit_ms=0)
    discarded = log2.truncate_tail()
    assert discarded > 0
    log2.append(OP_CREATE_FILE, filename='/after_recovery')
    log2.close()

    names = [r['filename'] for r in OperationLog(logpath).replay()]
    assert names == ['/f0', '/f1', '/after_recovery']


def test_truncate_tail_is_a_noop_on_a_clean_log(logpath):
    log = OperationLog(logpath, group_commit_ms=0)
    for i in range(4):
        log.append(OP_CREATE_FILE, filename=f'/f{i}')
    log.close()

    log2 = OperationLog(logpath)
    assert log2.truncate_tail() == 0
    log2.close()
    assert len(list(OperationLog(logpath).replay())) == 4


# --------------------------------------------------------- group commit
def test_group_commit_batches_concurrent_appends(logpath):
    """24 threads appending under a 20 ms window should share far fewer
    fsyncs than one each -- that ratio is the whole justification for the
    added latency."""
    log = OperationLog(logpath, group_commit_ms=20)
    errors = []

    def worker(i):
        try:
            for j in range(5):
                log.append(OP_CREATE_FILE, filename=f'/t{i}_{j}')
        except Exception as e:        # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(24)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors
    assert log.stats['appends'] == 120
    assert log.stats['fsyncs'] < 120, 'group commit did not batch anything'
    log.close()

    assert len(list(OperationLog(logpath).replay())) == 120


def test_concurrent_appends_are_all_durable_and_intact(logpath):
    """Batching must not lose or interleave records."""
    log = OperationLog(logpath, group_commit_ms=5)

    def worker(i):
        for j in range(10):
            log.append(OP_ALLOCATE_CHUNK, filename=f'/t{i}', chunk_index=j,
                       chunk_handle=f'chunk_{i}_{j}')

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    log.close()

    records = list(OperationLog(logpath).replay())
    assert len(records) == 80
    assert len({r['chunk_handle'] for r in records}) == 80


def test_zero_window_means_one_fsync_each(logpath):
    log = OperationLog(logpath, group_commit_ms=0)
    for i in range(10):
        log.append(OP_CREATE_FILE, filename=f'/f{i}')
    assert log.stats['fsyncs'] == 10
    log.close()


# ----------------------------------------------------------- checkpoints
def test_checkpoint_write_and_read_back(tmp_path):
    store = CheckpointStore(str(tmp_path), keep=2)
    payload = {'files': {'/a': {'size': 1}}, 'version': 7}
    store.write(1, payload)
    got = store.latest()
    assert got is not None
    number, data = got
    assert number == 1
    assert data == payload


def test_latest_picks_the_highest_number(tmp_path):
    store = CheckpointStore(str(tmp_path), keep=5)
    for n in (1, 2, 3):
        store.write(n, {'version': n})
    number, data = store.latest()
    assert (number, data['version']) == (3, 3)


def test_no_checkpoints_returns_none(tmp_path):
    assert CheckpointStore(str(tmp_path)).latest() is None


def test_prune_keeps_only_the_configured_count(tmp_path):
    store = CheckpointStore(str(tmp_path), keep=2)
    for n in range(1, 6):
        store.write(n, {'version': n})
    assert [n for n, _ in store.list_checkpoints()] == [4, 5]


def test_corrupt_newest_checkpoint_falls_back_to_previous(tmp_path):
    """Exactly why keep > 1. A corrupt newest checkpoint should cost us the
    delta since the previous one, not the entire namespace."""
    store = CheckpointStore(str(tmp_path), keep=3)
    store.write(1, {'version': 1})
    path2 = store.write(2, {'version': 2})

    with open(path2, 'r+b') as f:
        f.seek(8)
        f.write(b'\xff\xff\xff\xff')

    number, data = store.latest()
    assert (number, data['version']) == (1, 1)


def test_no_partial_checkpoint_is_ever_visible(tmp_path):
    """Atomic rename: a .tmp left behind by a crash must not be picked up."""
    store = CheckpointStore(str(tmp_path), keep=2)
    store.write(1, {'version': 1})
    with open(os.path.join(str(tmp_path), 'checkpoint.000000000002.json.tmp'), 'wb') as f:
        f.write(b'garbage that never finished writing')

    number, data = store.latest()
    assert (number, data['version']) == (1, 1)


# ------------------------------------------------- checkpoint + log together
def test_reset_after_checkpoint_bounds_replay(tmp_path):
    """The point of checkpointing: recovery replays the tail, not all history."""
    logp = str(tmp_path / 'oplog.bin')
    log = OperationLog(logp, group_commit_ms=0)
    store = CheckpointStore(str(tmp_path), keep=2)

    for i in range(100):
        log.append(OP_CREATE_FILE, filename=f'/f{i}')
    assert log.records_since_checkpoint == 100

    store.write(1, {'files': [f'/f{i}' for i in range(100)]})
    log.reset()
    assert log.records_since_checkpoint == 0

    log.append(OP_CREATE_FILE, filename='/after')
    log.close()

    tail = list(OperationLog(logp).replay())
    assert [r['filename'] for r in tail] == ['/after']
    _, snap = store.latest()
    assert len(snap['files']) == 100

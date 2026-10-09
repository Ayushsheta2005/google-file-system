#!/usr/bin/env python3
"""Operation log and checkpoints for master metadata.

The original master persisted by dumping its entire metadata to JSON from a
background loop every 30 seconds, which means a crash loses up to 30 seconds of
acknowledged namespace operations and the cost of one `CreateFile` grows with
the size of the whole namespace.

This is the GFS paper's answer instead (§2.6.3): an append-only operation log
that is flushed and fsync'd *before* the master replies to a client, plus
periodic checkpoints so replay stays bounded. Recovery is "load newest valid
checkpoint, replay the log tail".

Record framing on disk:

    u32 payload_len | u32 crc32(payload) | payload bytes

The length-and-checksum frame is what makes a torn tail detectable. A crash
mid-write leaves a short read or a bad CRC; recovery truncates there and keeps
everything before it, rather than refusing to start. See ADR-001.
"""

import json
import os
import struct
import threading
import time
import zlib
from typing import Callable, Dict, Iterator, List, Optional, Tuple

_HEADER = struct.Struct('<II')      # payload_len, crc32
_HEADER_SIZE = _HEADER.size


# --------------------------------------------------------------------------
# Record types
# --------------------------------------------------------------------------
# Only namespace and chunk-identity facts are logged. Replica *locations* are
# deliberately NOT logged: the paper rebuilds them from chunk server heartbeats
# at startup, because the chunk servers are the authority on what they actually
# hold and a logged location is a lie the moment a disk dies. See ADR-001.

OP_CREATE_FILE = 'create_file'
OP_DELETE_FILE = 'delete_file'
OP_CREATE_DIR = 'create_dir'
OP_DELETE_DIR = 'delete_dir'
OP_MOVE = 'move'
OP_ALLOCATE_CHUNK = 'allocate_chunk'
OP_UPDATE_CHUNK_SIZE = 'update_chunk_size'
OP_UPDATE_FILE_SIZE = 'update_file_size'
OP_BUMP_CHUNK_VERSION = 'bump_chunk_version'

ALL_OPS = {
    OP_CREATE_FILE, OP_DELETE_FILE, OP_CREATE_DIR, OP_DELETE_DIR, OP_MOVE,
    OP_ALLOCATE_CHUNK, OP_UPDATE_CHUNK_SIZE, OP_UPDATE_FILE_SIZE,
    OP_BUMP_CHUNK_VERSION,
}


class CorruptRecord(Exception):
    pass


# --------------------------------------------------------------------------
# Append-only log
# --------------------------------------------------------------------------
class OperationLog:
    """Append-only, fsync-on-commit, with group commit.

    Group commit matters more than it looks. An fsync on this machine costs
    on the order of a millisecond, so one fsync per mutation caps the master at
    roughly a thousand namespace ops/sec *regardless* of how many threads are
    calling. Batching concurrent appends into a single fsync lifts that ceiling
    by about the batch size, at the cost of up to `group_commit_ms` of extra
    latency on an otherwise-idle master. See ADR-002.
    """

    def __init__(self, path: str, group_commit_ms: float = 5.0,
                 logger=None):
        self.path = path
        self.group_commit_s = max(0.0, group_commit_ms / 1000.0)
        self.logger = logger

        os.makedirs(os.path.dirname(os.path.abspath(path)) or '.', exist_ok=True)
        self._f = open(path, 'ab', buffering=0)

        self._lock = threading.Lock()
        self._durable_seq = 0      # records fsync'd
        self._queued_seq = 0       # records written to the file object
        self._commit_cv = threading.Condition(self._lock)
        self._committing = False

        self.records_since_checkpoint = self._count_records()

        # Observability: these go straight into the benchmark output.
        self.stats = {'appends': 0, 'fsyncs': 0, 'bytes': 0}

    # ------------------------------------------------------------- writing
    def append(self, op: str, **fields) -> int:
        """Write one record and do not return until it is on disk.

        Returns the record's sequence number. Callers must not reply to a
        client before this returns -- that ordering *is* the durability
        guarantee.
        """
        if op not in ALL_OPS:
            raise ValueError(f'unknown op {op!r}')
        payload = json.dumps({'op': op, 'ts': time.time(), **fields},
                             separators=(',', ':')).encode()
        frame = _HEADER.pack(len(payload), zlib.crc32(payload) & 0xFFFFFFFF) + payload

        with self._lock:
            self._f.write(frame)
            self._queued_seq += 1
            seq = self._queued_seq
            self.records_since_checkpoint += 1
            self.stats['appends'] += 1
            self.stats['bytes'] += len(frame)
            self._await_durable(seq)
        return seq

    def _await_durable(self, seq: int):
        """Called with the lock held. Either performs the group's fsync or
        waits for whoever is already doing one to cover this record."""
        while self._durable_seq < seq:
            if self._committing:
                # Someone else is mid-fsync. Their flush may or may not cover
                # us depending on when we wrote, so just wait and re-check.
                self._commit_cv.wait(timeout=1.0)
                continue

            self._committing = True
            if self.group_commit_s:
                # Hold the lock during the sleep on purpose: writers that
                # arrive in this window queue up behind us and get folded into
                # the same fsync, which is the entire point. The window is
                # single-digit milliseconds.
                self._commit_cv.wait(timeout=self.group_commit_s)

            batch_through = self._queued_seq
            try:
                self._f.flush()
                os.fsync(self._f.fileno())
                self._durable_seq = batch_through
                self.stats['fsyncs'] += 1
            finally:
                self._committing = False
                self._commit_cv.notify_all()

    # ------------------------------------------------------------- reading
    def _count_records(self) -> int:
        return sum(1 for _ in self.replay(tolerate_tail=True))

    def replay(self, tolerate_tail: bool = True) -> Iterator[dict]:
        """Yield every intact record in order.

        A short read or bad CRC at the end is a crash that happened mid-append;
        that is expected, and we stop there. The same damage in the *middle* of
        the file is real corruption and raises.
        """
        if not os.path.exists(self.path):
            return
        size = os.path.getsize(self.path)
        with open(self.path, 'rb') as f:
            while True:
                offset = f.tell()
                header = f.read(_HEADER_SIZE)
                if len(header) < _HEADER_SIZE:
                    if header and not tolerate_tail:
                        raise CorruptRecord(f'short header at {offset}')
                    return
                length, crc = _HEADER.unpack(header)
                payload = f.read(length)
                if len(payload) < length:
                    if not tolerate_tail:
                        raise CorruptRecord(f'short payload at {offset}')
                    return
                if (zlib.crc32(payload) & 0xFFFFFFFF) != crc:
                    at_tail = f.tell() >= size
                    if at_tail and tolerate_tail:
                        return
                    raise CorruptRecord(f'crc mismatch at offset {offset}')
                try:
                    yield json.loads(payload)
                except json.JSONDecodeError as e:
                    raise CorruptRecord(f'bad json at {offset}: {e}')

    def truncate_tail(self) -> int:
        """Cut the file back to the last intact record.

        Run once at startup. Without it the next append lands after a torn
        frame, and every later replay stops at that point -- silently losing
        everything written afterwards.
        """
        good_end = 0
        try:
            with open(self.path, 'rb') as f:
                while True:
                    header = f.read(_HEADER_SIZE)
                    if len(header) < _HEADER_SIZE:
                        break
                    length, crc = _HEADER.unpack(header)
                    payload = f.read(length)
                    if len(payload) < length:
                        break
                    if (zlib.crc32(payload) & 0xFFFFFFFF) != crc:
                        break
                    good_end = f.tell()
        except FileNotFoundError:
            return 0

        actual = os.path.getsize(self.path)
        if good_end < actual:
            with self._lock:
                self._f.close()
                with open(self.path, 'r+b') as f:
                    f.truncate(good_end)
                    f.flush()
                    os.fsync(f.fileno())
                self._f = open(self.path, 'ab', buffering=0)
            if self.logger:
                self.logger.warning(
                    f'oplog: discarded {actual - good_end} bytes of torn tail '
                    f'(crash during append)')
        return actual - good_end

    # -------------------------------------------------------------- rotate
    def reset(self):
        """Empty the log. Only safe immediately after a checkpoint that
        covers everything in it."""
        with self._lock:
            self._f.close()
            with open(self.path, 'wb') as f:
                f.flush()
                os.fsync(f.fileno())
            self._f = open(self.path, 'ab', buffering=0)
            self._durable_seq = 0
            self._queued_seq = 0
            self.records_since_checkpoint = 0

    def close(self):
        with self._lock:
            try:
                self._f.flush()
                os.fsync(self._f.fileno())
            except (OSError, ValueError):
                pass
            try:
                self._f.close()
            except (OSError, ValueError):
                pass


# --------------------------------------------------------------------------
# Checkpoints
# --------------------------------------------------------------------------
class CheckpointStore:
    """Numbered checkpoints written atomically.

    Write to a temp file in the same directory, fsync it, then `os.rename` over
    the target. Rename is atomic within a filesystem, so a reader either sees
    the whole old checkpoint or the whole new one -- never a half-written file.
    The directory itself is fsync'd afterwards, otherwise the rename can still
    be lost on a crash even though the file contents are safe.
    """

    PREFIX = 'checkpoint.'
    SUFFIX = '.json'

    def __init__(self, directory: str, keep: int = 2, logger=None):
        self.dir = directory
        self.keep = max(1, keep)
        self.logger = logger
        os.makedirs(directory, exist_ok=True)

    def _path(self, number: int) -> str:
        return os.path.join(self.dir, f'{self.PREFIX}{number:012d}{self.SUFFIX}')

    def list_checkpoints(self) -> List[Tuple[int, str]]:
        out = []
        for name in os.listdir(self.dir):
            if name.startswith(self.PREFIX) and name.endswith(self.SUFFIX):
                stem = name[len(self.PREFIX):-len(self.SUFFIX)]
                try:
                    out.append((int(stem), os.path.join(self.dir, name)))
                except ValueError:
                    continue
        return sorted(out)

    def latest(self) -> Optional[Tuple[int, dict]]:
        """Newest checkpoint that parses. Walk backwards so a corrupt newest
        checkpoint falls back to the previous one instead of losing the
        namespace -- which is the reason we keep more than one."""
        for number, path in reversed(self.list_checkpoints()):
            try:
                with open(path, 'rb') as f:
                    raw = f.read()
                if len(raw) < 4:
                    raise CorruptRecord('too short')
                stored_crc = struct.unpack('<I', raw[:4])[0]
                body = raw[4:]
                if (zlib.crc32(body) & 0xFFFFFFFF) != stored_crc:
                    raise CorruptRecord('crc mismatch')
                return number, json.loads(body)
            except (OSError, ValueError, CorruptRecord) as e:
                if self.logger:
                    self.logger.error(f'checkpoint {path} unusable ({e}), trying older')
                continue
        return None

    def write(self, number: int, payload: dict) -> str:
        body = json.dumps(payload, separators=(',', ':')).encode()
        blob = struct.pack('<I', zlib.crc32(body) & 0xFFFFFFFF) + body

        final = self._path(number)
        tmp = final + '.tmp'
        with open(tmp, 'wb') as f:
            f.write(blob)
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp, final)

        # Without this the rename itself can be lost on power failure.
        dir_fd = os.open(self.dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

        self._prune()
        return final

    def _prune(self):
        existing = self.list_checkpoints()
        for _, path in existing[:-self.keep]:
            try:
                os.remove(path)
            except OSError:
                pass

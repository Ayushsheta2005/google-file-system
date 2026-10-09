#!/usr/bin/env python3
"""Deterministic simulation: virtual clock, seeded randomness, message bus.

The problem this solves: the concurrency bugs in a distributed system live in
orderings that happen rarely and never twice. `tests/test_crash_recovery.py`
kills a real master and hopes to catch one; it usually does not, and when it
does the run is not reproducible, so the bug cannot be studied.

The approach, from FoundationDB and TigerBeetle: run the whole cluster's
logic in **one thread over virtual time**, and route *every* source of
nondeterminism -- the clock, message latency, message loss, event ordering,
which node crashes when -- through a single seeded PRNG. A complete run then
becomes a pure function of one 64-bit seed. Found a bug on seed 48213? Run
seed 48213 again and it happens again, in exactly the same order, as many
times as you like.

Two rules make the determinism real, and both are easy to break by accident:

1. **No wall-clock time.** Nothing calls `time.time()`; everything asks the
   `Clock`. Virtual time also means a 10-minute lease timeout costs no real
   seconds, so a test can explore hours of cluster life in milliseconds.
2. **No unordered iteration over mutable state.** A `set` of node ids
   iterates in an order that depends on hash seeds and insertion history. The
   event queue breaks ties on a monotonic sequence number for the same
   reason, so two events scheduled for the same instant always fire in the
   order they were scheduled.

See ADR-007.
"""

import heapq
import itertools
import random
from typing import Any, Callable, Dict, List, Optional, Tuple


class Clock:
    """Virtual time. Advances only when the event loop says so."""

    def __init__(self):
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def advance_to(self, t: float):
        # Guard against a bug that would silently destroy determinism:
        # an event scheduled in the past means the queue ordering is wrong.
        assert t >= self.now, f'time went backwards: {t} < {self.now}'
        self.now = t

    def __repr__(self):
        return f'<Clock t={self.now:.4f}>'


class Event:
    __slots__ = ('time', 'seq', 'fn', 'label')

    def __init__(self, time, seq, fn, label):
        self.time = time
        self.seq = seq
        self.fn = fn
        self.label = label

    def __lt__(self, other):
        # Tie-break on sequence, never on object identity: identity ordering
        # varies between runs and would make the simulation nondeterministic
        # in a way that is almost impossible to notice.
        return (self.time, self.seq) < (other.time, other.seq)


class Simulator:
    """Single-threaded event loop over virtual time."""

    def __init__(self, seed: int):
        self.seed = seed
        self.rng = random.Random(seed)
        self.clock = Clock()
        self._queue: List[Event] = []
        self._seq = itertools.count()
        self.trace: List[Tuple[float, str]] = []
        self.max_trace = 20000
        self.stopped = False

    # ---------------------------------------------------------- scheduling
    def at(self, when: float, fn: Callable, label: str = ''):
        heapq.heappush(self._queue,
                       Event(when, next(self._seq), fn, label))

    def after(self, delay: float, fn: Callable, label: str = ''):
        self.at(self.clock.now + max(0.0, delay), fn, label)

    def every(self, interval: float, fn: Callable, label: str = '',
              jitter: float = 0.0, start_delay: Optional[float] = None):
        """Repeating timer. Jitter is drawn from the seeded PRNG, so a
        'random' heartbeat offset is still reproducible."""
        def tick():
            if self.stopped:
                return
            fn()
            nxt = interval + (self.rng.uniform(0, jitter) if jitter else 0.0)
            self.after(nxt, tick, label)

        first = start_delay if start_delay is not None else interval
        self.after(first + (self.rng.uniform(0, jitter) if jitter else 0.0),
                   tick, label)

    # --------------------------------------------------------------- trace
    def record(self, msg: str):
        if len(self.trace) < self.max_trace:
            self.trace.append((self.clock.now, msg))

    def dump_trace(self, last: int = 60) -> str:
        return '\n'.join(f'  {t:9.4f}  {m}' for t, m in self.trace[-last:])

    # ---------------------------------------------------------------- loop
    def run_until(self, deadline: float, on_step: Optional[Callable] = None):
        """Process events in time order until the virtual deadline."""
        steps = 0
        while self._queue and not self.stopped:
            if self._queue[0].time > deadline:
                break
            event = heapq.heappop(self._queue)
            self.clock.advance_to(event.time)
            event.fn()
            steps += 1
            if on_step:
                on_step()
            if steps > 2_000_000:
                raise RuntimeError('event loop did not settle (runaway timer?)')
        self.clock.advance_to(deadline)
        return steps

    def stop(self):
        self.stopped = True


# --------------------------------------------------------------------------
# Network
# --------------------------------------------------------------------------
class Network:
    """Message bus with injectable faults.

    Every message's latency, and whether it is dropped at all, comes from the
    simulator's PRNG -- so a fault schedule is reproducible from the seed. A
    partition is modelled as a set of blocked (from, to) pairs rather than as
    dropped messages, because the two behave differently on healing: a
    partition that ends lets subsequent messages through, while a drop is
    permanent for that message.
    """

    def __init__(self, sim: Simulator, min_latency: float = 0.001,
                 max_latency: float = 0.010, drop_rate: float = 0.0,
                 reorder: bool = True):
        self.sim = sim
        self.min_latency = min_latency
        self.max_latency = max_latency
        self.drop_rate = drop_rate
        self.reorder = reorder
        self.handlers: Dict[str, Callable] = {}
        self.partitions: set = set()      # (from_node, to_node) blocked
        self.down: set = set()            # crashed nodes
        self.paused: set = set()          # SIGSTOP-equivalent
        self.stats = {'sent': 0, 'delivered': 0, 'dropped': 0,
                      'partitioned': 0, 'to_dead': 0}

    def register(self, node_id: str, handler: Callable):
        self.handlers[node_id] = handler

    def reachable(self, src: str, dst: str) -> bool:
        if dst in self.down or src in self.down:
            return False
        if (src, dst) in self.partitions:
            return False
        return True

    def send(self, src: str, dst: str, message: Any):
        self.stats['sent'] += 1

        if src in self.down or dst in self.down:
            self.stats['to_dead'] += 1
            return
        if (src, dst) in self.partitions:
            self.stats['partitioned'] += 1
            self.sim.record(f'PARTITIONED {src}->{dst} {_name(message)}')
            return
        if self.drop_rate and self.sim.rng.random() < self.drop_rate:
            self.stats['dropped'] += 1
            self.sim.record(f'DROP {src}->{dst} {_name(message)}')
            return

        latency = self.sim.rng.uniform(self.min_latency, self.max_latency) \
            if self.reorder else self.min_latency

        def deliver():
            # Re-check at delivery time, not send time: a node that crashed
            # or became partitioned while the message was in flight must not
            # receive it. Checking only at send is a classic simulator bug
            # that hides real partition behaviour.
            if dst in self.down or dst in self.paused:
                self.stats['to_dead'] += 1
                return
            if (src, dst) in self.partitions:
                self.stats['partitioned'] += 1
                return
            handler = self.handlers.get(dst)
            if handler is None:
                return
            self.stats['delivered'] += 1
            handler(src, message)

        self.sim.after(latency, deliver, f'{src}->{dst}')

    # ----------------------------------------------------------- faults
    def partition(self, group_a, group_b):
        """Bidirectional split between two groups of nodes."""
        for a in group_a:
            for b in group_b:
                self.partitions.add((a, b))
                self.partitions.add((b, a))
        self.sim.record(f'PARTITION {sorted(group_a)} | {sorted(group_b)}')

    def heal(self):
        if self.partitions:
            self.sim.record('HEAL all partitions')
        self.partitions.clear()

    def crash(self, node_id: str):
        self.down.add(node_id)
        self.sim.record(f'CRASH {node_id}')

    def restart(self, node_id: str):
        self.down.discard(node_id)
        self.paused.discard(node_id)
        self.sim.record(f'RESTART {node_id}')

    def pause(self, node_id: str):
        """A node that is alive but not running -- a GC pause or a swapped
        out process. Distinct from a crash: its timers resume with stale
        state, which is what actually breaks lease protocols."""
        self.paused.add(node_id)
        self.sim.record(f'PAUSE {node_id}')

    def resume(self, node_id: str):
        self.paused.discard(node_id)
        self.sim.record(f'RESUME {node_id}')

    def is_running(self, node_id: str) -> bool:
        return node_id not in self.down and node_id not in self.paused


def _name(message) -> str:
    return getattr(message, 'kind', type(message).__name__)

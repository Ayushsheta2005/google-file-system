#!/usr/bin/env python3
"""Benchmark harness.

Every number that ends up in the README or on a resume comes from here, and
each benchmark records the full config snapshot alongside its result so a
figure can always be traced back to the settings that produced it.

The A/B benchmarks work by running the same cluster twice under different
environment overrides (see gfs_config.py), so the two runs differ only in the
knob under test.
"""

import json
import os
import statistics
import sys
import time
from typing import Callable, Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

RESULTS_DIR = os.path.join(HERE, 'results')


def percentiles(samples: List[float]) -> Dict[str, float]:
    if not samples:
        return {}
    s = sorted(samples)

    def pct(p):
        if len(s) == 1:
            return s[0]
        k = (len(s) - 1) * p
        lo, hi = int(k), min(int(k) + 1, len(s) - 1)
        return s[lo] + (s[hi] - s[lo]) * (k - lo)

    return {
        'min': round(s[0], 4),
        'p50': round(pct(0.50), 4),
        'p95': round(pct(0.95), 4),
        'p99': round(pct(0.99), 4),
        'max': round(s[-1], 4),
        'mean': round(statistics.fmean(s), 4),
    }


class Results:
    """Accumulates rows and writes JSON + a markdown table."""

    def __init__(self, name: str):
        self.name = name
        self.rows: List[Dict] = []
        self.started = time.time()
        os.makedirs(RESULTS_DIR, exist_ok=True)

    def add(self, **row):
        row['_t'] = round(time.time() - self.started, 2)
        self.rows.append(row)
        printable = {k: v for k, v in row.items() if not k.startswith('_')}
        print('  ' + '  '.join(f'{k}={v}' for k, v in printable.items()), flush=True)

    def save(self, extra: Optional[Dict] = None) -> str:
        import gfs_config
        payload = {
            'benchmark': self.name,
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'config': gfs_config.summary(),
            'env_overrides': {k: v for k, v in os.environ.items()
                              if k.startswith('GFS_')},
            'rows': self.rows,
        }
        if extra:
            payload.update(extra)
        path = os.path.join(RESULTS_DIR, f'{self.name}.json')
        with open(path, 'w') as f:
            json.dump(payload, f, indent=2)
        print(f'\n-> {path}')
        return path


def timed(fn: Callable, *args, **kwargs):
    t = time.perf_counter()
    result = fn(*args, **kwargs)
    return result, time.perf_counter() - t


def mb(n_bytes: int) -> float:
    return n_bytes / (1024 * 1024)


def throughput_mbps(n_bytes: int, seconds: float) -> float:
    return round(mb(n_bytes) / seconds, 2) if seconds > 0 else 0.0

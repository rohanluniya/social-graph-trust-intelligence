"""Approximate graph and streaming sketches: HyperLogLog (2-hop reach) & Count-Min Sketch (burst velocity)."""

from __future__ import annotations

import hashlib
import math
from typing import Any

import numpy as np


def _hash64(value: str, seed: int = 0) -> int:
    digest = hashlib.blake2b(f"{seed}:{value}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big")


class HyperLogLog:
    """Compact HyperLogLog cardinality sketch with vectorized NumPy register merging.

    Used to estimate 2-hop distinct neighborhood reach in O(m) register unions
    instead of materializing massive celebrity neighbor sets in memory.
    """

    def __init__(self, precision: int = 10) -> None:
        if not (4 <= precision <= 16):
            raise ValueError("HLL precision must be between 4 and 16")
        self.precision = precision
        self.m = 1 << precision
        self.registers = np.zeros(self.m, dtype=np.uint8)
        if self.m == 16:
            self.alpha = 0.673
        elif self.m == 32:
            self.alpha = 0.697
        elif self.m == 64:
            self.alpha = 0.709
        else:
            self.alpha = 0.7213 / (1.0 + 1.079 / self.m)

    def add(self, item: str) -> None:
        h = _hash64(item)
        idx = h >> (64 - self.precision)
        w = h & ((1 << (64 - self.precision)) - 1)
        # Count leading zeros in the remaining (64 - precision) bits + 1
        rem_bits = 64 - self.precision
        rho = rem_bits + 1 if w == 0 else (rem_bits - w.bit_length()) + 1
        if rho > self.registers[idx]:
            self.registers[idx] = rho

    def add_many(self, items: list[str] | set[str]) -> None:
        for item in items:
            self.add(item)

    def merge_inplace(self, other: HyperLogLog) -> None:
        if self.precision != other.precision:
            raise ValueError("Cannot merge HLL sketches with different precision")
        np.maximum(self.registers, other.registers, out=self.registers)

    def estimate(self) -> int:
        inv_sum = float(np.sum(np.power(2.0, -self.registers.astype(np.float64))))
        raw_est = self.alpha * (self.m**2) / inv_sum
        # Small-range linear counting bias correction
        zeros = int(np.count_nonzero(self.registers == 0))
        if raw_est <= 2.5 * self.m and zeros > 0:
            return int(round(self.m * math.log(self.m / zeros)))
        return int(round(raw_est))

    @property
    def memory_bytes(self) -> int:
        return int(self.registers.nbytes)


class CountMinSketch:
    """Fixed-memory Count-Min Sketch guaranteeing zero underestimation (`query(x) >= exact(x)`)."""

    def __init__(self, depth: int = 5, width: int = 2048) -> None:
        self.depth = depth
        self.width = width
        self.table = np.zeros((depth, width), dtype=np.uint32)
        self._seeds = [104_729 * (i + 1) + 7919 for i in range(depth)]

    def _indices(self, key: str) -> list[int]:
        return [_hash64(key, seed=s) % self.width for s in self._seeds]

    def increment(self, key: str, count: int = 1) -> None:
        if count <= 0:
            return
        for row, col in enumerate(self._indices(key)):
            self.table[row, col] += np.uint32(count)

    def query(self, key: str) -> int:
        return int(min(self.table[row, col] for row, col in enumerate(self._indices(key))))

    @property
    def memory_bytes(self) -> int:
        return int(self.table.nbytes)


def compute_two_hop_hll_and_compare(
    user_ids: list[str],
    out_neighbors: dict[str, set[str]],
    in_neighbors: dict[str, set[str]],
    precision: int = 10,
) -> tuple[dict[str, int], dict[str, int], dict[str, Any]]:
    """Compute exact vs HyperLogLog 2-hop distinct reach across the undirected neighborhood.

    Returns (exact_two_hop, hll_two_hop, accuracy_summary).
    """
    # Build 1-hop undirected neighbor sets and compact 1-hop HLL registers per node
    one_hop: dict[str, set[str]] = {}
    hll_1hop: dict[str, HyperLogLog] = {}
    for uid in user_ids:
        nbrs = set(out_neighbors.get(uid, set())) | set(in_neighbors.get(uid, set()))
        one_hop[uid] = nbrs
        sketch = HyperLogLog(precision=precision)
        sketch.add(uid)
        sketch.add_many(nbrs)
        hll_1hop[uid] = sketch

    exact_two_hop: dict[str, int] = {}
    hll_two_hop: dict[str, int] = {}
    rel_errors: list[float] = []
    exact_bytes_total = 0
    hll_bytes_total = 0

    for uid in user_ids:
        nbrs = one_hop[uid]
        if not nbrs:
            exact_two_hop[uid] = 0
            hll_two_hop[uid] = 0
            hll_bytes_total += hll_1hop[uid].memory_bytes
            continue

        # Exact 2-hop union (excluding self)
        exact_set: set[str] = set(nbrs)
        for v in nbrs:
            exact_set.update(one_hop.get(v, set()))
        exact_set.discard(uid)
        exact_cnt = len(exact_set)
        exact_two_hop[uid] = exact_cnt
        # Approximate Python/Arrow string-set memory footprint (64 bytes per distinct user_id entry)
        exact_bytes_total += max(64, exact_cnt * 64)

        # Approximate 2-hop via O(m) HLL register unions
        merged = HyperLogLog(precision=precision)
        merged.merge_inplace(hll_1hop[uid])
        for v in nbrs:
            if v in hll_1hop:
                merged.merge_inplace(hll_1hop[v])
        approx_cnt = max(0, merged.estimate() - 1)  # exclude self
        hll_two_hop[uid] = approx_cnt
        hll_bytes_total += merged.memory_bytes

        if exact_cnt >= 10:
            rel_errors.append(abs(approx_cnt - exact_cnt) / exact_cnt)

    mean_err_pct = float(np.mean(rel_errors) * 100.0) if rel_errors else 0.0
    p95_err_pct = float(np.percentile(rel_errors, 95) * 100.0) if rel_errors else 0.0
    mem_ratio = (exact_bytes_total / hll_bytes_total) if hll_bytes_total > 0 else 1.0

    summary = {
        "hll_precision": precision,
        "hll_registers": 1 << precision,
        "hll_mean_rel_error_pct": round(mean_err_pct, 3),
        "hll_p95_rel_error_pct": round(p95_err_pct, 3),
        "exact_memory_bytes": int(exact_bytes_total),
        "hll_memory_bytes": int(hll_bytes_total),
        "memory_reduction_ratio": round(mem_ratio, 2),
    }
    return exact_two_hop, hll_two_hop, summary

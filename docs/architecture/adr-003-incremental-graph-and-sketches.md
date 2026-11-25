# ADR-003: Incremental Graph Feature Updates & Approximate Sketches

## Status
Accepted and Implemented (`social_graph/graph_engine.py`, `social_graph/sketches.py`)

## Context
1. Recomputing full graph features (`in_degree`, `out_degree`, `reciprocal_ratio`, exact triangles, `local_clustering`, `PageRank`) from scratch on every micro-batch wastes compute when only a small fraction of edges changed.
2. Materializing exact 2-hop distinct neighbor sets across celebrity supernodes consumes gigabytes to terabytes of RAM because every follower of a mega-celebrity inherits the celebrity's entire follower neighborhood.

## Decision
1. **Exact Incremental State Engine (`IncrementalGraphState`):** Maintain incremental directed/undirected incidence sets with exact triangle delta updates (`common = undir_adj[u] & undir_adj[v]`) and warm-started sparse CSR PageRank. Verify exact numerical parity (`max_abs_diff == 0.0`) against full `scipy.sparse` CSR recomputation (`(U @ U).multiply(U)`).
2. **Degree-Dampened Label Propagation (`1 / degree^0.75`):** Dampen supernode neighbor vote weights during community detection so celebrity bridges cannot collapse distinct organic communities into a single monster cluster (improving Modularity `Q` from `0.0282` -> `0.4935` on the `local` profile).
3. **HyperLogLog (`p=10`, 1,024 registers) for 2-Hop Distinct Reach:** Merge 1-hop register arrays in $O(m)$ vectorized NumPy `np.maximum` operations instead of set unions.
4. **Count-Min Sketch (`5 x 2048` uint32):** Track per-actor engagement burst counts in fixed `40 KB` memory with guaranteed zero underestimation (`query(x) >= exact(x)`).

## Measured Impact
- **Incremental speedup (`local` profile, 10% edge delta):** `2.89 ms` structural delta vs `135.31 ms` full CSR (`46.8×` faster; `19.69 ms` / `6.9×` faster including warm-start PageRank) with `0.0` exact feature diff.
- **HyperLogLog memory savings:** `84.9×` smaller on `local` (`3.55%` mean rel error) and **`320.1×` smaller on `stress`** (`3.60%` mean rel error).

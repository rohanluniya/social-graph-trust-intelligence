# Staff Data Engineering Interview Deep-Dive Guide

## 1. Why Vertex-Cut Partitioning Beats Edge-Cut for Social Graphs
In power-law graphs, an edge-cut partitions vertices so every vertex lives on one shard — meaning a celebrity with 10M followers forces 10M edges onto a single shard (extreme straggler / OOM risk). A **vertex-cut** partitions edges and splits only the tiny subset of supernode vertices (`< 1%`) across `S` salted mirror partitions. In our implementation (`social_graph/partitioning.py` and `processing/spark/graphframes_social_job.py`), vertex replication stays at **`1.035×`** while reducing skew by `-23.3%` on `local` (`-64.3%` under the Chaos Drill 1 celebrity storm) and cutting celebrity 2-hop p95 latency by **`4.2×`**.

## 2. How Event-Time Temporal Edges Prevent Phantom Edge Resurrection
Kafka and CDC pipelines guarantee at-least-once delivery, not global ordering. If a `follow(t=1000)` retry arrives after `unfollow(t=5000)`, processing-time ordering resurrects a deleted relationship. `silver.temporal_edges` orders all state transitions strictly by `(event_ts, event_id)`, tracks explicit `valid_from`, `valid_to`, `is_active`, `is_deleted`, and `is_blocked` columns, and closes bidirectional edges immediately on `block` or `user_deleted`. Chaos Drill 3 verified `0 / 50` resurrected edges under late-arrival storms.

## 3. Proving Incremental Graph Feature Equivalence Without Drift
Incremental pipelines silently drift over months if edge cases (reciprocal edges turning one-way, self-loops, user erasures) are mishandled. We eliminate drift via two mechanisms:
- **Exact undirected triangle invariant:** A directed edge `(a -> b)` only changes undirected triangles when reverse edge `(b -> a)` is absent; otherwise only `reciprocal_degree` changes.
- **Hypothesis property-based testing + continuous `DQ-06` parity gate:** Every run asserts `max_exact_feature_abs_diff == 0.0` against full `scipy.sparse` CSR `(U @ U).multiply(U)` recomputation.

## 4. Why Approximate Sketches (HLL & CMS) Are Mandatory at Scale
- **HyperLogLog (`p=10`):** Merging 1-hop HLL registers via `np.maximum` computes 2-hop distinct reach in $O(m)$ fixed memory per vertex (`1 KB/node`), shrinking `stress` 2-hop memory **`320.1×`** at `3.60%` mean relative error.
- **Count-Min Sketch (`5 x 2048`):** Tracks burst engagement velocity in `40 KB` fixed RAM with mathematical zero-underestimation guarantee (`DQ-09`, Hypothesis property verified).

## 5. Handling GDPR Article 17 Right-to-Erasure End to End
Deleting a row from an OLTP database does not purge downstream graph lakehouse tables, precomputed adjacency partitions, or serving caches. Our `POST /v1/privacy/erase/{user_id}` admin workflow appends an immutable tombstone, closes active temporal edges, purges all 4 Gold Iceberg tables and DuckDB/in-memory serving indexes, and executes a synchronous zero-leak audit verifying `residual_active_edges == 0` and `residual_serving_records == 0` (`192.5 ms` in Chaos Drill 4 for a 354-edge celebrity supernode).

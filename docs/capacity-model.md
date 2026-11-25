# Capacity Model: Billion-Edge Logical Design vs. Local Measured Scale

This document explicitly separates the **billion-edge target architectural capacity model** (distributed Spark/GraphFrames + Iceberg V2 + Trino) from **locally measured workstation benchmarks** (`smoke`, `local`, `stress`).

## 1. Billion-Edge Logical Target Sizing (Analytical Projection)

Assumptions for a Meta-scale social graph tier:
- **Vertices ($|V|$):** $50\text{M}$ active accounts ($0.1\%$ or $50\text{k}$ celebrity supernodes with degree $\ge 10\text{k}$).
- **Active Directed Edges ($|E|$):** $1.0 \times 10^9$ active follow edges ($\approx 20$ avg out-degree, heavy-tailed in-degree up to $10^7$).
- **Daily Event Velocity:** $250\text{M}$ social mutations/day ($\approx 2,900\text{ events/s}$ steady, $15,000\text{ events/s}$ peak burst).

| Storage / Compute Layer | Logical Formula | Projected Footprint (1B Edges) | Partitioning Strategy |
| --- | --- | --- | --- |
| `silver.temporal_edges` (Iceberg V2 Zstd Parquet) | $\approx 28\text{ B/edge}$ compressed $\times 1.25\times$ history | $\approx 35\text{ GB}$ | Bucketed by `(partition_id)` + daily `valid_from` |
| `gold.adjacency_partitioned` (Vertex-cut) | $\approx 22\text{ B/edge} \times 1.04\times$ mirror replication | $\approx 23\text{ GB}$ | 256–1024 partitions; supernodes salted across $S=32$ buckets |
| `gold.node_graph_features` ($50\text{M}$ rows) | $\approx 120\text{ B/node}$ compressed | $\approx 6.0\text{ GB}$ | Hash-bucketed on `user_id` |
| Exact 2-Hop Neighbor Sets (Uncompressed RAM) | $\sum_u |N_2(u)| \times 64\text{ B}$ | **Terabytes** (infeasible in RAM due to supernode blowup) | Replaced by HyperLogLog |
| HyperLogLog 2-Hop Registers ($p=10$, $1\text{ KB/node}$) | $50\text{M} \times 1,024\text{ B}$ | **51.2 GB** distributed RAM ($\approx 100\times$ smaller) | Co-located with vertex shard |
| Count-Min Sketch (`5 x 2048` uint32 per shard) | $40\text{ KB}$ per streaming worker | $< 10\text{ MB}$ cluster-wide | Fixed memory independent of cardinality |

## 2. Measured Local Scale Separation

Local workstation benchmarks (`benchmarks/run_benchmarks.py` on Apple M4 Pro, 14 cores, 48 GiB RAM) measure real end-to-end execution across three reproducible scales (`smoke`: 400 users, `local`: 1,600 users, `stress`: 6,000 users / 180k+ events) using the exact same Iceberg V2 schemas, sparse CSR math, vertex-cut salting, HLL/CMS sketches, and GDPR erasure logic. See `results/benchmarks/benchmark_results.json` and `docs/benchmarks/benchmark-report.md` for all measured numbers.

# ADR-002: Skew-Aware Vertex-Cut Supernode Partitioning

## Status
Accepted and Implemented (`social_graph/partitioning.py`, `processing/spark/graphframes_social_job.py`)

## Context
Social graphs exhibit heavy-tailed power-law degree distributions: a tiny fraction of celebrity supernodes (`< 1%` of vertices) accumulate massive inbound follower lists. Under naive vertex-hash partitioning (`hash(node_id) % P`), all incident edges of a celebrity land on a single partition, causing severe memory skew, shuffle bottlenecks, and straggler tail latency during 2-hop neighborhood expansions.

## Decision
Implement **skew-aware vertex-cut partitioning** in `gold.adjacency_partitioned`:
1. Identify supernodes dynamically (`in_degree + out_degree >= supernode_threshold`).
2. Salt supernode incident edges across `S` mirror buckets (`partition_id = (hash(node_id) + hash(neighbor_id) % S) % P`, storing `salt_bucket` and `is_supernode_edge = True`).
3. Assign regular (non-supernode) vertices using degree-balanced capacity-aware placement (`LPT` descending degree scheduling respecting community affinity up to `1.03×` mean shard load) so no single community collapses partition balance.

## Measured Impact
- **Local profile (`32,380` active edges, 16 shards):** Max/mean skew ratio dropped from `1.397×` -> `1.071×` (`-23.34%`), partition CV dropped `-65.6%` (`0.1403` -> `0.0482`), and celebrity 2-hop p95 latency dropped from `1.36 ms` -> `0.32 ms` (`4.2×` faster) at only `1.035×` vertex replication.
- **Extreme supernode skew storm (Chaos Drill 1, 550 followers on 1 celebrity):** Max/mean skew dropped from `3.520×` -> `1.256×` (`-64.32%`) and 2-hop p95 latency dropped `5.5×` (`0.233 ms` -> `0.042 ms`).

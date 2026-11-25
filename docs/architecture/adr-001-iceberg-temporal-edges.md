# ADR-001: Apache Iceberg V2 Medallion & Temporal Directed Edge Lifecycle

## Status
Accepted and Implemented (`social_graph/iceberg_store.py`, `social_graph/temporal_edges.py`)

## Context
Social interaction graphs mutate continuously: users follow, unfollow, block, unblock, and invoke GDPR Article 17 right-to-erasure. Stream redeliveries and out-of-order arrivals can deliver an older `follow` event after a newer `unfollow` or `block`. A flat overwritten edge table cannot audit historical state or guarantee point-in-time reproducibility.

## Decision
1. Store all 10 medallion tables (`bronze.*`, `silver.*`, `gold.*`, `quality.*`) in **Apache Iceberg V2** (`format-version=2`, Zstd-compressed Parquet) managed by PyIceberg (`0.12.0`).
2. Model `silver.temporal_edges` as directed valid-time intervals `[valid_from, valid_to)` with explicit `event_id`, `is_active`, `is_deleted`, `is_blocked`, and `snapshot_version` columns.
3. Enforce strict **event-time ordering** `(event_ts, event_id)` when applying mutations so late-arriving stale `follow` events cannot resurrect edges closed by a newer `unfollow`, `block`, or `user_deleted`.
4. Maintain `silver.graph_snapshots` linking logical `snapshot_version` integers to Iceberg `snapshot_id`s for deterministic time-travel scans (`tbl.scan(snapshot_id=...)`).

## Tradeoffs
- **Pros:** Atomic multi-file commits, snapshot time travel, zero-copy PyArrow/DuckDB reads, native small-file compaction (`12 files -> 1 file`, `2.93×` faster scan).
- **Cons:** Full Iceberg table overwrites during GDPR erasure rewrite Parquet files synchronously in `local_native` mode; at billion-edge scale, production deployments should use Iceberg V2 row-level position/equality deletes with asynchronous background compaction.

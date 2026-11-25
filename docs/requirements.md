# System Requirements, SLOs & Consistency Boundaries

## 1. Functional Requirements

1. **Deterministic Graph & Adversarial Generation:** Seeded synthesis of organic communities, heavy-tailed celebrity supernodes, bot swarms, coordinated engagement rings, edge churn (`unfollow`), bidirectional `block`/`unblock`, out-of-order late retractions, GDPR Article 17 `user_deleted` tombstones, duplicates, and schema-invalid poison records.
2. **Temporal Directed Edge Lifecycle:** Maintain SCD2-style valid-time edges (`valid_from`, `valid_to`, `is_active`, `is_deleted`, `is_blocked`, `snapshot_version`) ordered strictly by authoritative `(event_ts, event_id)` rather than `ingest_ts`.
3. **Graph-Aware Partitioning & Skew Mitigation:** Partition adjacency tables using skew-aware vertex-cut salting on supernodes and measure max/mean skew ratio, coefficient of variation, replication factor, and 2-hop p95 latency against naive source/node-hash partitioning.
4. **Incremental Graph Updates & Exact Parity:** Maintain incremental degree, reciprocal ratio, triangle counts, local clustering, and warm-started PageRank, proving exact numerical parity (`max_abs_diff == 0.0`) against full CSR recomputation.
5. **Approximate Sketches:** Estimate 2-hop distinct reach via HyperLogLog (`p=10`) and track engagement bursts via Count-Min Sketch (`5 x 2048`, zero underestimation guaranteed).
6. **Explainable Trust & Abuse Intelligence:** Produce human-readable reason codes and additive feature attributions evaluated strictly against synthetic ground-truth labels, with explicit celebrity false-positive mitigation and FP error analysis.
7. **Privacy & Right-to-Erasure Propagation:** Propagate user erasure tombstones across Iceberg V2 Silver/Gold tables, DuckDB serving tables, and in-memory adjacency caches with verified `0` residual leaks.

## 2. Service-Level Objectives (Local Native Verified Targets)

| Capability | Target SLO | Verification Method |
| --- | --- | --- |
| API node inspection latency (`GET /v1/graph/node/{id}`) | p95 < 100 ms | `tests/performance/test_serving_slo.py` |
| GDPR right-to-erasure propagation (`POST /v1/privacy/erase/{id}`) | < 2,000 ms, 0 residual leaks | `tests/integration/test_iceberg_and_api.py`, Chaos Drill 4 |
| Incremental vs. full graph feature parity | Exact `max_abs_diff <= 1e-6` (0.0 structural) | Hypothesis property tests & DQ-06 |
| Data Quality gate pass rate | 100% (`9/9` checks) | `quality.dq_results` |
| Poison event isolation | 100% quarantined to `bronze.quarantine_dlq`, 0 Bronze leaks | Chaos Drill 7 & DQ-02 |
| RBAC access control | 100% unauthenticated `/v1/*` -> `401`; non-admin erasure -> `403` | Integration security tests |

## 3. Explicit Consistency Boundaries

- **Ingestion Delivery vs. Business Effect:** The topic spool / Kafka path is at-least-once; exactly-once business effects in `bronze.social_events` are enforced via deterministic `event_id` deduplication prior to atomic Iceberg V2 table commit.
- **Event-Time vs. Processing-Time Precedence:** `silver.temporal_edges` applies last-writer-wins state transitions in authoritative `(event_ts, event_id)` order. Late stale events cannot overwrite newer closures.
- **Atomic Medallion Snapshots:** Each Apache Iceberg V2 table commits atomically as a new snapshot (`format-version=2`), enabling consistent historical reads via `snapshot_id` time travel.
- **Erasure Consistency:** `propagate_user_erasure` updates Iceberg Silver/Gold tables and serving indexes synchronously within a single administrative workflow and executes a post-write zero-leak verification read across every active layer before returning HTTP 200. Historical Bronze raw logs retain immutable provenance unless separate retention purge / crypto-shredding is invoked (documented in `docs/limitations.md`).

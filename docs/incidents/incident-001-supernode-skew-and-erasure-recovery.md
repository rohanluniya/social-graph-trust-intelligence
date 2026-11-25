# Incident Report & Runbook: Supernode Skew Storm, Cache Loss & GDPR Erasure Recovery

## Incident Summary (Controlled Chaos Simulation)

- **Scenario:** Combined execution of Chaos Drills 1, 4, 5, 6, and 7 (`tests/chaos/test_chaos_drills.py`, raw evidence in `results/chaos/chaos_results.json`).
- **Simulated Faults:**
  1. Mega-celebrity follower storm (`550` rapid followers concentrating on `usr_000001`).
  2. Live GDPR Article 17 right-to-erasure request on the 354-edge supernode `usr_000001`.
  3. Broker outage buffering `250` events to local WAL spool with `40` duplicate redeliveries.
  4. Complete wipe of DuckDB serving database and in-memory adjacency caches.
  5. `60` schema-invalid poison records mixed into live event traffic.

## Observed Recovery Behavior (Measured)

| Drill | Fault Injected | Observed Recovery Metric | Outcome |
| --- | --- | --- | :---: |
| Drill 1 | 550-follower celebrity skew storm | Vertex-cut reduced skew `3.520× -> 1.256×` (`-64.32%`), 2-hop p95 `0.233 ms -> 0.042 ms` | PASS |
| Drill 3 | 50 out-of-order late follow retractions | `0` closed edges resurrected under event-time LWW | PASS |
| Drill 4 | Live GDPR erasure on `usr_000001` (354 edges) | Propagated in `192.51 ms`, `residual_active_edges == 0`, `residual_serving_records == 0` | PASS |
| Drill 5 | Broker outage (`250` buffered) + `40` dupes | `250` recovered from WAL spool, `40` dupes dropped, `0` events lost | PASS |
| Drill 6 | Full DuckDB & memory cache wipe | Deterministic Iceberg rebuild in `45.84 ms`; exact `396` nodes / `6,588` edges restored; erased users stayed purged | PASS |
| Drill 7 | `60` poison events mixed with `100` valid | `60/60` quarantined to `bronze.quarantine_dlq`, `0` leaked to Bronze | PASS |

## Operator Runbook Actions

1. **If `SGTIPartitionSkewRegression` fires (`sgti_partition_skew_ratio > 1.5`):**
   - Lower `SGTI_SUPERNODE_DEGREE_THRESHOLD` or increase `SGTI_SUPERNODE_SALT_BUCKETS` (up to `SGTI_NUM_PARTITIONS`), then rerun `uv run python scripts/run_pipeline.py`.
2. **If `SGTIDataQualityFailure` fires (`sgti_dq_checks_failed > 0`):**
   - Inspect `GET /v1/dq` with viewer/analyst `X-API-Key` to identify the failing `DQ-*` check and quarantine breakdown.
3. **If DuckDB serving state is corrupted or stale:**
   - Trigger `POST /v1/admin/rebuild-serving` with admin `X-API-Key` to rebuild deterministically from durable Iceberg V2 Parquet snapshots.
4. **If a GDPR erasure audit flags residual records:**
   - Re-run `POST /v1/privacy/erase/{user_id}` (idempotent) and verify `zero_leak_verified == true` in `/v1/privacy/audit`.

# ADR-004: Explainable Trust Intelligence & GDPR Right-to-Erasure Propagation

## Status
Accepted and Implemented (`social_graph/trust_scoring.py`, `social_graph/privacy.py`)

## Context
Trust & Safety analysts cannot act on opaque black-box abuse scores, and high-in-degree organic celebrities frequently trigger naive anomaly detectors. Simultaneously, privacy regulations (GDPR Article 17) require verifiable deletion of user edges, features, and serving records upon erasure requests.

## Decision
1. **Additive Feature-Attributed Risk Scoring (`0..100`):** Combine five normalized, interpretable signals (`outbound_burst_velocity`, `low_reciprocity_fanout`, `coordinated_ring_jaccard`, `report_velocity_signal`, `block_ratio_signal`) emitting structured reason codes (`COORDINATED_RING_OVERLAP`, `LOW_RECIPROCITY_FANOUT`, `BOT_OUTBOUND_BURST`, etc.) and explicit per-feature point contributions.
2. **Celebrity Supernode False-Positive Guard:** Require asymmetric outbound fan-out (`out_degree / (in_degree + 1) >= 2.2`) for `low_reciprocity_fanout` and audience-scaled denominators on reports/blocks for verified high-in-degree supernodes.
3. **Synthetic Benchmark Discipline:** Frame all Precision/Recall/F1/FPR metrics strictly as evaluations against deterministic synthetic ground-truth cohorts — never unsupported real-world moderation claims — and publish explicit false-positive error analysis.
4. **Synchronous Zero-Leak Erasure Workflow:** `propagate_user_erasure()` writes an immutable tombstone to `silver.user_blocks_and_deletions`, closes all incident active edges in `silver.temporal_edges`, filters active rows from all four `gold.*` Iceberg V2 tables, purges DuckDB + in-memory adjacency caches, runs a mandatory post-write zero-leak verification read, and appends an audit receipt.

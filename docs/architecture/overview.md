# Architecture Overview & Design Decisions

## Component & Failure-Domain Architecture

```mermaid
flowchart LR
    subgraph Source["1. Deterministic Source & Ingestion"]
        GEN["Synthetic Adversarial Generator\n(Celebrities, Bots, Rings, Retractions, GDPR)"]
        CDC["Debezium PG CDC / Kafka 4.3\n(Local: Partitioned WAL Spool)"]
        AVRO["Avro v1/v2 Schema Validator"]
        DLQ["bronze.quarantine_dlq\n(Poison Isolation)"]
    end

    subgraph Lakehouse["2. Apache Iceberg V2 Medallion Lakehouse"]
        B1["bronze.social_events\n(Deduplicated by event_id)"]
        S1["silver.temporal_edges\n(Event-time LWW, valid_from/valid_to)"]
        S2["silver.user_blocks_and_deletions\n& silver.graph_snapshots"]
        G1["gold.adjacency_partitioned\n(Skew-Aware Vertex-Cut)"]
        G2["gold.node_graph_features\n(CSR + Incremental + HLL/CMS)"]
        G3["gold.communities_and_components\n(Degree-Dampened LPA)"]
        G4["gold.trust_risk_scores\n(Explainable Attributions)"]
        Q1["quality.dq_results\n(9 Automated Gates)"]
    end

    subgraph Serving["3. Serving, Privacy & Observability"]
        DUCK["DuckDB Serving Index +\nIn-Memory Adjacency Cache"]
        API["FastAPI RBAC API\n(127.0.0.1:8050)"]
        UI["Control Tower Dashboard\n(Inspector + Live GDPR Erasure)"]
        OBS["Prometheus /metrics +\nJSON Logs + OTel Spans + OpenLineage"]
    end

    GEN --> CDC --> AVRO
    AVRO -- "Invalid" --> DLQ
    AVRO -- "Valid + Dedup" --> B1
    B1 --> S1 & S2
    S1 --> G1 & G2 & G3
    G2 & B1 --> G4
    G1 & G2 & G3 & G4 --> Q1
    G1 & G2 & G3 & G4 & Q1 --> DUCK --> API --> UI
    API --> OBS
```

## Core Workflow Sequence (Ingestion, Incremental Update & GDPR Erasure)

```mermaid
sequenceDiagram
    participant Producer as Source / Spool
    participant Validator as Avro v1/v2 Validator
    participant Iceberg as Apache Iceberg V2
    participant Graph as Sparse CSR / Incremental Engine
    participant Serving as DuckDB + Adjacency Cache
    participant Admin as Admin / Privacy API

    Producer->>Validator: Batch social events (follows, unfollows, blocks, bursts, poison)
    Validator->>Iceberg: Quarantine invalid -> bronze.quarantine_dlq
    Validator->>Iceberg: Deduplicate event_id -> bronze.social_events
    Iceberg->>Graph: Sort by (event_ts, event_id) -> silver.temporal_edges
    Graph->>Graph: Incremental delta update + verify exact parity vs full CSR
    Graph->>Iceberg: Commit Gold tables + silver.graph_snapshots + quality.dq_results
    Iceberg->>Serving: Materialize DuckDB serving index + adjacency cache

    Admin->>Iceberg: POST /v1/privacy/erase/{user_id}
    Iceberg->>Iceberg: Append tombstone, close active edges, filter Gold tables
    Iceberg->>Serving: Purge user from DuckDB + in-memory cache
    Serving-->>Admin: Receipt (tombstone_propagation_ms, residual_leaks == 0)
```

## Architecture Decision Records (ADRs)

- [ADR-001: Apache Iceberg V2 Temporal Edge Modeling](adr-001-iceberg-temporal-edges.md)
- [ADR-002: Skew-Aware Vertex-Cut Supernode Partitioning](adr-002-skew-aware-vertex-cut.md)
- [ADR-003: Incremental Graph Feature Updates & Approximate Sketches](adr-003-incremental-graph-and-sketches.md)
- [ADR-004: Explainable Trust Intelligence & GDPR Erasure Propagation](adr-004-explainable-trust-and-privacy.md)

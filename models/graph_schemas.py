"""Pydantic v2 domain models for API responses, analytics payloads, and governance receipts."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    service: str = "social-graph-trust-intelligence"
    runtime_mode: str
    snapshot_version: int
    active_nodes: int
    active_edges: int
    erased_users: int
    freshness_seconds: float
    last_materialized_at: str | None = None
    uptime_seconds: float


class PartitionComparison(BaseModel):
    num_partitions: int
    supernode_threshold: int
    supernode_count: int
    naive_strategy: str
    naive_max_edges: int
    naive_mean_edges: float
    naive_skew_ratio: float
    naive_cv: float
    naive_two_hop_p95_ms: float
    vertex_cut_strategy: str
    vertex_cut_max_edges: int
    vertex_cut_mean_edges: float
    vertex_cut_skew_ratio: float
    vertex_cut_cv: float
    vertex_cut_two_hop_p95_ms: float
    skew_reduction_pct: float
    replication_factor: float
    naive_partition_loads: list[int]
    vertex_cut_partition_loads: list[int]


class SketchAccuracySummary(BaseModel):
    hll_precision: int
    hll_registers: int
    hll_mean_rel_error_pct: float
    hll_p95_rel_error_pct: float
    exact_memory_bytes: int
    hll_memory_bytes: int
    memory_reduction_ratio: float
    cms_depth: int
    cms_width: int
    cms_zero_underestimation_verified: bool
    cms_mean_overcount: float


class TrustEvaluationSummary(BaseModel):
    note: str = (
        "Evaluated strictly against synthetic benchmark ground-truth labels "
        "(bot_swarm + coordinated_ring vs organic + celebrity); not real-world moderation claims."
    )
    threshold: float
    true_positives: int
    false_positives: int
    true_negatives: int
    false_negatives: int
    precision: float
    recall: float
    f1_score: float
    false_positive_rate: float
    celebrity_false_positives: int
    celebrity_total: int
    false_positive_analysis: list[dict[str, Any]] = Field(default_factory=list)


class NodeDetailResponse(BaseModel):
    user_id: str
    is_erased: bool
    cohort_label: str | None = None
    in_degree: int = 0
    out_degree: int = 0
    reciprocal_degree: int = 0
    reciprocal_ratio: float = 0.0
    local_clustering: float = 0.0
    two_hop_exact: int = 0
    two_hop_hll: int = 0
    pagerank_score: float = 0.0
    engagement_burst_cms: int = 0
    report_count: int = 0
    block_received_count: int = 0
    ring_jaccard_max: float = 0.0
    is_supernode: bool = False
    component_id: int | None = None
    component_size: int = 0
    community_id: int | None = None
    community_size: int = 0
    risk_score: float = 0.0
    risk_tier: str = "erased"
    top_reasons: list[str] = Field(default_factory=list)
    feature_attributions: dict[str, float] = Field(default_factory=dict)
    sample_following: list[str] = Field(default_factory=list)
    sample_followers: list[str] = Field(default_factory=list)
    snapshot_version: int = 0


class ErasureReceipt(BaseModel):
    user_id: str
    status: str
    erased_at: str
    tombstone_event_id: str
    edges_closed: int
    residual_active_edges: int
    residual_serving_records: int
    zero_leak_verified: bool
    tombstone_propagation_ms: float
    snapshot_version: int


class OverviewResponse(BaseModel):
    run_id: str
    profile: str
    seed: int
    snapshot_version: int
    last_materialized_at: str
    freshness_seconds: float
    total_events_generated: int
    valid_events_ingested: int
    duplicates_deduped: int
    dlq_quarantined: int
    active_nodes: int
    active_edges: int
    erased_users: int
    blocked_pairs: int
    supernode_count: int
    component_count: int
    largest_component_size: int
    community_count: int
    modularity_q: float
    incremental_parity_verified: bool
    incremental_max_abs_diff: float
    incremental_speedup_x: float
    partitioning: PartitionComparison
    sketches: SketchAccuracySummary
    trust_evaluation: TrustEvaluationSummary
    dq_passed: int
    dq_total: int
    iceberg_tables: list[dict[str, Any]]

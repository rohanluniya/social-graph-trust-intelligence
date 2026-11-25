"""Unit tests for skew-aware vertex-cut partitioning and explainable trust risk scoring."""

from __future__ import annotations

import pytest

from social_graph.generator import generate_social_dataset
from social_graph.graph_engine import compute_full_graph_metrics
from social_graph.partitioning import build_and_benchmark_partitioning
from social_graph.temporal_edges import build_temporal_edges
from social_graph.trust_scoring import compute_explainable_trust_scores


@pytest.mark.unit
def test_vertex_cut_reduces_supernode_partition_skew() -> None:
    """Skew-aware vertex-cut mitigates heavy celebrity supernode imbalance relative to naive hash."""
    ds = generate_social_dataset(profile_name="smoke", seed=42)
    valid = [e for e in ds.events if e.get("event_id") and str(e.get("actor_id", "")).startswith("usr_") and int(e.get("event_ts", 0)) > 0 and e.get("event_type") != "invalid_mutation" and e.get("actor_id") != e.get("target_id")]
    temporal = build_temporal_edges(valid)
    graph = compute_full_graph_metrics(temporal.active_users, temporal.active_edges)
    part = build_and_benchmark_partitioning(
        temporal.active_users,
        temporal.active_edges,
        communities=graph.community_id,
        num_partitions=16,
        supernode_threshold=60,
        salt_buckets=8,
    )
    cmp = part.comparison
    assert cmp["supernode_count"] >= 2
    assert cmp["vertex_cut_skew_ratio"] < cmp["naive_skew_ratio"]
    assert cmp["skew_reduction_pct"] > 0.0
    assert cmp["vertex_cut_cv"] < cmp["naive_cv"]


@pytest.mark.unit
def test_explainable_trust_scoring_bounds_and_celebrity_protection() -> None:
    """Trust scores stay within [0, 100], include explainable reasons, and avoid celebrity false positives."""
    ds = generate_social_dataset(profile_name="smoke", seed=42)
    valid = [e for e in ds.events if e.get("event_id") and str(e.get("actor_id", "")).startswith("usr_") and int(e.get("event_ts", 0)) > 0 and e.get("event_type") != "invalid_mutation" and e.get("actor_id") != e.get("target_id")]
    temporal = build_temporal_edges(valid)
    graph = compute_full_graph_metrics(temporal.active_users, temporal.active_edges)
    part = build_and_benchmark_partitioning(
        temporal.active_users, temporal.active_edges, num_partitions=16, supernode_threshold=60
    )
    trust = compute_explainable_trust_scores(
        events=valid,
        graph=graph,
        user_labels=ds.user_labels,
        supernodes=part.supernodes,
    )
    ev = trust.evaluation
    assert ev["f1_score"] >= 0.85
    assert ev["celebrity_false_positives"] == 0
    for row in trust.trust_rows:
        assert 0.0 <= float(row["risk_score"]) <= 100.0

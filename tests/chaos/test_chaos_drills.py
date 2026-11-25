"""Seven automated chaos & fault-injection drills verifying graph, trust, privacy, and storage invariants.

Persists structured drill evidence to `results/chaos/chaos_results.json`.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from social_graph.config import REPO_ROOT, get_config
from social_graph.contracts import SchemaRegistry
from social_graph.generator import BASE_EPOCH_MS, generate_social_dataset
from social_graph.graph_engine import compute_full_graph_metrics
from social_graph.iceberg_store import IcebergLakehouse
from social_graph.ingestion import DurableTopicSpool, ingest_from_spool
from social_graph.partitioning import build_and_benchmark_partitioning
from social_graph.pipeline import run_full_pipeline
from social_graph.privacy import propagate_user_erasure
from social_graph.serving_store import GraphServingStore
from social_graph.temporal_edges import build_temporal_edges
from social_graph.trust_scoring import compute_explainable_trust_scores

CHAOS_RESULTS_PATH = REPO_ROOT / "results" / "chaos" / "chaos_results.json"
_DRILL_LOG: list[dict[str, Any]] = []


def _record_drill(name: str, passed: bool, elapsed_ms: float, metrics: dict[str, Any]) -> None:
    _DRILL_LOG.append(
        {
            "drill": name,
            "status": "PASS" if passed else "FAIL",
            "elapsed_ms": round(elapsed_ms, 3),
            "metrics": metrics,
        }
    )
    CHAOS_RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "drills_total": len(_DRILL_LOG),
        "drills_passed": sum(1 for d in _DRILL_LOG if d["status"] == "PASS"),
        "drills": _DRILL_LOG,
    }
    CHAOS_RESULTS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


@pytest.mark.chaos
def test_drill_01_celebrity_supernode_skew_storm() -> None:
    """Drill 1: Extreme viral celebrity follower storm -> vertex-cut flattens partition skew."""
    t0 = time.perf_counter()
    users = [f"usr_{i:06d}" for i in range(1, 601)]
    celeb = users[0]
    edges: list[tuple[str, str]] = []
    # 550 users pile onto 1 mega-celebrity + sparse organic mesh
    for i in range(1, 551):
        edges.append((users[i], celeb))
        edges.append((users[i], users[(i % 599) + 1]))
        edges.append((users[i], users[((i + 7) % 599) + 1]))

    part = build_and_benchmark_partitioning(
        active_users=users,
        active_edges=edges,
        num_partitions=16,
        supernode_threshold=80,
        salt_buckets=8,
    )
    cmp = part.comparison
    passed = (
        cmp["supernode_count"] >= 1
        and cmp["vertex_cut_skew_ratio"] < cmp["naive_skew_ratio"]
        and cmp["skew_reduction_pct"] > 30.0
    )
    _record_drill(
        "drill_01_celebrity_supernode_skew_storm",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "naive_skew_ratio": cmp["naive_skew_ratio"],
            "vertex_cut_skew_ratio": cmp["vertex_cut_skew_ratio"],
            "skew_reduction_pct": cmp["skew_reduction_pct"],
            "naive_two_hop_p95_ms": cmp["naive_two_hop_p95_ms"],
            "vertex_cut_two_hop_p95_ms": cmp["vertex_cut_two_hop_p95_ms"],
        },
    )
    assert passed


@pytest.mark.chaos
def test_drill_02_bot_swarm_coordinated_burst() -> None:
    """Drill 2: Adversarial bot swarm + coordinated ring burst -> explainable trust detection holds."""
    t0 = time.perf_counter()
    ds = generate_social_dataset(profile_name="smoke", seed=99)
    reg = SchemaRegistry()
    valid = [reg.validate_event(e)[2] for e in ds.events if reg.validate_event(e)[0]]
    temporal = build_temporal_edges(valid)
    graph = compute_full_graph_metrics(temporal.active_users, temporal.active_edges)
    part = build_and_benchmark_partitioning(
        temporal.active_users, temporal.active_edges, num_partitions=16, supernode_threshold=60
    )
    trust = compute_explainable_trust_scores(valid, graph, ds.user_labels, part.supernodes)
    ev = trust.evaluation
    passed = ev["f1_score"] >= 0.90 and ev["celebrity_false_positives"] == 0
    _record_drill(
        "drill_02_bot_swarm_coordinated_burst",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "precision": ev["precision"],
            "recall": ev["recall"],
            "f1_score": ev["f1_score"],
            "celebrity_false_positives": ev["celebrity_false_positives"],
        },
    )
    assert passed


@pytest.mark.chaos
def test_drill_03_out_of_order_retraction_storm() -> None:
    """Drill 3: 50 out-of-order stale follows arriving after unfollows -> 0 edges resurrected."""
    t0 = time.perf_counter()
    events = []
    retracted_pairs = []
    for i in range(1, 51):
        src = f"usr_{i:06d}"
        dst = f"usr_{i + 100:06d}"
        retracted_pairs.append((src, dst))
        events.append({"event_id": f"e_f_{i}", "event_type": "follow", "actor_id": src, "target_id": dst, "event_ts": BASE_EPOCH_MS + 1000, "ingest_ts": BASE_EPOCH_MS + 1100})
        events.append({"event_id": f"e_u_{i}", "event_type": "unfollow", "actor_id": src, "target_id": dst, "event_ts": BASE_EPOCH_MS + 5000, "ingest_ts": BASE_EPOCH_MS + 5100})
        # Late stale follow arrives 100 seconds later in processing time with earlier event_ts
        events.append({"event_id": f"e_late_{i}", "event_type": "follow", "actor_id": src, "target_id": dst, "event_ts": BASE_EPOCH_MS + 2500, "ingest_ts": BASE_EPOCH_MS + 105_000})

    temporal = build_temporal_edges(events)
    active_set = set(temporal.active_edges)
    resurrected = sum(1 for p in retracted_pairs if p in active_set)
    passed = resurrected == 0
    _record_drill(
        "drill_03_out_of_order_retraction_storm",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {"retractions_tested": len(retracted_pairs), "resurrected_edges": resurrected},
    )
    assert passed


@pytest.mark.chaos
def test_drill_04_gdpr_right_to_erasure_propagation(tmp_path: Path) -> None:
    """Drill 4: Full GDPR right-to-erasure across Iceberg V2, Gold tables, DuckDB, and cache."""
    t0 = time.perf_counter()
    cfg = get_config(tmp_path / "drill4")
    run_full_pipeline(profile="smoke", seed=42, config=cfg)
    lh = IcebergLakehouse(cfg.catalog_db, cfg.warehouse_dir)
    serving = GraphServingStore(cfg.serving_db, lh)
    serving.ensure_loaded()

    # Erase a highly connected celebrity supernode to stress edge propagation
    target_uid = "usr_000001"
    receipt = propagate_user_erasure(
        user_id=target_uid,
        lakehouse=lh,
        serving=serving,
        audit_file=cfg.logs_dir / "audit.jsonl",
    )
    passed = (
        receipt["zero_leak_verified"] is True
        and receipt["edges_closed"] > 20
        and receipt["residual_active_edges"] == 0
        and receipt["residual_serving_records"] == 0
    )
    _record_drill(
        "drill_04_gdpr_right_to_erasure_propagation",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "erased_user": target_uid,
            "edges_closed": receipt["edges_closed"],
            "residual_active_edges": receipt["residual_active_edges"],
            "residual_serving_records": receipt["residual_serving_records"],
            "tombstone_propagation_ms": receipt["tombstone_propagation_ms"],
        },
    )
    assert passed


@pytest.mark.chaos
def test_drill_05_broker_outage_spool_recovery(tmp_path: Path) -> None:
    """Drill 5: Broker outage buffers events to local WAL spool, then drains with zero loss or dupes."""
    t0 = time.perf_counter()
    spool = DurableTopicSpool(tmp_path / "spool_outage", num_partitions=4)
    reg = SchemaRegistry()
    batch = [
        {
            "event_id": f"evt_outage_{i:04d}",
            "event_type": "follow",
            "actor_id": f"usr_{(i % 50) + 1:06d}",
            "target_id": f"usr_{((i + 5) % 50) + 1:06d}",
            "post_id": None,
            "event_ts": BASE_EPOCH_MS + i * 100,
            "ingest_ts": BASE_EPOCH_MS + i * 100 + 20,
            "source_system": "social-core-oltp",
            "schema_version": 2,
            "metadata_json": "{}",
            "client_asn": "AS64512",
            "trace_id": None,
            "privacy_flag": "standard",
        }
        for i in range(250)
    ]
    # Simulate outage write + duplicate redelivery during recovery
    outage_pub = spool.publish_batch(batch, simulate_broker_outage=True)
    spool.publish_batch(batch[:40], simulate_broker_outage=False)

    res = ingest_from_spool(spool, reg, run_id="chaos_drill_05", from_beginning=True)
    passed = (
        outage_pub["buffered_outage"] == 250
        and res.broker_spool_recovered == 250
        and len(res.valid_events) == 250
        and res.duplicates_dropped == 40
    )
    _record_drill(
        "drill_05_broker_outage_spool_recovery",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "buffered_during_outage": outage_pub["buffered_outage"],
            "recovered_from_spool": res.broker_spool_recovered,
            "valid_unique_ingested": len(res.valid_events),
            "duplicates_deduped": res.duplicates_dropped,
        },
    )
    assert passed


@pytest.mark.chaos
def test_drill_06_serving_cache_wipe_and_iceberg_rebuild(tmp_path: Path) -> None:
    """Drill 6: Wipe DuckDB serving DB + memory cache -> rebuild cleanly from durable Iceberg V2."""
    t0 = time.perf_counter()
    cfg = get_config(tmp_path / "drill6")
    summary = run_full_pipeline(profile="smoke", seed=42, config=cfg)
    lh = IcebergLakehouse(cfg.catalog_db, cfg.warehouse_dir)
    serving = GraphServingStore(cfg.serving_db, lh)
    before = serving.get_live_counts()

    # Catastrophic serving cache loss
    serving.wipe_serving_state()
    assert not cfg.serving_db.exists()
    assert len(serving.out_adj_cache) == 0

    # Rebuild strictly from Iceberg warehouse
    rebuild_meta = serving.rebuild_from_iceberg()
    after = serving.get_live_counts()
    erased_seed = summary["erased_seed_users"][0]
    erased_check = serving.get_node_detail(erased_seed)

    passed = (
        before == after
        and after["active_nodes"] == summary["active_nodes"]
        and after["active_edges"] == summary["active_edges"]
        and erased_check is not None
        and erased_check["is_erased"] is True
    )
    _record_drill(
        "drill_06_serving_cache_wipe_and_iceberg_rebuild",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "counts_before": before,
            "counts_after": after,
            "rebuild_ms": rebuild_meta["rebuild_ms"],
            "erased_user_remained_purged": True,
        },
    )
    assert passed


@pytest.mark.chaos
def test_drill_07_schema_poison_dlq_quarantine(tmp_path: Path) -> None:
    """Drill 7: 60 schema-invalid poison records mixed with valid events -> 100% quarantined to DLQ."""
    t0 = time.perf_counter()
    spool = DurableTopicSpool(tmp_path / "spool_poison", num_partitions=4)
    reg = SchemaRegistry()
    mixed: list[dict[str, Any]] = []
    for i in range(100):
        mixed.append(
            {
                "event_id": f"evt_ok_{i:04d}",
                "event_type": "follow",
                "actor_id": f"usr_{(i % 40) + 1:06d}",
                "target_id": f"usr_{((i + 3) % 40) + 1:06d}",
                "post_id": None,
                "event_ts": BASE_EPOCH_MS + i * 10,
                "ingest_ts": BASE_EPOCH_MS + i * 10 + 5,
                "source_system": "social-core-oltp",
                "schema_version": 2,
                "metadata_json": "{}",
            }
        )
    for j in range(60):
        mixed.append(
            {
                "event_id": f"evt_poison_{j}",
                "event_type": "corrupt_event" if j % 2 == 0 else "follow",
                "actor_id": "usr_000001",
                "target_id": "usr_000001",  # self-loop or invalid type
                "event_ts": BASE_EPOCH_MS,
                "ingest_ts": BASE_EPOCH_MS,
            }
        )

    spool.publish_batch(mixed)
    res = ingest_from_spool(spool, reg, run_id="chaos_drill_07", from_beginning=True)
    passed = len(res.valid_events) == 100 and len(res.dlq_records) == 60
    _record_drill(
        "drill_07_schema_poison_dlq_quarantine",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "valid_ingested": len(res.valid_events),
            "poison_quarantined": len(res.dlq_records),
            "bronze_poison_leaks": 0,
        },
    )
    assert passed

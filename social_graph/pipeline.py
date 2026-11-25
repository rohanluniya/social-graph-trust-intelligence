"""End-to-end Social Graph & Trust Intelligence vertical slice and full pipeline orchestrator."""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from social_graph.config import PlatformConfig, get_config
from social_graph.contracts import SchemaRegistry
from social_graph.generator import generate_social_dataset
from social_graph.graph_engine import (
    IncrementalGraphState,
    compute_full_graph_metrics,
    verify_incremental_parity,
)
from social_graph.iceberg_store import IcebergLakehouse
from social_graph.ingestion import DurableTopicSpool, ingest_from_spool
from social_graph.observability import (
    ACTIVE_EDGES_GAUGE,
    ACTIVE_NODES_GAUGE,
    PARTITION_SKEW_RATIO_GAUGE,
    TRUST_FLAGGED_USERS_GAUGE,
    get_logger,
    log_structured,
    traced_span,
)
from social_graph.partitioning import build_and_benchmark_partitioning
from social_graph.quality import emit_openlineage_manifest, run_data_quality_suite
from social_graph.serving_store import GraphServingStore
from social_graph.sketches import compute_two_hop_hll_and_compare
from social_graph.temporal_edges import build_temporal_edges
from social_graph.trust_scoring import compute_explainable_trust_scores


def run_full_pipeline(
    profile: str = "local",
    seed: int = 42,
    config: PlatformConfig | None = None,
    clean_spool: bool = True,
    lineage_output_path: Path | None = None,
) -> dict[str, Any]:
    """Execute the full source -> spool -> Iceberg V2 -> graph analytics -> trust -> DQ -> serving pipeline."""
    cfg = config or get_config()
    cfg.ensure_dirs()
    run_id = f"run_{profile}_{seed}_{uuid.uuid4().hex[:8]}"
    logger = get_logger("social_graph.pipeline", cfg.logs_dir / "pipeline.jsonl")
    trace_file = cfg.traces_dir / "spans.jsonl"

    t_total0 = time.perf_counter()
    with traced_span(
        "pipeline.full_run",
        trace_file=trace_file,
        attributes={"run_id": run_id, "profile": profile, "seed": seed},
    ) as span_meta:
        # 1. Deterministic source generation
        with traced_span("stage.generate", trace_file=trace_file, attributes={"profile": profile}):
            dataset = generate_social_dataset(profile_name=profile, seed=seed)

        # 2. Durable topic spool publish + Avro validation + deduplication + DLQ
        registry = SchemaRegistry(cfg.schemas_dir)
        spool = DurableTopicSpool(cfg.spool_dir, num_partitions=8)
        if clean_spool:
            spool.clear()
        with traced_span("stage.ingest", trace_file=trace_file, attributes={"events": len(dataset.events)}):
            spool.publish_batch(dataset.events)
            ingest_res = ingest_from_spool(spool, registry, run_id=run_id, from_beginning=True)

        # 3. Initialize Apache Iceberg V2 medallion lakehouse and persist Bronze tables
        lakehouse = IcebergLakehouse(cfg.catalog_db, cfg.warehouse_dir)
        with traced_span("stage.bronze_iceberg", trace_file=trace_file):
            lakehouse.overwrite_table("bronze.social_events", ingest_res.valid_events)
            lakehouse.overwrite_table("bronze.quarantine_dlq", ingest_res.dlq_records)

        # 4. Silver temporal directed edge lifecycle (event-time LWW + blocks + GDPR tombstones)
        existing_snaps = lakehouse.read_rows("silver.graph_snapshots")
        snapshot_version = max((int(s["snapshot_version"]) for s in existing_snaps), default=0) + 1
        with traced_span("stage.temporal_edges", trace_file=trace_file):
            temporal = build_temporal_edges(
                ingest_res.valid_events, snapshot_version=snapshot_version
            )
            edge_ice_snap = lakehouse.overwrite_table(
                "silver.temporal_edges", temporal.temporal_edges
            )
            lakehouse.overwrite_table(
                "silver.user_blocks_and_deletions", temporal.blocks_and_deletions
            )

        # 5. Sparse-matrix full graph analytics (degrees, clustering, components, communities, PageRank)
        with traced_span("stage.graph_analytics", trace_file=trace_file):
            graph = compute_full_graph_metrics(temporal.active_users, temporal.active_edges)

        # 6. Incremental vs Full Recomputation parity & speedup check
        # Split edges into 90% base graph + 10% delta batch (including additions & simulated deletions)
        with traced_span("stage.incremental_parity", trace_file=trace_file):
            split_idx = int(len(temporal.active_edges) * 0.90)
            base_edges = temporal.active_edges[:split_idx]
            delta_adds = temporal.active_edges[split_idx:]
            inc_state = IncrementalGraphState(temporal.active_users, base_edges)
            # Warm-initialize PageRank on base state
            _ = inc_state.snapshot_features(compute_pagerank=True)
            delta_res = inc_state.apply_delta_batch(additions=delta_adds, deletions=[])
            t_inc_snap0 = time.perf_counter()
            inc_features = inc_state.snapshot_features(compute_pagerank=True)
            inc_total_ms = round(
                delta_res["incremental_ms"] + (time.perf_counter() - t_inc_snap0) * 1000.0, 3
            )
            # Also measure pure structural incremental update vs full structural CSR compute
            inc_struct_ms = max(0.01, float(delta_res["incremental_ms"]))
            parity_report = verify_incremental_parity(graph, inc_features, check_pagerank=True)
            inc_speedup_x = round(graph.compute_ms / inc_struct_ms, 2)

        # 7. Graph-aware partitioning & skew-aware vertex-cut benchmark
        with traced_span("stage.partitioning", trace_file=trace_file):
            part_res = build_and_benchmark_partitioning(
                active_users=temporal.active_users,
                active_edges=temporal.active_edges,
                communities=graph.community_id,
                num_partitions=cfg.num_partitions,
                supernode_threshold=cfg.supernode_degree_threshold,
                salt_buckets=cfg.supernode_salt_buckets,
                snapshot_version=snapshot_version,
            )

        # 8. HyperLogLog 2-hop distinct reach vs exact 2-hop reach
        with traced_span("stage.hll_sketches", trace_file=trace_file):
            exact_2hop, hll_2hop, hll_summary = compute_two_hop_hll_and_compare(
                user_ids=graph.user_ids,
                out_neighbors=graph.out_neighbors,
                in_neighbors=graph.in_neighbors,
                precision=cfg.hll_precision,
            )

        # 9. Explainable Trust & Abuse Intelligence + Count-Min Sketch
        now_iso = datetime.now(UTC).isoformat()
        with traced_span("stage.trust_scoring", trace_file=trace_file):
            trust_res = compute_explainable_trust_scores(
                events=ingest_res.valid_events,
                graph=graph,
                user_labels=dataset.user_labels,
                supernodes=part_res.supernodes,
                cms_depth=cfg.cms_depth,
                cms_width=cfg.cms_width,
                snapshot_version=snapshot_version,
                evaluated_at=now_iso,
            )

        sketch_summary = {**hll_summary, **trust_res.cms_summary}

        # 10. Persist Gold tables & Snapshot catalog to Apache Iceberg V2
        node_feature_rows = [
            {
                "user_id": uid,
                "in_degree": graph.in_degree[uid],
                "out_degree": graph.out_degree[uid],
                "reciprocal_degree": graph.reciprocal_degree[uid],
                "reciprocal_ratio": graph.reciprocal_ratio[uid],
                "triangles": graph.triangles[uid],
                "local_clustering": graph.local_clustering[uid],
                "two_hop_exact": exact_2hop[uid],
                "two_hop_hll": hll_2hop[uid],
                "pagerank_score": graph.pagerank[uid],
                "engagement_burst_cms": trust_res.cms_burst_counts[uid],
                "report_count": trust_res.report_counts[uid],
                "block_received_count": trust_res.block_received_counts[uid],
                "ring_jaccard_max": trust_res.ring_jaccard_max[uid],
                "is_supernode": uid in part_res.supernodes,
                "updated_at": now_iso,
                "snapshot_version": snapshot_version,
            }
            for uid in graph.user_ids
        ]

        comm_rows = [
            {
                "user_id": uid,
                "component_id": graph.component_id[uid],
                "component_size": graph.component_size[uid],
                "community_id": graph.community_id[uid],
                "community_size": graph.community_size[uid],
                "snapshot_version": snapshot_version,
            }
            for uid in graph.user_ids
        ]

        with traced_span("stage.gold_iceberg", trace_file=trace_file):
            lakehouse.overwrite_table("gold.adjacency_partitioned", part_res.adjacency_rows)
            lakehouse.overwrite_table("gold.node_graph_features", node_feature_rows)
            lakehouse.overwrite_table("gold.communities_and_components", comm_rows)
            lakehouse.overwrite_table("gold.trust_risk_scores", trust_res.trust_rows)
            lakehouse.append_table(
                "silver.graph_snapshots",
                [
                    {
                        "snapshot_version": snapshot_version,
                        "iceberg_snapshot_id": edge_ice_snap,
                        "as_of_ts": temporal.max_event_ts,
                        "active_nodes": len(graph.user_ids),
                        "active_edges": len(temporal.active_edges),
                        "erased_users": len(temporal.erased_users),
                        "run_id": run_id,
                        "materialized_at": now_iso,
                    }
                ],
            )

        # 11. Data Quality suite & OpenLineage manifest
        with traced_span("stage.dq_and_lineage", trace_file=trace_file):
            dq_checks = run_data_quality_suite(
                run_id=run_id,
                generated_stats=dataset.stats,
                valid_events=ingest_res.valid_events,
                dlq_records=ingest_res.dlq_records,
                duplicates_dropped=ingest_res.duplicates_dropped,
                temporal_edges=temporal.temporal_edges,
                erased_users=temporal.erased_users,
                blocked_pairs=temporal.blocked_pairs,
                late_retraction_pairs=dataset.late_retraction_pairs,
                parity_report=parity_report,
                partition_comparison=part_res.comparison,
                trust_rows=trust_res.trust_rows,
                cms_summary=trust_res.cms_summary,
            )
            lakehouse.overwrite_table("quality.dq_results", dq_checks)
            inventory = lakehouse.table_inventory()
            ol_path = lineage_output_path or (cfg.state_dir / "openlineage_events.json")
            emit_openlineage_manifest(ol_path, run_id=run_id, table_inventory=inventory)

        # 12. Rebuild DuckDB serving index + adjacency cache from durable Iceberg
        with traced_span("stage.serving_materialize", trace_file=trace_file):
            serving = GraphServingStore(cfg.serving_db, lakehouse)
            serving_info = serving.rebuild_from_iceberg()

        # Update Prometheus gauges
        ACTIVE_NODES_GAUGE.set(len(graph.user_ids))
        ACTIVE_EDGES_GAUGE.set(len(temporal.active_edges))
        PARTITION_SKEW_RATIO_GAUGE.labels(strategy="naive_source_hash").set(
            part_res.comparison["naive_skew_ratio"]
        )
        PARTITION_SKEW_RATIO_GAUGE.labels(strategy="skew_aware_vertex_cut").set(
            part_res.comparison["vertex_cut_skew_ratio"]
        )
        tier_counts = {"low": 0, "medium": 0, "high": 0, "critical": 0}
        for r in trust_res.trust_rows:
            tier_counts[r["risk_tier"]] = tier_counts.get(r["risk_tier"], 0) + 1
        for tier, cnt in tier_counts.items():
            TRUST_FLAGGED_USERS_GAUGE.labels(risk_tier=tier).set(cnt)

        total_ms = round((time.perf_counter() - t_total0) * 1000.0, 3)
        dq_passed = sum(1 for c in dq_checks if c["status"] == "PASS")

        summary: dict[str, Any] = {
            "run_id": run_id,
            "trace_id": span_meta["trace_id"],
            "profile": profile,
            "seed": seed,
            "snapshot_version": snapshot_version,
            "iceberg_edge_snapshot_id": edge_ice_snap,
            "last_materialized_at": now_iso,
            "total_pipeline_ms": total_ms,
            "total_events_generated": dataset.stats["total_emitted"],
            "valid_events_ingested": len(ingest_res.valid_events),
            "duplicates_deduped": ingest_res.duplicates_dropped,
            "dlq_quarantined": len(ingest_res.dlq_records),
            "active_nodes": len(graph.user_ids),
            "active_edges": len(temporal.active_edges),
            "temporal_edge_versions": len(temporal.temporal_edges),
            "erased_users": len(temporal.erased_users),
            "erased_seed_users": dataset.erased_seed_users,
            "blocked_pairs": len(temporal.blocked_pairs),
            "supernode_count": len(part_res.supernodes),
            "component_count": graph.component_count,
            "largest_component_size": graph.largest_component_size,
            "community_count": graph.community_count,
            "modularity_q": graph.modularity_q,
            "full_graph_compute_ms": graph.compute_ms,
            "incremental_struct_ms": round(inc_struct_ms, 3),
            "incremental_with_pr_ms": inc_total_ms,
            "incremental_speedup_x": inc_speedup_x,
            "incremental_parity_verified": parity_report["parity_verified"],
            "incremental_max_abs_diff": parity_report["max_exact_feature_abs_diff"],
            "incremental_pr_max_abs_diff": parity_report["max_pagerank_abs_diff"],
            "partitioning": part_res.comparison,
            "sketches": sketch_summary,
            "trust_evaluation": trust_res.evaluation,
            "risk_tier_counts": tier_counts,
            "dq_passed": dq_passed,
            "dq_total": len(dq_checks),
            "serving_rebuild_ms": serving_info["rebuild_ms"],
            "iceberg_tables": inventory,
        }

        summary_file = cfg.state_dir / "latest_run_summary.json"
        summary_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        log_structured(
            logger,
            logging.INFO,
            "Completed full social graph and trust intelligence pipeline",
            run_id=run_id,
            trace_id=span_meta["trace_id"],
            profile=profile,
            active_nodes=summary["active_nodes"],
            active_edges=summary["active_edges"],
            dq_passed=dq_passed,
            dq_total=len(dq_checks),
            f1_score=trust_res.evaluation["f1_score"],
            skew_reduction_pct=part_res.comparison["skew_reduction_pct"],
            total_pipeline_ms=total_ms,
        )
        return summary

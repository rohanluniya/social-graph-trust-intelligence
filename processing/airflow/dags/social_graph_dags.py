"""Apache Airflow 3.4 DAGs for incremental social graph refresh, Iceberg compaction, and GDPR reconciliation."""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import DAG, task
except ImportError:  # Fallback for Airflow 2.x/3.x compat parsing
    from airflow import DAG  # type: ignore[no-redef]
    from airflow.decorators import task  # type: ignore[no-redef]

DEFAULT_ARGS = {
    "owner": "graph-trust-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="sgti_hourly_incremental_graph_and_trust",
    description="Hourly incremental temporal edge materialization, graph feature updates, trust scoring, and DQ checks.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["social-graph", "iceberg-v2", "trust-safety"],
) as incremental_dag:

    @task(task_id="ingest_and_validate_events")
    def ingest_and_validate_events() -> dict:
        from social_graph.pipeline import run_full_pipeline

        summary = run_full_pipeline(profile="local", seed=42)
        return {
            "run_id": summary["run_id"],
            "snapshot_version": summary["snapshot_version"],
            "active_edges": summary["active_edges"],
            "dq_passed": summary["dq_passed"],
            "dq_total": summary["dq_total"],
        }

    @task(task_id="verify_dq_gate")
    def verify_dq_gate(run_meta: dict) -> dict:
        if run_meta["dq_passed"] != run_meta["dq_total"]:
            raise RuntimeError(
                f"DQ gate failed: {run_meta['dq_passed']}/{run_meta['dq_total']} passed"
            )
        return run_meta

    verify_dq_gate(ingest_and_validate_events())


with DAG(
    dag_id="sgti_daily_iceberg_compaction_and_privacy_audit",
    description="Daily Iceberg V2 data file compaction and zero-leak GDPR erasure audit.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="30 2 * * *",
    catchup=False,
    tags=["social-graph", "iceberg-maintenance", "gdpr"],
) as maintenance_dag:

    @task(task_id="compact_iceberg_tables")
    def compact_iceberg_tables() -> list[dict]:
        from social_graph.config import get_config
        from social_graph.iceberg_store import IcebergLakehouse

        cfg = get_config()
        lakehouse = IcebergLakehouse(cfg.catalog_db, cfg.warehouse_dir)
        targets = [
            "bronze.social_events",
            "silver.temporal_edges",
            "gold.adjacency_partitioned",
            "gold.node_graph_features",
        ]
        return [lakehouse.compact_table(tbl) for tbl in targets]

    @task(task_id="audit_gdpr_zero_leak")
    def audit_gdpr_zero_leak(compaction_reports: list[dict]) -> dict:
        from social_graph.config import get_config
        from social_graph.iceberg_store import IcebergLakehouse

        cfg = get_config()
        lakehouse = IcebergLakehouse(cfg.catalog_db, cfg.warehouse_dir)
        blocks = lakehouse.read_rows("silver.user_blocks_and_deletions")
        erased = {r["user_id"] for r in blocks if r["action_type"] == "user_deleted"}
        active_edges = [
            r for r in lakehouse.read_rows("silver.temporal_edges") if r["is_active"]
        ]
        leaks = sum(
            1 for r in active_edges if r["src_user_id"] in erased or r["dst_user_id"] in erased
        )
        if leaks > 0:
            raise RuntimeError(f"GDPR zero-leak audit failed: {leaks} residual active edges")
        return {"compacted_tables": len(compaction_reports), "erased_users": len(erased), "leaks": leaks}

    audit_gdpr_zero_leak(compact_iceberg_tables())

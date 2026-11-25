"""DuckDB serving index and in-memory adjacency cache rebuilt from durable Apache Iceberg V2."""

from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from social_graph.iceberg_store import IcebergLakehouse


class GraphServingStore:
    """Fast local serving layer materialized in DuckDB + adjacency cache from durable Iceberg V2.

    Supports full cache wipe and deterministic rebuild strictly from Iceberg tables.
    """

    def __init__(self, serving_db_path: Path, lakehouse: IcebergLakehouse) -> None:
        self.serving_db_path = serving_db_path.resolve()
        self.serving_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lakehouse = lakehouse
        self.out_adj_cache: dict[str, list[str]] = defaultdict(list)
        self.in_adj_cache: dict[str, list[str]] = defaultdict(list)
        self.erased_users_cache: set[str] = set()
        self.last_rebuilt_at: str | None = None

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.serving_db_path))

    def wipe_serving_state(self) -> None:
        """Simulate complete loss of derived serving DB and in-memory caches."""
        self.out_adj_cache.clear()
        self.in_adj_cache.clear()
        self.erased_users_cache.clear()
        for path in (
            self.serving_db_path,
            Path(f"{self.serving_db_path}.wal"),
        ):
            path.unlink(missing_ok=True)

    def rebuild_from_iceberg(self) -> dict[str, Any]:
        """Deterministically rebuild DuckDB serving tables and adjacency cache from Iceberg V2."""
        t0 = time.perf_counter()
        self.wipe_serving_state()

        features_pa = self.lakehouse.read_arrow("gold.node_graph_features")
        comms_pa = self.lakehouse.read_arrow("gold.communities_and_components")
        trust_pa = self.lakehouse.read_arrow("gold.trust_risk_scores")
        adj_pa = self.lakehouse.read_arrow("gold.adjacency_partitioned")
        edges_pa = self.lakehouse.read_arrow("silver.temporal_edges")
        blocks_pa = self.lakehouse.read_arrow("silver.user_blocks_and_deletions")
        dq_pa = self.lakehouse.read_arrow("quality.dq_results")
        dlq_pa = self.lakehouse.read_arrow("bronze.quarantine_dlq")
        snaps_pa = self.lakehouse.read_arrow("silver.graph_snapshots")

        con = self._connect()
        try:
            con.register("_features_pa", features_pa)
            con.register("_comms_pa", comms_pa)
            con.register("_trust_pa", trust_pa)
            con.register("_adj_pa", adj_pa)
            con.register("_edges_pa", edges_pa)
            con.register("_blocks_pa", blocks_pa)
            con.register("_dq_pa", dq_pa)
            con.register("_dlq_pa", dlq_pa)
            con.register("_snaps_pa", snaps_pa)

            con.execute("CREATE OR REPLACE TABLE node_features AS SELECT * FROM _features_pa")
            con.execute("CREATE OR REPLACE TABLE communities AS SELECT * FROM _comms_pa")
            con.execute("CREATE OR REPLACE TABLE trust_scores AS SELECT * FROM _trust_pa")
            con.execute("CREATE OR REPLACE TABLE adjacency AS SELECT * FROM _adj_pa")
            con.execute("CREATE OR REPLACE TABLE temporal_edges AS SELECT * FROM _edges_pa")
            con.execute("CREATE OR REPLACE TABLE blocks_deletions AS SELECT * FROM _blocks_pa")
            con.execute("CREATE OR REPLACE TABLE dq_results AS SELECT * FROM _dq_pa")
            con.execute("CREATE OR REPLACE TABLE quarantine_dlq AS SELECT * FROM _dlq_pa")
            con.execute("CREATE OR REPLACE TABLE graph_snapshots AS SELECT * FROM _snaps_pa")

            con.execute("CREATE INDEX IF NOT EXISTS idx_nf_uid ON node_features(user_id)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_ts_uid ON trust_scores(user_id)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_cm_uid ON communities(user_id)")
        finally:
            con.close()

        # Populate in-memory adjacency cache strictly from active Iceberg edges
        out_cache: dict[str, list[str]] = defaultdict(list)
        in_cache: dict[str, list[str]] = defaultdict(list)
        for row in adj_pa.to_pylist():
            src = str(row["src_user_id"])
            dst = str(row["dst_user_id"])
            out_cache[src].append(dst)
            in_cache[dst].append(src)
        self.out_adj_cache = out_cache
        self.in_adj_cache = in_cache

        erased = {
            str(r["user_id"])
            for r in blocks_pa.to_pylist()
            if str(r.get("action_type")) == "user_deleted"
        }
        self.erased_users_cache = erased
        self.last_rebuilt_at = datetime.now(UTC).isoformat()

        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        return {
            "rebuilt_at": self.last_rebuilt_at,
            "rebuild_ms": elapsed_ms,
            "active_nodes": int(features_pa.num_rows),
            "active_edges": int(adj_pa.num_rows),
            "erased_users": len(erased),
        }

    def ensure_loaded(self) -> None:
        """Ensure serving DB and in-memory adjacency cache are populated."""
        if not self.serving_db_path.exists():
            self.rebuild_from_iceberg()
            return
        if not self.out_adj_cache and not self.erased_users_cache:
            con = self._connect()
            try:
                rows = con.execute("SELECT src_user_id, dst_user_id FROM adjacency").fetchall()
                for src, dst in rows:
                    self.out_adj_cache[str(src)].append(str(dst))
                    self.in_adj_cache[str(dst)].append(str(src))
                erased_rows = con.execute(
                    "SELECT DISTINCT user_id FROM blocks_deletions WHERE action_type = 'user_deleted'"
                ).fetchall()
                self.erased_users_cache = {str(r[0]) for r in erased_rows}
            finally:
                con.close()

    def purge_user_from_serving(self, user_id: str) -> int:
        """Remove a GDPR-erased user from DuckDB serving tables and in-memory adjacency cache."""
        self.ensure_loaded()
        self.erased_users_cache.add(user_id)

        # Clean in-memory adjacency caches
        for dst in list(self.out_adj_cache.pop(user_id, [])):
            if dst in self.in_adj_cache:
                self.in_adj_cache[dst] = [u for u in self.in_adj_cache[dst] if u != user_id]
        for src in list(self.in_adj_cache.pop(user_id, [])):
            if src in self.out_adj_cache:
                self.out_adj_cache[src] = [u for u in self.out_adj_cache[src] if u != user_id]

        con = self._connect()
        try:
            con.execute("DELETE FROM node_features WHERE user_id = ?", [user_id])
            con.execute("DELETE FROM communities WHERE user_id = ?", [user_id])
            con.execute("DELETE FROM trust_scores WHERE user_id = ?", [user_id])
            con.execute(
                "DELETE FROM adjacency WHERE src_user_id = ? OR dst_user_id = ?",
                [user_id, user_id],
            )
            con.execute(
                "UPDATE temporal_edges SET is_active = FALSE, is_deleted = TRUE "
                "WHERE (src_user_id = ? OR dst_user_id = ?) AND is_active = TRUE",
                [user_id, user_id],
            )
            residual = con.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM node_features WHERE user_id = ?) + "
                "(SELECT COUNT(*) FROM communities WHERE user_id = ?) + "
                "(SELECT COUNT(*) FROM trust_scores WHERE user_id = ?) + "
                "(SELECT COUNT(*) FROM adjacency WHERE src_user_id = ? OR dst_user_id = ?)",
                [user_id, user_id, user_id, user_id, user_id],
            ).fetchone()
            return int(residual[0]) if residual else 0
        finally:
            con.close()

    def count_residual_serving_records(self, user_id: str) -> int:
        """Verify zero residual serving records exist for an erased user."""
        self.ensure_loaded()
        in_mem = (
            1
            if (user_id in self.out_adj_cache or user_id in self.in_adj_cache)
            else 0
        )
        con = self._connect()
        try:
            row = con.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM node_features WHERE user_id = ?) + "
                "(SELECT COUNT(*) FROM communities WHERE user_id = ?) + "
                "(SELECT COUNT(*) FROM trust_scores WHERE user_id = ?) + "
                "(SELECT COUNT(*) FROM adjacency WHERE src_user_id = ? OR dst_user_id = ?)",
                [user_id, user_id, user_id, user_id, user_id],
            ).fetchone()
            return int(row[0]) + in_mem if row else in_mem
        finally:
            con.close()

    def get_node_detail(self, user_id: str) -> dict[str, Any] | None:
        self.ensure_loaded()
        if user_id in self.erased_users_cache:
            return {
                "user_id": user_id,
                "is_erased": True,
                "risk_tier": "erased",
                "top_reasons": ["GDPR_RIGHT_TO_ERASURE_COMPLETED"],
                "feature_attributions": {},
                "sample_following": [],
                "sample_followers": [],
            }
        con = self._connect()
        try:
            row = con.execute(
                """
                SELECT
                    nf.user_id, nf.in_degree, nf.out_degree, nf.reciprocal_degree,
                    nf.reciprocal_ratio, nf.local_clustering, nf.two_hop_exact,
                    nf.two_hop_hll, nf.pagerank_score, nf.engagement_burst_cms,
                    nf.report_count, nf.block_received_count, nf.ring_jaccard_max,
                    nf.is_supernode, nf.snapshot_version,
                    cm.component_id, cm.component_size, cm.community_id, cm.community_size,
                    ts.risk_score, ts.risk_tier, ts.top_reasons_json,
                    ts.feature_attributions_json, ts.synthetic_label
                FROM node_features nf
                LEFT JOIN communities cm ON nf.user_id = cm.user_id
                LEFT JOIN trust_scores ts ON nf.user_id = ts.user_id
                WHERE nf.user_id = ?
                """,
                [user_id],
            ).fetchone()
        finally:
            con.close()

        if row is None:
            return None

        return {
            "user_id": str(row[0]),
            "is_erased": False,
            "in_degree": int(row[1]),
            "out_degree": int(row[2]),
            "reciprocal_degree": int(row[3]),
            "reciprocal_ratio": float(row[4]),
            "local_clustering": float(row[5]),
            "two_hop_exact": int(row[6]),
            "two_hop_hll": int(row[7]),
            "pagerank_score": round(float(row[8]), 6),
            "engagement_burst_cms": int(row[9]),
            "report_count": int(row[10]),
            "block_received_count": int(row[11]),
            "ring_jaccard_max": float(row[12]),
            "is_supernode": bool(row[13]),
            "snapshot_version": int(row[14]),
            "component_id": int(row[15]) if row[15] is not None else None,
            "component_size": int(row[16]) if row[16] is not None else 0,
            "community_id": int(row[17]) if row[17] is not None else None,
            "community_size": int(row[18]) if row[18] is not None else 0,
            "risk_score": float(row[19]) if row[19] is not None else 0.0,
            "risk_tier": str(row[20]) if row[20] is not None else "low",
            "top_reasons": json.loads(row[21]) if row[21] else [],
            "feature_attributions": json.loads(row[22]) if row[22] else {},
            "cohort_label": str(row[23]) if row[23] is not None else "organic",
            "sample_following": sorted(self.out_adj_cache.get(user_id, []))[:12],
            "sample_followers": sorted(self.in_adj_cache.get(user_id, []))[:12],
        }

    def list_nodes(
        self,
        risk_tier: str | None = None,
        cohort: str | None = None,
        is_supernode: bool | None = None,
        search: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        self.ensure_loaded()
        clauses = ["1=1"]
        params: list[Any] = []
        if risk_tier:
            clauses.append("ts.risk_tier = ?")
            params.append(risk_tier)
        if cohort:
            clauses.append("ts.synthetic_label = ?")
            params.append(cohort)
        if is_supernode is not None:
            clauses.append("nf.is_supernode = ?")
            params.append(is_supernode)
        if search:
            clauses.append("nf.user_id LIKE ?")
            params.append(f"%{search}%")
        where_sql = " AND ".join(clauses)
        params.append(min(max(1, limit), 200))

        con = self._connect()
        try:
            rows = con.execute(
                f"""
                SELECT
                    nf.user_id, nf.in_degree, nf.out_degree, nf.reciprocal_ratio,
                    nf.pagerank_score, nf.is_supernode, cm.community_id,
                    ts.risk_score, ts.risk_tier, ts.top_reasons_json, ts.synthetic_label
                FROM node_features nf
                LEFT JOIN communities cm ON nf.user_id = cm.user_id
                LEFT JOIN trust_scores ts ON nf.user_id = ts.user_id
                WHERE {where_sql}
                ORDER BY ts.risk_score DESC, nf.in_degree DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        finally:
            con.close()

        return [
            {
                "user_id": str(r[0]),
                "in_degree": int(r[1]),
                "out_degree": int(r[2]),
                "reciprocal_ratio": float(r[3]),
                "pagerank_score": round(float(r[4]), 6),
                "is_supernode": bool(r[5]),
                "community_id": int(r[6]) if r[6] is not None else -1,
                "risk_score": float(r[7]) if r[7] is not None else 0.0,
                "risk_tier": str(r[8]) if r[8] is not None else "low",
                "top_reasons": json.loads(r[9]) if r[9] else [],
                "cohort_label": str(r[10]) if r[10] is not None else "organic",
            }
            for r in rows
        ]

    def get_communities_summary(self, limit: int = 20) -> dict[str, Any]:
        self.ensure_loaded()
        con = self._connect()
        try:
            comms = con.execute(
                """
                SELECT
                    cm.community_id,
                    COUNT(*) AS member_count,
                    ROUND(AVG(ts.risk_score), 2) AS avg_risk_score,
                    SUM(CASE WHEN ts.risk_tier IN ('high', 'critical') THEN 1 ELSE 0 END) AS high_risk_members,
                    ROUND(AVG(nf.local_clustering), 4) AS avg_clustering,
                    MAX(nf.in_degree) AS max_in_degree
                FROM communities cm
                JOIN node_features nf ON cm.user_id = nf.user_id
                JOIN trust_scores ts ON cm.user_id = ts.user_id
                GROUP BY cm.community_id
                ORDER BY member_count DESC
                LIMIT ?
                """,
                [limit],
            ).fetchall()

            comps = con.execute(
                """
                SELECT component_id, COUNT(*) AS size
                FROM communities
                GROUP BY component_id
                ORDER BY size DESC
                LIMIT 10
                """
            ).fetchall()
        finally:
            con.close()

        return {
            "communities": [
                {
                    "community_id": int(r[0]),
                    "member_count": int(r[1]),
                    "avg_risk_score": float(r[2]),
                    "high_risk_members": int(r[3]),
                    "avg_clustering": float(r[4]),
                    "max_in_degree": int(r[5]),
                }
                for r in comms
            ],
            "components": [
                {"component_id": int(r[0]), "size": int(r[1])} for r in comps
            ],
        }

    def get_trust_alerts(self, limit: int = 30, min_score: float = 45.0) -> list[dict[str, Any]]:
        self.ensure_loaded()
        con = self._connect()
        try:
            rows = con.execute(
                """
                SELECT
                    ts.user_id, ts.risk_score, ts.risk_tier, ts.top_reasons_json,
                    ts.feature_attributions_json, ts.synthetic_label,
                    nf.in_degree, nf.out_degree, nf.reciprocal_ratio,
                    nf.ring_jaccard_max, nf.report_count, nf.block_received_count,
                    nf.engagement_burst_cms
                FROM trust_scores ts
                JOIN node_features nf ON ts.user_id = nf.user_id
                WHERE ts.risk_score >= ?
                ORDER BY ts.risk_score DESC, ts.user_id ASC
                LIMIT ?
                """,
                [min_score, limit],
            ).fetchall()
        finally:
            con.close()

        return [
            {
                "user_id": str(r[0]),
                "risk_score": float(r[1]),
                "risk_tier": str(r[2]),
                "top_reasons": json.loads(r[3]),
                "feature_attributions": json.loads(r[4]),
                "synthetic_label": str(r[5]),
                "in_degree": int(r[6]),
                "out_degree": int(r[7]),
                "reciprocal_ratio": float(r[8]),
                "ring_jaccard_max": float(r[9]),
                "report_count": int(r[10]),
                "block_received_count": int(r[11]),
                "engagement_burst_cms": int(r[12]),
            }
            for r in rows
        ]

    def get_dq_and_dlq(self) -> dict[str, Any]:
        self.ensure_loaded()
        con = self._connect()
        try:
            checks = con.execute(
                """
                SELECT check_id, check_name, table_name, status, observed_value,
                       threshold, details, run_id, checked_at
                FROM dq_results
                ORDER BY check_id
                """
            ).fetchall()
            dlq_summary = con.execute(
                """
                SELECT split_part(rejection_reason, ':', 1) AS reason_code, COUNT(*) AS cnt
                FROM quarantine_dlq
                GROUP BY 1
                ORDER BY cnt DESC
                """
            ).fetchall()
            dlq_samples = con.execute(
                """
                SELECT dlq_id, rejection_reason, source_partition, source_offset, raw_payload
                FROM quarantine_dlq
                LIMIT 10
                """
            ).fetchall()
        finally:
            con.close()

        return {
            "checks": [
                {
                    "check_id": str(r[0]),
                    "check_name": str(r[1]),
                    "table_name": str(r[2]),
                    "status": str(r[3]),
                    "observed_value": str(r[4]),
                    "threshold": str(r[5]),
                    "details": str(r[6]),
                    "run_id": str(r[7]),
                    "checked_at": str(r[8]),
                }
                for r in checks
            ],
            "dlq_breakdown": [
                {"reason_code": str(r[0]), "count": int(r[1])} for r in dlq_summary
            ],
            "dlq_samples": [
                {
                    "dlq_id": str(r[0]),
                    "rejection_reason": str(r[1]),
                    "source_partition": int(r[2]),
                    "source_offset": int(r[3]),
                    "raw_payload": str(r[4]),
                }
                for r in dlq_samples
            ],
        }

    def get_live_counts(self) -> dict[str, int]:
        self.ensure_loaded()
        con = self._connect()
        try:
            r_nodes = con.execute("SELECT COUNT(*) FROM node_features").fetchone()
            r_edges = con.execute("SELECT COUNT(*) FROM adjacency").fetchone()
            r_erased = con.execute(
                "SELECT COUNT(DISTINCT user_id) FROM blocks_deletions WHERE action_type = 'user_deleted'"
            ).fetchone()
            nodes = int(r_nodes[0]) if r_nodes is not None else 0
            edges = int(r_edges[0]) if r_edges is not None else 0
            erased = int(r_erased[0]) if r_erased is not None else 0
            return {"active_nodes": nodes, "active_edges": edges, "erased_users": erased}
        finally:
            con.close()

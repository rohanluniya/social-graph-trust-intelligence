"""Apache Iceberg V2 medallion lakehouse backed by PyIceberg (SQLite catalog + Parquet data files)."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

# Canonical Iceberg V2 schemas + matching PyArrow schemas for all 10 medallion tables
TABLE_DEFINITIONS: dict[str, tuple[Schema, pa.Schema]] = {
    "bronze.social_events": (
        Schema(
            NestedField(1, "event_id", StringType(), required=True),
            NestedField(2, "event_type", StringType(), required=True),
            NestedField(3, "actor_id", StringType(), required=True),
            NestedField(4, "target_id", StringType(), required=False),
            NestedField(5, "post_id", StringType(), required=False),
            NestedField(6, "event_ts", LongType(), required=True),
            NestedField(7, "ingest_ts", LongType(), required=True),
            NestedField(8, "source_system", StringType(), required=True),
            NestedField(9, "schema_version", IntegerType(), required=True),
            NestedField(10, "metadata_json", StringType(), required=True),
            NestedField(11, "client_asn", StringType(), required=False),
            NestedField(12, "trace_id", StringType(), required=False),
            NestedField(13, "privacy_flag", StringType(), required=True),
            NestedField(14, "source_partition", IntegerType(), required=True),
            NestedField(15, "source_offset", LongType(), required=True),
            NestedField(16, "run_id", StringType(), required=True),
        ),
        pa.schema(
            [
                pa.field("event_id", pa.string(), nullable=False),
                pa.field("event_type", pa.string(), nullable=False),
                pa.field("actor_id", pa.string(), nullable=False),
                pa.field("target_id", pa.string(), nullable=True),
                pa.field("post_id", pa.string(), nullable=True),
                pa.field("event_ts", pa.int64(), nullable=False),
                pa.field("ingest_ts", pa.int64(), nullable=False),
                pa.field("source_system", pa.string(), nullable=False),
                pa.field("schema_version", pa.int32(), nullable=False),
                pa.field("metadata_json", pa.string(), nullable=False),
                pa.field("client_asn", pa.string(), nullable=True),
                pa.field("trace_id", pa.string(), nullable=True),
                pa.field("privacy_flag", pa.string(), nullable=False),
                pa.field("source_partition", pa.int32(), nullable=False),
                pa.field("source_offset", pa.int64(), nullable=False),
                pa.field("run_id", pa.string(), nullable=False),
            ]
        ),
    ),
    "bronze.quarantine_dlq": (
        Schema(
            NestedField(1, "dlq_id", StringType(), required=True),
            NestedField(2, "rejection_reason", StringType(), required=True),
            NestedField(3, "source_partition", IntegerType(), required=True),
            NestedField(4, "source_offset", LongType(), required=True),
            NestedField(5, "raw_payload", StringType(), required=True),
            NestedField(6, "ingest_ts", LongType(), required=True),
            NestedField(7, "run_id", StringType(), required=True),
        ),
        pa.schema(
            [
                pa.field("dlq_id", pa.string(), nullable=False),
                pa.field("rejection_reason", pa.string(), nullable=False),
                pa.field("source_partition", pa.int32(), nullable=False),
                pa.field("source_offset", pa.int64(), nullable=False),
                pa.field("raw_payload", pa.string(), nullable=False),
                pa.field("ingest_ts", pa.int64(), nullable=False),
                pa.field("run_id", pa.string(), nullable=False),
            ]
        ),
    ),
    "silver.temporal_edges": (
        Schema(
            NestedField(1, "edge_key", StringType(), required=True),
            NestedField(2, "src_user_id", StringType(), required=True),
            NestedField(3, "dst_user_id", StringType(), required=True),
            NestedField(4, "edge_type", StringType(), required=True),
            NestedField(5, "event_id", StringType(), required=True),
            NestedField(6, "valid_from", LongType(), required=True),
            NestedField(7, "valid_to", LongType(), required=False),
            NestedField(8, "is_active", BooleanType(), required=True),
            NestedField(9, "is_deleted", BooleanType(), required=True),
            NestedField(10, "is_blocked", BooleanType(), required=True),
            NestedField(11, "snapshot_version", IntegerType(), required=True),
        ),
        pa.schema(
            [
                pa.field("edge_key", pa.string(), nullable=False),
                pa.field("src_user_id", pa.string(), nullable=False),
                pa.field("dst_user_id", pa.string(), nullable=False),
                pa.field("edge_type", pa.string(), nullable=False),
                pa.field("event_id", pa.string(), nullable=False),
                pa.field("valid_from", pa.int64(), nullable=False),
                pa.field("valid_to", pa.int64(), nullable=True),
                pa.field("is_active", pa.bool_(), nullable=False),
                pa.field("is_deleted", pa.bool_(), nullable=False),
                pa.field("is_blocked", pa.bool_(), nullable=False),
                pa.field("snapshot_version", pa.int32(), nullable=False),
            ]
        ),
    ),
    "silver.user_blocks_and_deletions": (
        Schema(
            NestedField(1, "user_id", StringType(), required=True),
            NestedField(2, "target_user_id", StringType(), required=False),
            NestedField(3, "action_type", StringType(), required=True),
            NestedField(4, "effective_ts", LongType(), required=True),
            NestedField(5, "event_id", StringType(), required=True),
            NestedField(6, "snapshot_version", IntegerType(), required=True),
        ),
        pa.schema(
            [
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("target_user_id", pa.string(), nullable=True),
                pa.field("action_type", pa.string(), nullable=False),
                pa.field("effective_ts", pa.int64(), nullable=False),
                pa.field("event_id", pa.string(), nullable=False),
                pa.field("snapshot_version", pa.int32(), nullable=False),
            ]
        ),
    ),
    "silver.graph_snapshots": (
        Schema(
            NestedField(1, "snapshot_version", IntegerType(), required=True),
            NestedField(2, "iceberg_snapshot_id", LongType(), required=True),
            NestedField(3, "as_of_ts", LongType(), required=True),
            NestedField(4, "active_nodes", IntegerType(), required=True),
            NestedField(5, "active_edges", IntegerType(), required=True),
            NestedField(6, "erased_users", IntegerType(), required=True),
            NestedField(7, "run_id", StringType(), required=True),
            NestedField(8, "materialized_at", StringType(), required=True),
        ),
        pa.schema(
            [
                pa.field("snapshot_version", pa.int32(), nullable=False),
                pa.field("iceberg_snapshot_id", pa.int64(), nullable=False),
                pa.field("as_of_ts", pa.int64(), nullable=False),
                pa.field("active_nodes", pa.int32(), nullable=False),
                pa.field("active_edges", pa.int32(), nullable=False),
                pa.field("erased_users", pa.int32(), nullable=False),
                pa.field("run_id", pa.string(), nullable=False),
                pa.field("materialized_at", pa.string(), nullable=False),
            ]
        ),
    ),
    "gold.adjacency_partitioned": (
        Schema(
            NestedField(1, "partition_id", IntegerType(), required=True),
            NestedField(2, "salt_bucket", IntegerType(), required=True),
            NestedField(3, "is_supernode_edge", BooleanType(), required=True),
            NestedField(4, "src_user_id", StringType(), required=True),
            NestedField(5, "dst_user_id", StringType(), required=True),
            NestedField(6, "edge_weight", DoubleType(), required=True),
            NestedField(7, "partition_strategy", StringType(), required=True),
            NestedField(8, "snapshot_version", IntegerType(), required=True),
        ),
        pa.schema(
            [
                pa.field("partition_id", pa.int32(), nullable=False),
                pa.field("salt_bucket", pa.int32(), nullable=False),
                pa.field("is_supernode_edge", pa.bool_(), nullable=False),
                pa.field("src_user_id", pa.string(), nullable=False),
                pa.field("dst_user_id", pa.string(), nullable=False),
                pa.field("edge_weight", pa.float64(), nullable=False),
                pa.field("partition_strategy", pa.string(), nullable=False),
                pa.field("snapshot_version", pa.int32(), nullable=False),
            ]
        ),
    ),
    "gold.node_graph_features": (
        Schema(
            NestedField(1, "user_id", StringType(), required=True),
            NestedField(2, "in_degree", IntegerType(), required=True),
            NestedField(3, "out_degree", IntegerType(), required=True),
            NestedField(4, "reciprocal_degree", IntegerType(), required=True),
            NestedField(5, "reciprocal_ratio", DoubleType(), required=True),
            NestedField(6, "triangles", IntegerType(), required=True),
            NestedField(7, "local_clustering", DoubleType(), required=True),
            NestedField(8, "two_hop_exact", IntegerType(), required=True),
            NestedField(9, "two_hop_hll", IntegerType(), required=True),
            NestedField(10, "pagerank_score", DoubleType(), required=True),
            NestedField(11, "engagement_burst_cms", IntegerType(), required=True),
            NestedField(12, "report_count", IntegerType(), required=True),
            NestedField(13, "block_received_count", IntegerType(), required=True),
            NestedField(14, "ring_jaccard_max", DoubleType(), required=True),
            NestedField(15, "is_supernode", BooleanType(), required=True),
            NestedField(16, "updated_at", StringType(), required=True),
            NestedField(17, "snapshot_version", IntegerType(), required=True),
        ),
        pa.schema(
            [
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("in_degree", pa.int32(), nullable=False),
                pa.field("out_degree", pa.int32(), nullable=False),
                pa.field("reciprocal_degree", pa.int32(), nullable=False),
                pa.field("reciprocal_ratio", pa.float64(), nullable=False),
                pa.field("triangles", pa.int32(), nullable=False),
                pa.field("local_clustering", pa.float64(), nullable=False),
                pa.field("two_hop_exact", pa.int32(), nullable=False),
                pa.field("two_hop_hll", pa.int32(), nullable=False),
                pa.field("pagerank_score", pa.float64(), nullable=False),
                pa.field("engagement_burst_cms", pa.int32(), nullable=False),
                pa.field("report_count", pa.int32(), nullable=False),
                pa.field("block_received_count", pa.int32(), nullable=False),
                pa.field("ring_jaccard_max", pa.float64(), nullable=False),
                pa.field("is_supernode", pa.bool_(), nullable=False),
                pa.field("updated_at", pa.string(), nullable=False),
                pa.field("snapshot_version", pa.int32(), nullable=False),
            ]
        ),
    ),
    "gold.communities_and_components": (
        Schema(
            NestedField(1, "user_id", StringType(), required=True),
            NestedField(2, "component_id", IntegerType(), required=True),
            NestedField(3, "component_size", IntegerType(), required=True),
            NestedField(4, "community_id", IntegerType(), required=True),
            NestedField(5, "community_size", IntegerType(), required=True),
            NestedField(6, "snapshot_version", IntegerType(), required=True),
        ),
        pa.schema(
            [
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("component_id", pa.int32(), nullable=False),
                pa.field("component_size", pa.int32(), nullable=False),
                pa.field("community_id", pa.int32(), nullable=False),
                pa.field("community_size", pa.int32(), nullable=False),
                pa.field("snapshot_version", pa.int32(), nullable=False),
            ]
        ),
    ),
    "gold.trust_risk_scores": (
        Schema(
            NestedField(1, "user_id", StringType(), required=True),
            NestedField(2, "risk_score", DoubleType(), required=True),
            NestedField(3, "risk_tier", StringType(), required=True),
            NestedField(4, "top_reasons_json", StringType(), required=True),
            NestedField(5, "feature_attributions_json", StringType(), required=True),
            NestedField(6, "synthetic_label", StringType(), required=True),
            NestedField(7, "evaluated_at", StringType(), required=True),
            NestedField(8, "snapshot_version", IntegerType(), required=True),
        ),
        pa.schema(
            [
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("risk_score", pa.float64(), nullable=False),
                pa.field("risk_tier", pa.string(), nullable=False),
                pa.field("top_reasons_json", pa.string(), nullable=False),
                pa.field("feature_attributions_json", pa.string(), nullable=False),
                pa.field("synthetic_label", pa.string(), nullable=False),
                pa.field("evaluated_at", pa.string(), nullable=False),
                pa.field("snapshot_version", pa.int32(), nullable=False),
            ]
        ),
    ),
    "quality.dq_results": (
        Schema(
            NestedField(1, "check_id", StringType(), required=True),
            NestedField(2, "check_name", StringType(), required=True),
            NestedField(3, "table_name", StringType(), required=True),
            NestedField(4, "status", StringType(), required=True),
            NestedField(5, "observed_value", StringType(), required=True),
            NestedField(6, "threshold", StringType(), required=True),
            NestedField(7, "details", StringType(), required=True),
            NestedField(8, "run_id", StringType(), required=True),
            NestedField(9, "checked_at", StringType(), required=True),
        ),
        pa.schema(
            [
                pa.field("check_id", pa.string(), nullable=False),
                pa.field("check_name", pa.string(), nullable=False),
                pa.field("table_name", pa.string(), nullable=False),
                pa.field("status", pa.string(), nullable=False),
                pa.field("observed_value", pa.string(), nullable=False),
                pa.field("threshold", pa.string(), nullable=False),
                pa.field("details", pa.string(), nullable=False),
                pa.field("run_id", pa.string(), nullable=False),
                pa.field("checked_at", pa.string(), nullable=False),
            ]
        ),
    ),
}


class IcebergLakehouse:
    """Manages Apache Iceberg V2 namespaces, tables, snapshots, time travel, and compaction."""

    def __init__(self, catalog_db: Path, warehouse_dir: Path) -> None:
        self.catalog_db = catalog_db.resolve()
        self.warehouse_dir = warehouse_dir.resolve()
        self.catalog_db.parent.mkdir(parents=True, exist_ok=True)
        self.warehouse_dir.mkdir(parents=True, exist_ok=True)
        self.catalog: Catalog = load_catalog(
            "sgti_local_iceberg",
            type="sql",
            uri=f"sqlite:///{self.catalog_db}",
            warehouse=f"file://{self.warehouse_dir}",
        )
        self._init_namespaces_and_tables()

    def _init_namespaces_and_tables(self) -> None:
        for ns in ("bronze", "silver", "gold", "quality"):
            try:
                self.catalog.create_namespace(ns)
            except NamespaceAlreadyExistsError:
                pass
        for full_name, (ice_schema, _) in TABLE_DEFINITIONS.items():
            try:
                self.catalog.load_table(full_name)
            except NoSuchTableError:
                self.catalog.create_table(
                    identifier=full_name,
                    schema=ice_schema,
                    properties={"format-version": "2", "write.parquet.compression-codec": "zstd"},
                )

    def _to_arrow(self, full_name: str, rows: list[dict[str, Any]]) -> pa.Table:
        _, arrow_schema = TABLE_DEFINITIONS[full_name]
        if not rows:
            arrays = [pa.array([], type=field.type) for field in arrow_schema]
            return pa.Table.from_arrays(arrays, schema=arrow_schema)
        columns: dict[str, list[Any]] = {field.name: [] for field in arrow_schema}
        for row in rows:
            for field in arrow_schema:
                columns[field.name].append(row.get(field.name))
        arrays = [pa.array(columns[field.name], type=field.type) for field in arrow_schema]
        return pa.Table.from_arrays(arrays, schema=arrow_schema)

    def load_table(self, full_name: str) -> Table:
        return self.catalog.load_table(full_name)

    def overwrite_table(self, full_name: str, rows: list[dict[str, Any]]) -> int:
        """Atomically overwrite an Iceberg V2 table and return the new snapshot ID."""
        tbl = self.load_table(full_name)
        tbl.refresh()
        arrow_tbl = self._to_arrow(full_name, rows)
        if tbl.current_snapshot() is None:
            if arrow_tbl.num_rows > 0:
                tbl.append(arrow_tbl)
        else:
            tbl.overwrite(arrow_tbl)
        tbl.refresh()
        snap = tbl.current_snapshot()
        return int(snap.snapshot_id) if snap is not None else 0

    def append_table(self, full_name: str, rows: list[dict[str, Any]]) -> int:
        """Append rows to an Iceberg V2 table and return the resulting snapshot ID."""
        tbl = self.load_table(full_name)
        if not rows:
            snap = tbl.current_snapshot()
            return int(snap.snapshot_id) if snap is not None else 0
        arrow_tbl = self._to_arrow(full_name, rows)
        tbl.append(arrow_tbl)
        tbl.refresh()
        snap = tbl.current_snapshot()
        return int(snap.snapshot_id) if snap is not None else 0

    def read_arrow(self, full_name: str, snapshot_id: int | None = None) -> pa.Table:
        """Read an Iceberg table (or a historical Iceberg snapshot via time travel) as PyArrow."""
        tbl = self.load_table(full_name)
        tbl.refresh()
        if tbl.current_snapshot() is None:
            return self._to_arrow(full_name, [])
        scan = tbl.scan(snapshot_id=snapshot_id) if snapshot_id is not None else tbl.scan()
        return scan.to_arrow()

    def read_rows(self, full_name: str, snapshot_id: int | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = self.read_arrow(full_name, snapshot_id=snapshot_id).to_pylist()
        return rows

    def list_snapshots(self, full_name: str) -> list[dict[str, Any]]:
        tbl = self.load_table(full_name)
        tbl.refresh()
        out: list[dict[str, Any]] = []
        for snap in tbl.snapshots():
            out.append(
                {
                    "snapshot_id": int(snap.snapshot_id),
                    "timestamp_ms": int(snap.timestamp_ms),
                    "operation": str(snap.summary.operation.value) if snap.summary else "unknown",
                }
            )
        return out

    def count_data_files(self, full_name: str) -> int:
        tbl = self.load_table(full_name)
        tbl.refresh()
        if tbl.current_snapshot() is None:
            return 0
        return sum(1 for _ in tbl.scan().plan_files())

    def compact_table(self, full_name: str) -> dict[str, Any]:
        """Rewrite fragmented small Parquet files in an Iceberg V2 table into a single compacted file."""
        files_before = self.count_data_files(full_name)
        t0 = time.perf_counter()
        arrow_tbl = self.read_arrow(full_name)
        scan_before_ms = (time.perf_counter() - t0) * 1000.0

        tbl = self.load_table(full_name)
        t_comp0 = time.perf_counter()
        tbl.overwrite(arrow_tbl)
        compact_write_ms = (time.perf_counter() - t_comp0) * 1000.0
        tbl.refresh()

        files_after = self.count_data_files(full_name)
        t1 = time.perf_counter()
        _ = self.read_arrow(full_name)
        scan_after_ms = (time.perf_counter() - t1) * 1000.0

        return {
            "table": full_name,
            "rows": int(arrow_tbl.num_rows),
            "files_before": files_before,
            "files_after": files_after,
            "scan_before_ms": round(scan_before_ms, 3),
            "scan_after_ms": round(scan_after_ms, 3),
            "compact_write_ms": round(compact_write_ms, 3),
        }

    def table_inventory(self) -> list[dict[str, Any]]:
        """Summarize row counts, file counts, and current snapshot IDs across all 10 Iceberg tables."""
        inventory: list[dict[str, Any]] = []
        for name in TABLE_DEFINITIONS:
            tbl = self.load_table(name)
            tbl.refresh()
            snap = tbl.current_snapshot()
            arrow_tbl = self.read_arrow(name)
            inventory.append(
                {
                    "table": name,
                    "format_version": 2,
                    "rows": int(arrow_tbl.num_rows),
                    "data_files": self.count_data_files(name),
                    "snapshot_count": len(list(tbl.snapshots())),
                    "current_snapshot_id": int(snap.snapshot_id) if snap else None,
                }
            )
        return inventory

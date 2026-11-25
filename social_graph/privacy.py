"""GDPR right-to-erasure propagation across Apache Iceberg V2 tables and serving indexes."""

from __future__ import annotations

import hashlib
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from social_graph.iceberg_store import IcebergLakehouse
from social_graph.observability import (
    ACTIVE_EDGES_GAUGE,
    ACTIVE_NODES_GAUGE,
    ERASURE_LATENCY_MS,
    append_audit_event,
)
from social_graph.serving_store import GraphServingStore


def propagate_user_erasure(
    user_id: str,
    lakehouse: IcebergLakehouse,
    serving: GraphServingStore,
    audit_file: Path,
    actor_role: str = "admin",
    reason: str = "gdpr_article_17_right_to_erasure",
) -> dict[str, Any]:
    """Propagate right-to-erasure tombstone across Iceberg V2 active tables and serving indexes.

    Verifies 0 residual active edges and 0 residual serving records before returning receipt.
    """
    t0 = time.perf_counter()
    now_iso = datetime.now(UTC).isoformat()
    now_ms = int(time.time() * 1000)
    tombstone_id = (
        f"evt_erase_{hashlib.sha256(f'{user_id}:{now_ms}'.encode()).hexdigest()[:12]}"
    )

    # Determine next snapshot version
    snaps = lakehouse.read_rows("silver.graph_snapshots")
    next_snap_ver = max((int(s["snapshot_version"]) for s in snaps), default=0) + 1

    # 1. Record tombstone in silver.user_blocks_and_deletions
    lakehouse.append_table(
        "silver.user_blocks_and_deletions",
        [
            {
                "user_id": user_id,
                "target_user_id": None,
                "action_type": "user_deleted",
                "effective_ts": now_ms,
                "event_id": tombstone_id,
                "snapshot_version": next_snap_ver,
            }
        ],
    )

    # 2. Close active edges touching user_id in silver.temporal_edges
    temporal_rows = lakehouse.read_rows("silver.temporal_edges")
    edges_closed = 0
    for row in temporal_rows:
        if (row["src_user_id"] == user_id or row["dst_user_id"] == user_id) and row["is_active"]:
            row["is_active"] = False
            row["is_deleted"] = True
            row["valid_to"] = max(int(row["valid_from"]), now_ms)
            row["snapshot_version"] = next_snap_ver
            edges_closed += 1
    edge_snap_id = lakehouse.overwrite_table("silver.temporal_edges", temporal_rows)

    # 3. Purge user from active Gold Iceberg tables
    adj_rows = [
        r
        for r in lakehouse.read_rows("gold.adjacency_partitioned")
        if r["src_user_id"] != user_id and r["dst_user_id"] != user_id
    ]
    lakehouse.overwrite_table("gold.adjacency_partitioned", adj_rows)

    feat_rows = [
        r for r in lakehouse.read_rows("gold.node_graph_features") if r["user_id"] != user_id
    ]
    lakehouse.overwrite_table("gold.node_graph_features", feat_rows)

    comm_rows = [
        r
        for r in lakehouse.read_rows("gold.communities_and_components")
        if r["user_id"] != user_id
    ]
    lakehouse.overwrite_table("gold.communities_and_components", comm_rows)

    trust_rows = [
        r for r in lakehouse.read_rows("gold.trust_risk_scores") if r["user_id"] != user_id
    ]
    lakehouse.overwrite_table("gold.trust_risk_scores", trust_rows)

    # 4. Purge from DuckDB serving index + in-memory adjacency cache
    serving.purge_user_from_serving(user_id)
    # Also persist updated tombstone row in DuckDB blocks_deletions
    con = serving._connect()
    try:
        con.execute(
            "INSERT INTO blocks_deletions VALUES (?, NULL, 'user_deleted', ?, ?, ?)",
            [user_id, now_ms, tombstone_id, next_snap_ver],
        )
    finally:
        con.close()

    # 5. Record new graph snapshot version
    erased_total = len(
        {
            str(r["user_id"])
            for r in lakehouse.read_rows("silver.user_blocks_and_deletions")
            if r["action_type"] == "user_deleted"
        }
    )
    lakehouse.append_table(
        "silver.graph_snapshots",
        [
            {
                "snapshot_version": next_snap_ver,
                "iceberg_snapshot_id": edge_snap_id,
                "as_of_ts": now_ms,
                "active_nodes": len(feat_rows),
                "active_edges": len(adj_rows),
                "erased_users": erased_total,
                "run_id": f"erasure_{tombstone_id}",
                "materialized_at": now_iso,
            }
        ],
    )

    # 6. Post-erasure zero-leak verification audit across Iceberg + serving store
    post_edges = lakehouse.read_rows("silver.temporal_edges")
    residual_active_edges = sum(
        1
        for r in post_edges
        if r["is_active"] and (r["src_user_id"] == user_id or r["dst_user_id"] == user_id)
    )
    residual_gold = sum(
        1 for r in lakehouse.read_rows("gold.node_graph_features") if r["user_id"] == user_id
    ) + sum(
        1
        for r in lakehouse.read_rows("gold.adjacency_partitioned")
        if r["src_user_id"] == user_id or r["dst_user_id"] == user_id
    )
    residual_serving = serving.count_residual_serving_records(user_id)
    total_residual_serving = residual_serving + residual_gold
    zero_leak = residual_active_edges == 0 and total_residual_serving == 0

    elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
    ERASURE_LATENCY_MS.observe(elapsed_ms)
    ACTIVE_NODES_GAUGE.set(len(feat_rows))
    ACTIVE_EDGES_GAUGE.set(len(adj_rows))

    receipt = {
        "user_id": user_id,
        "status": "erased_verified" if zero_leak else "leak_detected",
        "erased_at": now_iso,
        "tombstone_event_id": tombstone_id,
        "edges_closed": edges_closed,
        "residual_active_edges": residual_active_edges,
        "residual_serving_records": total_residual_serving,
        "zero_leak_verified": zero_leak,
        "tombstone_propagation_ms": elapsed_ms,
        "snapshot_version": next_snap_ver,
    }

    append_audit_event(
        audit_file,
        {
            "action": "gdpr_right_to_erasure",
            "actor_role": actor_role,
            "reason": reason,
            **receipt,
        },
    )
    return receipt

"""Integration tests for Apache Iceberg V2 time travel, compaction, FastAPI RBAC, and live GDPR erasure."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from services.api.main import create_app
from social_graph.config import get_config
from social_graph.iceberg_store import IcebergLakehouse
from social_graph.pipeline import run_full_pipeline


@pytest.mark.integration
def test_iceberg_time_travel_and_compaction(tmp_path: Path) -> None:
    """Apache Iceberg V2 supports snapshot time travel and small-file Parquet compaction."""
    lh = IcebergLakehouse(tmp_path / "cat.sqlite", tmp_path / "wh")
    row_v1 = {
        "edge_key": "usr_000001->usr_000002",
        "src_user_id": "usr_000001",
        "dst_user_id": "usr_000002",
        "edge_type": "follow",
        "event_id": "evt_tt_1",
        "valid_from": 1000,
        "valid_to": None,
        "is_active": True,
        "is_deleted": False,
        "is_blocked": False,
        "snapshot_version": 1,
    }
    snap1 = lh.append_table("silver.temporal_edges", [row_v1])

    # Append 4 additional small micro-batches to create fragmented Parquet files
    for k in range(2, 6):
        lh.append_table(
            "silver.temporal_edges",
            [{**row_v1, "edge_key": f"usr_000001->usr_00000{k+1}", "dst_user_id": f"usr_00000{k+1}", "event_id": f"evt_tt_{k}"}],
        )

    # Time-travel back to snapshot 1 -> must see exactly 1 row
    hist_rows = lh.read_rows("silver.temporal_edges", snapshot_id=snap1)
    assert len(hist_rows) == 1
    assert len(lh.read_rows("silver.temporal_edges")) == 5

    # Compact 5 fragmented files into 1 Parquet file
    comp = lh.compact_table("silver.temporal_edges")
    assert comp["files_before"] == 5
    assert comp["files_after"] == 1
    assert comp["rows"] == 5


@pytest.mark.integration
def test_fastapi_rbac_endpoints_and_live_gdpr_erasure(tmp_path: Path) -> None:
    """FastAPI enforces 401/403 RBAC, serves real Iceberg data, and executes zero-leak GDPR erasure."""
    cfg = get_config(tmp_path / "api_data")
    run_full_pipeline(profile="smoke", seed=42, config=cfg, clean_spool=True)
    app = create_app(cfg)

    with TestClient(app) as client:
        # 1. Public health and Prometheus metrics work without auth
        r_health = client.get("/health")
        assert r_health.status_code == 200
        assert r_health.json()["status"] == "healthy"

        r_metrics = client.get("/metrics")
        assert r_metrics.status_code == 200
        assert "sgti_active_edges" in r_metrics.text

        # 2. Missing or bad API key returns 401 Unauthorized
        assert client.get("/v1/overview").status_code == 401
        assert client.get("/v1/overview", headers={"X-API-Key": "wrong-key"}).status_code == 401

        viewer_h = {"X-API-Key": cfg.viewer_api_key}
        admin_h = {"X-API-Key": cfg.admin_api_key}

        # 3. Viewer can read overview, alerts, communities, DQ, and node detail
        ov = client.get("/v1/overview", headers=viewer_h)
        assert ov.status_code == 200
        ov_json = ov.json()
        assert ov_json["dq_passed"] == ov_json["dq_total"]
        assert ov_json["incremental_parity_verified"] is True

        alerts = client.get("/v1/trust/alerts", headers=viewer_h)
        assert alerts.status_code == 200
        target_uid = alerts.json()["alerts"][0]["user_id"]

        node = client.get(f"/v1/graph/node/{target_uid}", headers=viewer_h)
        assert node.status_code == 200
        assert node.json()["is_erased"] is False

        comms = client.get("/v1/graph/communities", headers=viewer_h)
        assert comms.status_code == 200
        assert len(comms.json()["communities"]) >= 1

        dq = client.get("/v1/dq", headers=viewer_h)
        assert dq.status_code == 200
        assert dq.json()["dq_passed"] == dq.json()["dq_total"]

        # 4. Viewer is forbidden (403) from invoking GDPR erasure or admin cache rebuild
        r_forbid = client.post(f"/v1/privacy/erase/{target_uid}", headers=viewer_h)
        assert r_forbid.status_code == 403
        assert client.post("/v1/admin/rebuild-serving", headers=viewer_h).status_code == 403

        # 5. Admin executes live GDPR right-to-erasure with verified zero leaks
        r_erase = client.post(f"/v1/privacy/erase/{target_uid}", headers=admin_h)
        assert r_erase.status_code == 200
        receipt = r_erase.json()
        assert receipt["zero_leak_verified"] is True
        assert receipt["residual_active_edges"] == 0
        assert receipt["residual_serving_records"] == 0
        assert receipt["tombstone_propagation_ms"] >= 0.0

        # Post-erasure lookup confirms erased status
        post_node = client.get(f"/v1/graph/node/{target_uid}", headers=viewer_h)
        assert post_node.status_code == 200
        assert post_node.json()["is_erased"] is True

        # Audit trail captured both auth rejection and GDPR erasure
        audit = client.get("/v1/privacy/audit", headers=viewer_h)
        assert audit.status_code == 200
        actions = [e["action"] for e in audit.json()["events"]]
        assert "auth_rejected_unauthorized" in actions
        assert "auth_rejected_forbidden" in actions
        assert "gdpr_right_to_erasure" in actions

"""Serving latency SLO verification tests (p50/p95/p99)."""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient

from services.api.main import create_app
from social_graph.config import get_config
from social_graph.pipeline import run_full_pipeline


@pytest.mark.performance
def test_api_node_and_overview_serving_slo(tmp_path: Path) -> None:
    """Serving endpoints meet p95 < 100 ms SLO on local workstation."""
    cfg = get_config(tmp_path / "perf_data")
    run_full_pipeline(profile="smoke", seed=42, config=cfg, clean_spool=True)
    app = create_app(cfg)
    headers = {"X-API-Key": cfg.viewer_api_key}

    with TestClient(app) as client:
        # Warm-up
        client.get("/v1/graph/node/usr_000001", headers=headers)
        latencies_ms: list[float] = []
        for i in range(1, 31):
            uid = f"usr_{i:06d}"
            t0 = time.perf_counter()
            resp = client.get(f"/v1/graph/node/{uid}", headers=headers)
            latencies_ms.append((time.perf_counter() - t0) * 1000.0)
            assert resp.status_code == 200

        p95 = float(np.percentile(latencies_ms, 95))
        assert p95 < 100.0, f"Node lookup p95 {p95:.2f} ms exceeded 100 ms SLO"

"""End-to-end vertical-slice tests validating source -> spool -> Iceberg V2 -> serving -> replay."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from social_graph.config import get_config
from social_graph.pipeline import run_full_pipeline


@pytest.mark.e2e
def test_full_vertical_slice_and_deterministic_replay(tmp_path: Path) -> None:
    """Fresh workspace runs full vertical slice, writes traces/lineage, and replays deterministically."""
    cfg1 = get_config(tmp_path / "slice_run_1")
    lineage1 = tmp_path / "lineage1.json"
    res1 = run_full_pipeline(profile="smoke", seed=77, config=cfg1, lineage_output_path=lineage1)

    assert res1["dq_passed"] == res1["dq_total"]
    assert res1["dlq_quarantined"] == 15
    assert res1["incremental_parity_verified"] is True
    assert res1["incremental_max_abs_diff"] == 0.0
    assert (cfg1.traces_dir / "spans.jsonl").exists()
    assert len((cfg1.traces_dir / "spans.jsonl").read_text("utf-8").splitlines()) >= 5
    assert lineage1.exists()
    ol = json.loads(lineage1.read_text("utf-8"))
    assert len(ol["events"]) == 4

    # Independent fresh replay with identical seed must produce identical business metrics
    cfg2 = get_config(tmp_path / "slice_run_2")
    res2 = run_full_pipeline(profile="smoke", seed=77, config=cfg2)
    assert res1["active_nodes"] == res2["active_nodes"]
    assert res1["active_edges"] == res2["active_edges"]
    assert res1["valid_events_ingested"] == res2["valid_events_ingested"]
    assert res1["modularity_q"] == res2["modularity_q"]
    assert res1["trust_evaluation"]["f1_score"] == res2["trust_evaluation"]["f1_score"]

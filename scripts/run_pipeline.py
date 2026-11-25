#!/usr/bin/env python3
"""CLI entrypoint to run the end-to-end Social Graph & Trust Intelligence pipeline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from social_graph.config import REPO_ROOT, get_config
from social_graph.pipeline import run_full_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Social Graph & Trust Intelligence Iceberg V2 pipeline."
    )
    parser.add_argument(
        "--profile",
        choices=["smoke", "local", "stress"],
        default="local",
        help="Workload profile scale (default: local).",
    )
    parser.add_argument("--seed", type=int, default=42, help="Deterministic RNG seed.")
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Optional override for SGTI_DATA_DIR.",
    )
    parser.add_argument(
        "--lineage-out",
        type=str,
        default=str(REPO_ROOT / "results" / "lineage" / "openlineage_events.json"),
        help="Output path for OpenLineage-compatible JSON manifest.",
    )
    args = parser.parse_args()

    cfg = get_config(args.data_dir)
    summary = run_full_pipeline(
        profile=args.profile,
        seed=args.seed,
        config=cfg,
        clean_spool=True,
        lineage_output_path=Path(args.lineage_out),
    )

    compact_out = {
        k: v
        for k, v in summary.items()
        if k not in {"erased_seed_users", "iceberg_tables"}
    }
    print(json.dumps(compact_out, indent=2))


if __name__ == "__main__":
    main()

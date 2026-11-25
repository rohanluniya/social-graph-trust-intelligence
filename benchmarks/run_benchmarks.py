#!/usr/bin/env python3
"""Reproducible benchmark suite for the Social Graph & Trust Intelligence Platform.

Measures:
1. Scale profiles: smoke, local, stress (ingestion throughput, Iceberg V2 write/read, graph compute)
2. Graph partitioning: Naive source/node-hash vs Skew-Aware Vertex-Cut (skew ratio, CV, 2-hop p95 ms)
3. Incremental delta updates vs Full CSR recomputation (exact parity + speedup)
4. Approximate sketches: Exact vs HyperLogLog 2-hop memory & relative error + Count-Min Sketch accuracy
5. Apache Iceberg V2 file compaction before vs after (small-file fragments -> compacted Parquet)

Writes raw JSON + CSV artifacts to results/benchmarks/ and renders a 4-panel chart to
docs/assets/benchmark_social_graph_trust.png.
"""

from __future__ import annotations

import csv
import json
import platform
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import matplotlib
import psutil

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from social_graph.config import REPO_ROOT, get_config
from social_graph.iceberg_store import IcebergLakehouse
from social_graph.pipeline import run_full_pipeline


def _hardware_metadata() -> dict[str, Any]:
    return {
        "os": f"{platform.system()} {platform.release()}",
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpu_cores_logical": psutil.cpu_count(logical=True),
        "ram_gib": round(psutil.virtual_memory().total / (1024**3), 1),
    }


def benchmark_iceberg_compaction(tmp_dir: Path, sample_edges: list[dict[str, Any]]) -> dict[str, Any]:
    """Create 12 small Parquet micro-batches in Iceberg V2, then compact and measure scan improvement."""
    lh = IcebergLakehouse(tmp_dir / "comp_cat.sqlite", tmp_dir / "comp_wh")
    chunk_size = max(1, len(sample_edges) // 12)
    for i in range(12):
        chunk = sample_edges[i * chunk_size : (i + 1) * chunk_size]
        if chunk:
            lh.append_table("silver.temporal_edges", chunk)
    return lh.compact_table("silver.temporal_edges")


def render_benchmark_chart(
    profile_runs: list[dict[str, Any]],
    compaction: dict[str, Any],
    out_png: Path,
) -> None:
    """Render a crisp 4-panel dark-themed benchmark chart."""
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update(
        {
            "figure.facecolor": "#0b1120",
            "axes.facecolor": "#0f172a",
            "axes.edgecolor": "#334155",
            "axes.labelcolor": "#e2e8f0",
            "text.color": "#e2e8f0",
            "xtick.color": "#cbd5e1",
            "ytick.color": "#cbd5e1",
            "grid.color": "#1e293b",
            "font.size": 10,
        }
    )

    fig, axes = plt.subplots(2, 2, figsize=(13.5, 8.6), dpi=160)
    fig.suptitle(
        "Social Graph & Trust Intelligence — Measured Benchmarks (Apple M4 Pro, Local Native Runtime)",
        fontsize=13.5,
        fontweight="bold",
        color="#f8fafc",
        y=0.98,
    )

    names = [r["profile"].upper() for r in profile_runs]
    x = np.arange(len(names))
    w = 0.34

    # Panel 1: Partition Skew Ratio (Naive Hash vs Skew-Aware Vertex-Cut)
    ax1 = axes[0, 0]
    naive_skews = [r["partitioning"]["naive_skew_ratio"] for r in profile_runs]
    vc_skews = [r["partitioning"]["vertex_cut_skew_ratio"] for r in profile_runs]
    b1 = ax1.bar(x - w / 2, naive_skews, w, label="Naive Source/Node Hash", color="#f97316")
    b2 = ax1.bar(x + w / 2, vc_skews, w, label="Skew-Aware Vertex-Cut", color="#38bdf8")
    ax1.axhline(1.0, color="#34d399", linestyle="--", linewidth=1, label="Ideal Balance (1.0x)")
    ax1.set_title("1. Supernode Partition Skew Ratio (Lower is Better)", fontweight="bold")
    ax1.set_ylabel("Max / Mean Partition Load (x)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(names)
    ax1.set_ylim(0, max(naive_skews) * 1.28)
    ax1.grid(axis="y", alpha=0.5)
    ax1.legend(frameon=False, fontsize=8.5, loc="upper right")
    for bars in (b1, b2):
        for bar in bars:
            val = bar.get_height()
            ax1.text(
                bar.get_x() + bar.get_width() / 2,
                val + 0.03,
                f"{val:.2f}x",
                ha="center",
                va="bottom",
                fontsize=8.5,
            )

    # Panel 2: Full CSR Recomputation vs Incremental Delta Update Latency
    ax2 = axes[0, 1]
    full_ms = [r["full_graph_compute_ms"] for r in profile_runs]
    inc_pr_ms = [r["incremental_with_pr_ms"] for r in profile_runs]
    inc_st_ms = [r["incremental_struct_ms"] for r in profile_runs]
    w3 = 0.25
    b3 = ax2.bar(x - w3, full_ms, w3, label="Full CSR Recompute", color="#f43f5e")
    b4 = ax2.bar(x, inc_pr_ms, w3, label="Incremental + Warm PageRank", color="#818cf8")
    b5 = ax2.bar(x + w3, inc_st_ms, w3, label="Incremental Structural Delta", color="#34d399")
    ax2.set_title("2. Full vs. Incremental Graph Feature Latency (Exact Parity = 0.0)", fontweight="bold")
    ax2.set_ylabel("Latency (ms)")
    ax2.set_xticks(x)
    ax2.set_xticklabels(names)
    ax2.set_ylim(0, max(full_ms) * 1.28)
    ax2.grid(axis="y", alpha=0.5)
    ax2.legend(frameon=False, fontsize=8.5, loc="upper left")
    for bars in (b3, b4, b5):
        for bar in bars:
            val = bar.get_height()
            ax2.text(
                bar.get_x() + bar.get_width() / 2,
                val + max(full_ms) * 0.015,
                f"{val:.1f}",
                ha="center",
                va="bottom",
                fontsize=8,
            )

    # Panel 3: Exact vs HyperLogLog 2-Hop Neighborhood Memory Footprint (MB, log scale)
    ax3 = axes[1, 0]
    exact_mb = [r["sketches"]["exact_memory_bytes"] / (1024**2) for r in profile_runs]
    hll_mb = [r["sketches"]["hll_memory_bytes"] / (1024**2) for r in profile_runs]
    ax3.bar(x - w / 2, exact_mb, w, label="Exact 2-Hop Neighbor Sets", color="#fbbf24")
    ax3.bar(x + w / 2, hll_mb, w, label="HyperLogLog (p=10, 1024 regs)", color="#38bdf8")
    ax3.set_yscale("log")
    ax3.set_title("3. Exact vs. HLL 2-Hop Memory Footprint (Log Scale MB)", fontweight="bold")
    ax3.set_ylabel("Memory Footprint (MB, log)")
    ax3.set_xticks(x)
    ax3.set_xticklabels(names)
    ax3.set_ylim(0.1, max(exact_mb) * 4.5)
    ax3.grid(axis="y", alpha=0.5)
    ax3.legend(frameon=False, fontsize=8.5, loc="upper left")
    for i, r in enumerate(profile_runs):
        ratio = r["sketches"]["memory_reduction_ratio"]
        err = r["sketches"]["hll_mean_rel_error_pct"]
        ax3.text(
            x[i],
            max(exact_mb[i], hll_mb[i]) * 1.45,
            f"{ratio:.0f}x smaller\n({err:.1f}% err)",
            ha="center",
            va="bottom",
            fontsize=8.2,
            color="#a7f3d0",
        )

    # Panel 4: Celebrity 2-Hop p95 Latency + Iceberg V2 Compaction Before/After
    ax4 = axes[1, 1]
    labels_p4 = [
        "Local 2-Hop\nNaive p95 (ms)",
        "Local 2-Hop\nVertex-Cut (ms)",
        "Iceberg Scan\n12 Files (ms)",
        "Iceberg Scan\n1 File (ms)",
    ]
    local_run = next(r for r in profile_runs if r["profile"] == "local")
    vals_p4 = [
        local_run["partitioning"]["naive_two_hop_p95_ms"],
        local_run["partitioning"]["vertex_cut_two_hop_p95_ms"],
        compaction["scan_before_ms"],
        compaction["scan_after_ms"],
    ]
    colors_p4 = ["#f97316", "#38bdf8", "#f43f5e", "#34d399"]
    xp4 = np.arange(len(labels_p4))
    b8 = ax4.bar(xp4, vals_p4, width=0.52, color=colors_p4)
    ax4.set_title("4. 2-Hop Supernode Latency & Iceberg V2 Compaction", fontweight="bold")
    ax4.set_ylabel("Measured Latency (ms)")
    ax4.set_xticks(xp4)
    ax4.set_xticklabels(labels_p4, fontsize=8.5)
    ax4.set_ylim(0, max(vals_p4) * 1.32)
    ax4.grid(axis="y", alpha=0.5)
    for bar in b8:
        val = bar.get_height()
        ax4.text(
            bar.get_x() + bar.get_width() / 2,
            val + max(vals_p4) * 0.025,
            f"{val:.2f} ms",
            ha="center",
            va="bottom",
            fontsize=8.5,
        )

    plt.tight_layout(rect=(0, 0.01, 1, 0.94))
    fig.savefig(out_png, dpi=160)
    plt.close(fig)


def main() -> None:
    out_dir = REPO_ROOT / "results" / "benchmarks"
    out_dir.mkdir(parents=True, exist_ok=True)
    chart_png = REPO_ROOT / "docs" / "assets" / "benchmark_social_graph_trust.png"

    profile_runs: list[dict[str, Any]] = []
    compaction_result: dict[str, Any] = {}

    with tempfile.TemporaryDirectory(prefix="sgti_bench_") as tmp_root:
        tmp_path = Path(tmp_root)
        for prof in ("smoke", "local", "stress"):
            cfg = get_config(tmp_path / f"run_{prof}")
            t0 = time.perf_counter()
            summary = run_full_pipeline(profile=prof, seed=42, config=cfg, clean_spool=True)
            wall_s = time.perf_counter() - t0
            summary["events_per_sec"] = round(
                summary["total_events_generated"] / max(0.001, wall_s), 1
            )
            profile_runs.append(summary)

            if prof == "local":
                lh = IcebergLakehouse(cfg.catalog_db, cfg.warehouse_dir)
                sample_edges = lh.read_rows("silver.temporal_edges")
                compaction_result = benchmark_iceberg_compaction(
                    tmp_path / "compaction", sample_edges
                )

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "hardware": _hardware_metadata(),
        "profiles": profile_runs,
        "iceberg_compaction": compaction_result,
    }
    json_path = out_dir / "benchmark_results.json"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    csv_path = out_dir / "benchmark_results.csv"
    fieldnames = [
        "profile",
        "seed",
        "total_events_generated",
        "active_nodes",
        "active_edges",
        "total_pipeline_ms",
        "events_per_sec",
        "full_graph_compute_ms",
        "incremental_struct_ms",
        "incremental_with_pr_ms",
        "incremental_speedup_x",
        "incremental_max_abs_diff",
        "naive_skew_ratio",
        "vertex_cut_skew_ratio",
        "skew_reduction_pct",
        "naive_two_hop_p95_ms",
        "vertex_cut_two_hop_p95_ms",
        "hll_mean_rel_error_pct",
        "hll_memory_reduction_ratio",
        "trust_precision",
        "trust_recall",
        "trust_f1",
        "celebrity_fp",
        "modularity_q",
        "dq_passed",
        "dq_total",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for r in profile_runs:
            writer.writerow(
                {
                    "profile": r["profile"],
                    "seed": r["seed"],
                    "total_events_generated": r["total_events_generated"],
                    "active_nodes": r["active_nodes"],
                    "active_edges": r["active_edges"],
                    "total_pipeline_ms": r["total_pipeline_ms"],
                    "events_per_sec": r["events_per_sec"],
                    "full_graph_compute_ms": r["full_graph_compute_ms"],
                    "incremental_struct_ms": r["incremental_struct_ms"],
                    "incremental_with_pr_ms": r["incremental_with_pr_ms"],
                    "incremental_speedup_x": r["incremental_speedup_x"],
                    "incremental_max_abs_diff": r["incremental_max_abs_diff"],
                    "naive_skew_ratio": r["partitioning"]["naive_skew_ratio"],
                    "vertex_cut_skew_ratio": r["partitioning"]["vertex_cut_skew_ratio"],
                    "skew_reduction_pct": r["partitioning"]["skew_reduction_pct"],
                    "naive_two_hop_p95_ms": r["partitioning"]["naive_two_hop_p95_ms"],
                    "vertex_cut_two_hop_p95_ms": r["partitioning"]["vertex_cut_two_hop_p95_ms"],
                    "hll_mean_rel_error_pct": r["sketches"]["hll_mean_rel_error_pct"],
                    "hll_memory_reduction_ratio": r["sketches"]["memory_reduction_ratio"],
                    "trust_precision": r["trust_evaluation"]["precision"],
                    "trust_recall": r["trust_evaluation"]["recall"],
                    "trust_f1": r["trust_evaluation"]["f1_score"],
                    "celebrity_fp": r["trust_evaluation"]["celebrity_false_positives"],
                    "modularity_q": r["modularity_q"],
                    "dq_passed": r["dq_passed"],
                    "dq_total": r["dq_total"],
                }
            )

    render_benchmark_chart(profile_runs, compaction_result, chart_png)
    print(f"Benchmarks complete:\n  JSON: {json_path}\n  CSV:  {csv_path}\n  Chart: {chart_png}")
    print(json.dumps({"compaction": compaction_result}, indent=2))


if __name__ == "__main__":
    main()

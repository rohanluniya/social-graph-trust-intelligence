"""Explainable trust & abuse risk scoring with synthetic ground-truth evaluation and FP analysis."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from social_graph.graph_engine import FullGraphMetrics
from social_graph.sketches import CountMinSketch


@dataclass
class TrustScoringResult:
    trust_rows: list[dict[str, Any]]
    cms_burst_counts: dict[str, int]
    cms_summary: dict[str, Any]
    report_counts: dict[str, int]
    block_received_counts: dict[str, int]
    ring_jaccard_max: dict[str, float]
    evaluation: dict[str, Any]


def compute_explainable_trust_scores(
    events: list[dict[str, Any]],
    graph: FullGraphMetrics,
    user_labels: dict[str, str],
    supernodes: set[str],
    cms_depth: int = 5,
    cms_width: int = 2048,
    risk_threshold: float = 45.0,
    snapshot_version: int = 1,
    evaluated_at: str = "",
) -> TrustScoringResult:
    """Compute explainable trust/abuse risk scores backed by graph & temporal signals.

    All evaluation metrics (Precision, Recall, F1, FPR, FP analysis) are measured strictly
    against the deterministic generator's synthetic benchmark ground-truth labels
    (`bot_swarm`, `coordinated_ring` vs `organic`, `celebrity`) — never real-world moderation claims.
    """
    active_set = set(graph.user_ids)

    # 1. Count-Min Sketch engagement burst velocity + exact verification counts
    cms = CountMinSketch(depth=cms_depth, width=cms_width)
    exact_eng_counts: dict[str, int] = defaultdict(int)
    user_posts: dict[str, set[str]] = defaultdict(set)
    post_users: dict[str, set[str]] = defaultdict(set)
    user_eng_timestamps: dict[str, list[int]] = defaultdict(list)

    report_reporters: dict[str, set[str]] = defaultdict(set)
    block_blockers: dict[str, set[str]] = defaultdict(set)

    for ev in events:
        etype = ev["event_type"]
        actor = str(ev["actor_id"])
        target = ev.get("target_id")
        post_id = ev.get("post_id")
        ets = int(ev["event_ts"])

        if etype in {"reaction", "comment", "share"} and actor in active_set:
            cms.increment(actor, 1)
            exact_eng_counts[actor] += 1
            user_eng_timestamps[actor].append(ets)
            if isinstance(post_id, str):
                user_posts[actor].add(post_id)
                post_users[post_id].add(actor)

        elif etype == "report" and isinstance(target, str) and target in active_set:
            report_reporters[target].add(actor)

        elif etype == "block" and isinstance(target, str) and target in active_set:
            block_blockers[target].add(actor)

    # Verify CMS no-underestimation guarantee across all active users
    cms_counts: dict[str, int] = {}
    overcounts: list[int] = []
    zero_underestimation = True
    for uid in graph.user_ids:
        est = cms.query(uid)
        exact = exact_eng_counts.get(uid, 0)
        cms_counts[uid] = est
        if est < exact:
            zero_underestimation = False
        overcounts.append(est - exact)

    cms_summary = {
        "cms_depth": cms_depth,
        "cms_width": cms_width,
        "cms_zero_underestimation_verified": zero_underestimation,
        "cms_mean_overcount": round(sum(overcounts) / max(1, len(overcounts)), 4),
    }

    # 2. Coordinated Engagement Ring Jaccard Overlap (bipartite user-post inverted index)
    ring_jaccard_max: dict[str, float] = {u: 0.0 for u in graph.user_ids}
    ring_peer_count: dict[str, int] = {u: 0 for u in graph.user_ids}
    for uid in graph.user_ids:
        posts_u = user_posts.get(uid)
        if not posts_u or len(posts_u) < 4:
            continue
        candidate_overlap: dict[str, int] = defaultdict(int)
        for pid in posts_u:
            for other in post_users[pid]:
                if other != uid:
                    candidate_overlap[other] += 1

        max_j = 0.0
        tight_peers = 0
        len_u = len(posts_u)
        for other, inter in candidate_overlap.items():
            if inter < 4:
                continue
            union_sz = len_u + len(user_posts[other]) - inter
            j = inter / union_sz if union_sz > 0 else 0.0
            if j > max_j:
                max_j = j
            if j >= 0.50:
                tight_peers += 1
        ring_jaccard_max[uid] = round(max_j, 4)
        ring_peer_count[uid] = tight_peers

    # 3. Compute temporal engagement burstiness (max events within a 60-second sliding window)
    burst_60s: dict[str, int] = {}
    for uid in graph.user_ids:
        ts_list = sorted(user_eng_timestamps.get(uid, []))
        if len(ts_list) < 2:
            burst_60s[uid] = len(ts_list)
            continue
        best_win = 1
        left = 0
        for right in range(len(ts_list)):
            while ts_list[right] - ts_list[left] > 60_000:
                left += 1
            win_sz = right - left + 1
            if win_sz > best_win:
                best_win = win_sz
        burst_60s[uid] = best_win

    report_counts = {u: len(report_reporters.get(u, set())) for u in graph.user_ids}
    block_received_counts = {u: len(block_blockers.get(u, set())) for u in graph.user_ids}

    # 4. Transparent weighted additive scoring with explicit reason codes
    # Weights sum to 100.0 for intuitive 0..100 risk_score scale
    weights = {
        "outbound_burst_velocity": 22.0,
        "low_reciprocity_fanout": 24.0,
        "coordinated_ring_jaccard": 26.0,
        "report_velocity_signal": 16.0,
        "block_ratio_signal": 12.0,
    }

    trust_rows: list[dict[str, Any]] = []
    tp = fp = tn = fn = 0
    celeb_fp = 0
    celeb_total = 0
    fp_cases: list[dict[str, Any]] = []

    for uid in graph.user_ids:
        in_d = graph.in_degree[uid]
        out_d = graph.out_degree[uid]
        recip = graph.reciprocal_ratio[uid]
        b60 = burst_60s[uid]
        j_max = ring_jaccard_max[uid]
        t_peers = ring_peer_count[uid]
        reps = report_counts[uid]
        blks = block_received_counts[uid]
        is_sn = uid in supernodes

        # Signal 1: Engagement burst velocity (CMS count + 60s sliding-window burst)
        s_burst = min(1.0, max(0.0, (b60 - 4) / 12.0))

        # Signal 2: Low-reciprocity asymmetric outbound fanout (protects organic celebrities where in_d >> out_d)
        asymmetry = out_d / (in_d + 1.0)
        if out_d >= 26 and recip < 0.15 and asymmetry >= 2.2:
            s_fanout = min(1.0, ((out_d - 24) / 30.0) * (1.0 - recip))
        else:
            s_fanout = 0.0

        # Signal 3: Coordinated engagement ring Jaccard density
        s_ring = min(1.0, (j_max - 0.30) / 0.55) if j_max >= 0.45 and t_peers >= 2 else 0.0

        # Signal 4: Distinct user report velocity (scaled modestly for supernodes with huge audience)
        rep_denom = 12.0 if is_sn else 4.0
        s_report = min(1.0, reps / rep_denom)

        # Signal 5: Block ratio received
        blk_denom = 10.0 if is_sn else 3.0
        s_block = min(1.0, blks / blk_denom)

        attributions = {
            "outbound_burst_velocity": round(s_burst * weights["outbound_burst_velocity"], 2),
            "low_reciprocity_fanout": round(s_fanout * weights["low_reciprocity_fanout"], 2),
            "coordinated_ring_jaccard": round(s_ring * weights["coordinated_ring_jaccard"], 2),
            "report_velocity_signal": round(s_report * weights["report_velocity_signal"], 2),
            "block_ratio_signal": round(s_block * weights["block_ratio_signal"], 2),
        }
        risk_score = round(min(100.0, sum(attributions.values())), 2)

        if risk_score >= 70.0:
            risk_tier = "critical"
        elif risk_score >= risk_threshold:
            risk_tier = "high"
        elif risk_score >= 20.0:
            risk_tier = "medium"
        else:
            risk_tier = "low"

        reasons: list[str] = []
        if attributions["coordinated_ring_jaccard"] >= 8.0:
            reasons.append("COORDINATED_RING_OVERLAP")
        if attributions["low_reciprocity_fanout"] >= 8.0:
            reasons.append("LOW_RECIPROCITY_FANOUT")
        if attributions["outbound_burst_velocity"] >= 8.0:
            reasons.append("BOT_OUTBOUND_BURST")
        if attributions["report_velocity_signal"] >= 6.0:
            reasons.append("ELEVATED_REPORT_VELOCITY")
        if attributions["block_ratio_signal"] >= 5.0:
            reasons.append("HIGH_BLOCK_RATIO")
        if not reasons:
            reasons.append("ORGANIC_BASELINE")

        label = user_labels.get(uid, "organic")
        is_positive_gt = label in {"bot_swarm", "coordinated_ring"}
        is_predicted_pos = risk_score >= risk_threshold

        if label == "celebrity":
            celeb_total += 1
            if is_predicted_pos:
                celeb_fp += 1

        if is_positive_gt and is_predicted_pos:
            tp += 1
        elif (not is_positive_gt) and is_predicted_pos:
            fp += 1
            fp_cases.append(
                {
                    "user_id": uid,
                    "synthetic_label": label,
                    "risk_score": risk_score,
                    "top_reasons": reasons,
                    "attributions": attributions,
                }
            )
        elif (not is_positive_gt) and (not is_predicted_pos):
            tn += 1
        else:
            fn += 1

        trust_rows.append(
            {
                "user_id": uid,
                "risk_score": risk_score,
                "risk_tier": risk_tier,
                "top_reasons_json": json.dumps(reasons),
                "feature_attributions_json": json.dumps(attributions, sort_keys=True),
                "synthetic_label": label,
                "evaluated_at": evaluated_at,
                "snapshot_version": snapshot_version,
            }
        )

    precision = round(tp / (tp + fp), 4) if (tp + fp) > 0 else 0.0
    recall = round(tp / (tp + fn), 4) if (tp + fn) > 0 else 0.0
    f1 = round((2 * precision * recall) / (precision + recall), 4) if (precision + recall) > 0 else 0.0
    fpr = round(fp / (fp + tn), 4) if (fp + tn) > 0 else 0.0

    evaluation = {
        "note": (
            "Evaluated strictly against synthetic benchmark ground-truth labels "
            "(bot_swarm + coordinated_ring vs organic + celebrity); not real-world moderation claims."
        ),
        "threshold": risk_threshold,
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "false_positive_rate": fpr,
        "celebrity_false_positives": celeb_fp,
        "celebrity_total": celeb_total,
        "false_positive_analysis": fp_cases[:10],
    }

    return TrustScoringResult(
        trust_rows=trust_rows,
        cms_burst_counts=cms_counts,
        cms_summary=cms_summary,
        report_counts=report_counts,
        block_received_counts=block_received_counts,
        ring_jaccard_max=ring_jaccard_max,
        evaluation=evaluation,
    )

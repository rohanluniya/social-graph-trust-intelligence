"""Deterministic social graph & adversarial scenario generator.

Synthesizes users, follows, unfollows, blocks, posts, reactions, comments, shares,
reports, and GDPR right-to-erasure tombstones with heavy-tailed celebrity supernodes,
bot swarms, coordinated engagement rings, late/out-of-order retractions, duplicates,
and schema-invalid poison records routed to DLQ.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import numpy as np

BASE_EPOCH_MS = 1_760_000_000_000  # Deterministic UTC baseline (2025-10-09T...)


@dataclass(frozen=True)
class GeneratorProfile:
    name: str
    num_users: int
    num_communities: int
    num_celebrities: int
    num_bot_swarms: int
    bots_per_swarm: int
    num_rings: int
    users_per_ring: int
    num_posts: int
    avg_organic_follows: int
    erasure_count: int
    poison_count: int


PROFILES: dict[str, GeneratorProfile] = {
    "smoke": GeneratorProfile(
        name="smoke",
        num_users=400,
        num_communities=5,
        num_celebrities=4,
        num_bot_swarms=2,
        bots_per_swarm=12,
        num_rings=2,
        users_per_ring=8,
        num_posts=160,
        avg_organic_follows=12,
        erasure_count=4,
        poison_count=15,
    ),
    "local": GeneratorProfile(
        name="local",
        num_users=1600,
        num_communities=8,
        num_celebrities=8,
        num_bot_swarms=4,
        bots_per_swarm=24,
        num_rings=4,
        users_per_ring=14,
        num_posts=600,
        avg_organic_follows=16,
        erasure_count=12,
        poison_count=40,
    ),
    "stress": GeneratorProfile(
        name="stress",
        num_users=6000,
        num_communities=12,
        num_celebrities=15,
        num_bot_swarms=6,
        bots_per_swarm=45,
        num_rings=6,
        users_per_ring=20,
        num_posts=2000,
        avg_organic_follows=20,
        erasure_count=30,
        poison_count=120,
    ),
}


@dataclass
class GeneratedDataset:
    profile: str
    seed: int
    events: list[dict[str, Any]]
    user_labels: dict[str, str]  # user_id -> organic | celebrity | bot_swarm | coordinated_ring
    user_communities: dict[str, int]
    erased_seed_users: list[str]
    late_retraction_pairs: list[tuple[str, str]]
    stats: dict[str, int]


def _det_event_id(seed: int, seq: int, event_type: str, actor_id: str, target: str) -> str:
    raw = f"{seed}:{seq}:{event_type}:{actor_id}:{target}".encode()
    return f"evt_{hashlib.sha256(raw).hexdigest()[:16]}"


def generate_social_dataset(profile_name: str = "local", seed: int = 42) -> GeneratedDataset:
    """Generate a fully deterministic social graph event stream with adversarial scenarios."""
    if profile_name not in PROFILES:
        raise ValueError(f"Unknown profile '{profile_name}'. Choose from {sorted(PROFILES)}")
    cfg = PROFILES[profile_name]
    rng = np.random.default_rng(seed)

    user_ids = [f"usr_{i:06d}" for i in range(1, cfg.num_users + 1)]
    user_labels: dict[str, str] = {}
    user_communities: dict[str, int] = {}

    # Partition users deterministically into cohorts
    cursor = 0
    celebrities = user_ids[cursor : cursor + cfg.num_celebrities]
    for uid in celebrities:
        user_labels[uid] = "celebrity"
        user_communities[uid] = 0
    cursor += cfg.num_celebrities

    bot_swarms: list[list[str]] = []
    for s_idx in range(cfg.num_bot_swarms):
        swarm = user_ids[cursor : cursor + cfg.bots_per_swarm]
        bot_swarms.append(swarm)
        for uid in swarm:
            user_labels[uid] = "bot_swarm"
            user_communities[uid] = cfg.num_communities + 1 + s_idx
        cursor += cfg.bots_per_swarm

    rings: list[list[str]] = []
    for r_idx in range(cfg.num_rings):
        ring = user_ids[cursor : cursor + cfg.users_per_ring]
        rings.append(ring)
        for uid in ring:
            user_labels[uid] = "coordinated_ring"
            user_communities[uid] = cfg.num_communities + cfg.num_bot_swarms + 1 + r_idx
        cursor += cfg.users_per_ring

    organics = user_ids[cursor:]
    community_members: dict[int, list[str]] = {c: [] for c in range(cfg.num_communities)}
    for idx, uid in enumerate(organics):
        comm = idx % cfg.num_communities
        user_labels[uid] = "organic"
        user_communities[uid] = comm
        community_members[comm].append(uid)

    events: list[dict[str, Any]] = []
    seq = 0

    def emit(
        event_type: str,
        actor_id: str,
        event_ts: int,
        target_id: str | None = None,
        post_id: str | None = None,
        ingest_delay_ms: int = 250,
        schema_version: int = 2,
        metadata: dict[str, Any] | None = None,
        client_asn: str | None = None,
        privacy_flag: str = "standard",
    ) -> dict[str, Any]:
        nonlocal seq
        seq += 1
        eid = _det_event_id(seed, seq, event_type, actor_id, target_id or post_id or "")
        ev: dict[str, Any] = {
            "event_id": eid,
            "event_type": event_type,
            "actor_id": actor_id,
            "target_id": target_id,
            "post_id": post_id,
            "event_ts": int(event_ts),
            "ingest_ts": int(event_ts + max(10, ingest_delay_ms)),
            "source_system": "social-core-oltp",
            "schema_version": schema_version,
            "metadata_json": json.dumps(metadata or {}, sort_keys=True),
        }
        if schema_version == 2:
            ev["client_asn"] = client_asn or f"AS{64512 + (seq % 120)}"
            ev["trace_id"] = hashlib.md5(eid.encode()).hexdigest()
            ev["privacy_flag"] = privacy_flag
        events.append(ev)
        return ev

    # 1. User creation events (mix of v1 and v2 schema versions to test backward compatibility)
    for idx, uid in enumerate(user_ids):
        t0 = BASE_EPOCH_MS + idx * 150
        ver = 1 if (idx % 5 == 0) else 2
        emit(
            "user_created",
            uid,
            t0,
            schema_version=ver,
            metadata={"cohort_hint": "public" if user_labels[uid] == "celebrity" else "standard"},
        )

    # 2. Organic follows (intra-community clustering + reciprocal ties + celebrity supernode skew)
    follow_events_for_churn: list[tuple[str, str, int]] = []
    all_follows_seen: set[tuple[str, str]] = set()

    for idx, uid in enumerate(organics):
        comm = user_communities[uid]
        peers = community_members[comm]
        base_ts = BASE_EPOCH_MS + 1_000_000 + idx * 900

        # Heavy-tailed celebrity follows: top mega-celebrities followed by 55-80% of network (true supernode skew)
        if celebrities and rng.random() < 0.80:
            pair0 = (uid, celebrities[0])
            if pair0 not in all_follows_seen:
                all_follows_seen.add(pair0)
                emit("follow", uid, base_ts, target_id=celebrities[0])
        if len(celebrities) > 1 and rng.random() < 0.62:
            pair1 = (uid, celebrities[1])
            if pair1 not in all_follows_seen:
                all_follows_seen.add(pair1)
                emit("follow", uid, base_ts + 250, target_id=celebrities[1])
        num_celeb_follows = int(rng.integers(1, min(len(celebrities), 4) + 1))
        celeb_weights = np.array([1.0 / (k + 1) ** 1.15 for k in range(len(celebrities))])
        celeb_weights /= celeb_weights.sum()
        chosen_celebs = rng.choice(
            celebrities, size=min(num_celeb_follows, len(celebrities)), replace=False, p=celeb_weights
        )
        for c_i, cid in enumerate(chosen_celebs):
            pair = (uid, str(cid))
            if pair not in all_follows_seen:
                all_follows_seen.add(pair)
                ts = base_ts + 500 + c_i * 500
                emit("follow", uid, ts, target_id=str(cid))

        # Intra-community follows (creates strong natural communities & local triangles)
        n_intra = max(5, int(rng.poisson(cfg.avg_organic_follows * 0.78)))
        n_intra = min(n_intra, len(peers) - 1)
        if n_intra > 0:
            pos = idx // cfg.num_communities
            window_start = max(0, pos - 16)
            window_end = min(len(peers), pos + 17)
            local_pool = [p for p in peers[window_start:window_end] if p != uid]
            if len(local_pool) < n_intra:
                local_pool = [p for p in peers if p != uid]
            chosen_peers = rng.choice(
                local_pool, size=min(n_intra, len(local_pool)), replace=False
            )
            for p_i, pid in enumerate(chosen_peers):
                target = str(pid)
                pair = (uid, target)
                if pair not in all_follows_seen:
                    all_follows_seen.add(pair)
                    ts = base_ts + 5000 + p_i * 1200
                    emit("follow", uid, ts, target_id=target)
                    follow_events_for_churn.append((uid, target, ts))
                    if rng.random() < 0.56 and (target, uid) not in all_follows_seen:
                        all_follows_seen.add((target, uid))
                        emit("follow", target, ts + 45_000, target_id=uid)

        # Sparse cross-community follows (~5% of users)
        if rng.random() < 0.35:
            ct = str(rng.choice(organics))
            if ct != uid and (uid, ct) not in all_follows_seen:
                all_follows_seen.add((uid, ct))
                emit("follow", uid, base_ts + 25_000, target_id=ct)

    # Celebrities follow a modest curated set of peers/other celebrities (organic high in-degree)
    for c_idx, cid in enumerate(celebrities):
        t_c = BASE_EPOCH_MS + 1_500_000 + c_idx * 3000
        for other_c in celebrities:
            if other_c != cid and (cid, other_c) not in all_follows_seen:
                all_follows_seen.add((cid, other_c))
                emit("follow", cid, t_c, target_id=other_c)
        sample_org = rng.choice(organics, size=min(12, len(organics)), replace=False)
        for o_i, oid in enumerate(sample_org):
            if (cid, str(oid)) not in all_follows_seen:
                all_follows_seen.add((cid, str(oid)))
                emit("follow", cid, t_c + 10_000 + o_i * 4000, target_id=str(oid))

    # 3. Posts creation
    post_ids = [f"post_{i:06d}" for i in range(1, cfg.num_posts + 1)]
    post_authors: dict[str, str] = {}
    for p_idx, pid in enumerate(post_ids):
        if p_idx < len(celebrities) * 4:
            author = celebrities[p_idx % len(celebrities)]
        else:
            author = organics[p_idx % len(organics)]
        post_authors[pid] = author
        emit(
            "post_created",
            author,
            BASE_EPOCH_MS + 2_000_000 + p_idx * 1000,
            post_id=pid,
            metadata={"topic": f"topic_{p_idx % 12}"},
        )

    # 4. Organic engagement (reactions, comments, shares spread smoothly over time)
    organic_posts = post_ids[: max(20, int(len(post_ids) * 0.85))]
    for idx, uid in enumerate(organics):
        n_eng = int(rng.integers(2, 6))
        chosen_posts = rng.choice(organic_posts, size=min(n_eng, len(organic_posts)), replace=False)
        for j, pid in enumerate(chosen_posts):
            etype = str(rng.choice(["reaction", "comment", "share"], p=[0.65, 0.25, 0.10]))
            ts = BASE_EPOCH_MS + 3_000_000 + idx * 1500 + j * 180_000
            emit(etype, uid, ts, target_id=post_authors[str(pid)], post_id=str(pid))

    # 5. Adversarial Scenario A: Bot Swarms (burst outbound follows, low reciprocity, high velocity, reports & blocks)
    for s_idx, swarm in enumerate(bot_swarms):
        burst_start = BASE_EPOCH_MS + 4_500_000 + s_idx * 120_000
        shared_asn = f"AS649{s_idx:02d}"
        target_posts = post_ids[-(20 + s_idx * 6) : -(12 + s_idx * 6) or None]
        if not target_posts:
            target_posts = post_ids[-8:]
        for b_i, bot_id in enumerate(swarm):
            n_fanout = int(rng.integers(38, 62))
            victims = rng.choice(organics, size=min(n_fanout, len(organics)), replace=False)
            for v_i, vid in enumerate(victims):
                target = str(vid)
                if (bot_id, target) not in all_follows_seen:
                    all_follows_seen.add((bot_id, target))
                    emit(
                        "follow",
                        bot_id,
                        burst_start + b_i * 30 + v_i * 120,
                        target_id=target,
                        client_asn=shared_asn,
                    )
            for p_i, pid in enumerate(target_posts[:6]):
                for rep in range(3):
                    etype = "reaction" if rep < 2 else "share"
                    emit(
                        etype,
                        bot_id,
                        burst_start + 5000 + p_i * 150 + rep * 60,
                        target_id=post_authors[pid],
                        post_id=pid,
                        client_asn=shared_asn,
                    )
            n_reports = int(rng.integers(3, 8))
            reporters = rng.choice(victims[:12], size=min(n_reports, len(victims)), replace=False)
            for r_i, rid in enumerate(reporters):
                emit(
                    "report",
                    str(rid),
                    burst_start + 600_000 + r_i * 5000,
                    target_id=bot_id,
                    metadata={"reason": "spam_burst"},
                )
            n_blocks = int(rng.integers(2, 6))
            blockers = rng.choice(victims[:10], size=min(n_blocks, len(victims)), replace=False)
            for bl_i, bid in enumerate(blockers):
                emit("block", str(bid), burst_start + 650_000 + bl_i * 4000, target_id=bot_id)

    # 6. Adversarial Scenario B: Coordinated Engagement Rings (tight Jaccard overlap on identical target posts + mutual boosting)
    for r_idx, ring in enumerate(rings):
        ring_ts = BASE_EPOCH_MS + 5_500_000 + r_idx * 90_000
        ring_asn = f"AS648{r_idx:02d}"
        start_p = max(0, len(post_ids) - 60 - r_idx * 10)
        ring_posts = post_ids[start_p : start_p + 10]
        for i, u in enumerate(ring):
            for j in range(1, min(7, len(ring))):
                v = ring[(i + j) % len(ring)]
                if u != v and (u, v) not in all_follows_seen:
                    all_follows_seen.add((u, v))
                    emit("follow", u, ring_ts + i * 200 + j * 100, target_id=v, client_asn=ring_asn)
            for p_i, pid in enumerate(ring_posts):
                if rng.random() < 0.92:
                    emit(
                        "reaction",
                        u,
                        ring_ts + 20_000 + p_i * 200 + i * 25,
                        target_id=post_authors[pid],
                        post_id=pid,
                        client_asn=ring_asn,
                    )
                    emit(
                        "share",
                        u,
                        ring_ts + 20_500 + p_i * 200 + i * 30,
                        target_id=post_authors[pid],
                        post_id=pid,
                        client_asn=ring_asn,
                    )
            rep_users = rng.choice(organics, size=3, replace=False)
            for r_i, ru in enumerate(rep_users):
                emit(
                    "report",
                    str(ru),
                    ring_ts + 400_000 + r_i * 3000,
                    target_id=u,
                    metadata={"reason": "coordinated_inauthentic"},
                )

    # Add realistic borderline benign "power-fan / controversial" organic users for honest FP analysis
    borderline_count = max(2, min(4, len(organics) // 350))
    for bf_idx in range(borderline_count):
        bf_uid = organics[bf_idx * 7 + 5]
        bf_ts = BASE_EPOCH_MS + 6_100_000 + bf_idx * 50_000
        # Rapid fan-club liking spree on celebrity posts + heated replies drawing a few disputes/blocks
        for p_i, pid in enumerate(post_ids[:14]):
            emit(
                "reaction",
                bf_uid,
                bf_ts + p_i * 1500,
                target_id=post_authors[pid],
                post_id=pid,
                metadata={"organic_power_fan": True},
            )
        for rep_i in range(4):
            rep_actor = organics[(bf_idx * 29 + rep_i + 40) % len(organics)]
            if rep_actor != bf_uid:
                emit("report", rep_actor, bf_ts + 120_000 + rep_i * 3000, target_id=bf_uid, metadata={"reason": "heated_debate"})
        for blk_i in range(3):
            blk_actor = organics[(bf_idx * 31 + blk_i + 60) % len(organics)]
            if blk_actor != bf_uid:
                emit("block", blk_actor, bf_ts + 140_000 + blk_i * 3000, target_id=bf_uid)

    # Add minor baseline organic noise (single isolated report)
    noisy_organics = rng.choice(organics[50:], size=min(8, max(1, len(organics) - 50)), replace=False)
    for n_i, n_uid in enumerate(noisy_organics):
        reporter = organics[(n_i * 17 + 3) % len(organics)]
        if reporter != str(n_uid):
            emit(
                "report",
                reporter,
                BASE_EPOCH_MS + 6_250_000 + n_i * 10_000,
                target_id=str(n_uid),
                metadata={"reason": "dispute"},
            )

    # 7. Edge Deletions (unfollows), Blocks/Unblocks, and Late Out-of-Order Retractions
    n_unfollows = max(20, int(len(follow_events_for_churn) * 0.08))
    churn_indices = rng.choice(
        len(follow_events_for_churn),
        size=min(n_unfollows, len(follow_events_for_churn)),
        replace=False,
    )
    late_retraction_pairs: list[tuple[str, str]] = []
    for k, f_idx in enumerate(churn_indices):
        src, dst, f_ts = follow_events_for_churn[int(f_idx)]
        unfollow_ts = f_ts + 900_000
        if k < 15:
            # Out-of-order late arrival test: emit unfollow with normal ingest_ts,
            # then emit a stale duplicate-like older follow with event_ts < unfollow_ts
            # but LATER ingest_ts. Temporal engine MUST keep edge closed!
            emit("unfollow", src, unfollow_ts, target_id=dst, ingest_delay_ms=500)
            emit(
                "follow",
                src,
                f_ts + 1000,  # earlier event_ts than unfollow_ts
                target_id=dst,
                ingest_delay_ms=1_500_000,  # arrives much later in processing time
                metadata={"late_arrival": True},
            )
            late_retraction_pairs.append((src, dst))
        else:
            emit("unfollow", src, unfollow_ts, target_id=dst)

    # Organic block + partial unblock lifecycle
    n_org_blocks = max(6, len(organics) // 80)
    for b_idx in range(n_org_blocks):
        u_a = organics[(b_idx * 13) % len(organics)]
        u_b = organics[(b_idx * 13 + 1) % len(organics)]
        if u_a != u_b:
            t_blk = BASE_EPOCH_MS + 7_000_000 + b_idx * 5000
            emit("follow", u_a, t_blk - 50_000, target_id=u_b)
            emit("block", u_a, t_blk, target_id=u_b)
            if b_idx % 4 == 0:
                emit("unblock", u_a, t_blk + 120_000, target_id=u_b)

    # 8. GDPR Right-to-Erasure Tombstones (seed subset of organic users)
    erased_seed_users = [str(u) for u in organics[-cfg.erasure_count :]]
    for e_i, e_uid in enumerate(erased_seed_users):
        emit(
            "user_deleted",
            e_uid,
            BASE_EPOCH_MS + 8_000_000 + e_i * 2000,
            privacy_flag="erasure_request",
            metadata={"gdpr_article": "17", "reason": "user_right_to_erasure"},
        )

    # 9. Exact Duplicate Replay Injection (~2% of valid events)
    valid_count_before_dups = len(events)
    n_dups = max(10, int(valid_count_before_dups * 0.02))
    dup_indices = rng.choice(valid_count_before_dups, size=n_dups, replace=False)
    for d_idx in dup_indices:
        dup = dict(events[int(d_idx)])
        dup["ingest_ts"] = int(dup["ingest_ts"]) + 30_000  # redelivered later with same event_id
        events.append(dup)

    # 10. Schema-Invalid Poison Events -> Quarantined to DLQ
    poison_templates: list[dict[str, Any]] = [
        {"event_id": "", "event_type": "follow", "actor_id": "usr_000001", "target_id": "usr_000002", "event_ts": BASE_EPOCH_MS, "ingest_ts": BASE_EPOCH_MS},
        {"event_id": "evt_poison_badtype", "event_type": "invalid_mutation", "actor_id": "usr_000001", "target_id": "usr_000002", "event_ts": BASE_EPOCH_MS, "ingest_ts": BASE_EPOCH_MS},
        {"event_id": "evt_poison_selfloop", "event_type": "follow", "actor_id": "usr_000010", "target_id": "usr_000010", "event_ts": BASE_EPOCH_MS, "ingest_ts": BASE_EPOCH_MS},
        {"event_id": "evt_poison_negts", "event_type": "reaction", "actor_id": "usr_000005", "post_id": "post_000001", "event_ts": -999, "ingest_ts": BASE_EPOCH_MS},
        {"event_id": "evt_poison_actor", "event_type": "follow", "actor_id": "bad_actor_format", "target_id": "usr_000002", "event_ts": BASE_EPOCH_MS, "ingest_ts": BASE_EPOCH_MS},
        {"event_id": "evt_poison_nopost", "event_type": "comment", "actor_id": "usr_000005", "post_id": None, "event_ts": BASE_EPOCH_MS, "ingest_ts": BASE_EPOCH_MS},
    ]
    for p_i in range(cfg.poison_count):
        tpl = dict(poison_templates[p_i % len(poison_templates)])
        if tpl["event_id"]:
            tpl["event_id"] = f"{tpl['event_id']}_{seed}_{p_i}"
        events.append(tpl)

    # Sort deterministically by ingest_ts to emulate realistic processing-time arrival stream
    # (preserving out-of-order event_ts relative to ingest_ts!)
    events.sort(key=lambda e: (int(e.get("ingest_ts", 0)), str(e.get("event_id", ""))))

    stats = {
        "total_emitted": len(events),
        "unique_valid_emitted": valid_count_before_dups,
        "duplicates_injected": n_dups,
        "poison_injected": cfg.poison_count,
        "users_total": len(user_ids),
        "celebrities": len(celebrities),
        "bot_swarm_users": sum(len(s) for s in bot_swarms),
        "coordinated_ring_users": sum(len(r) for r in rings),
        "erased_seed_users": len(erased_seed_users),
        "late_retractions": len(late_retraction_pairs),
    }

    return GeneratedDataset(
        profile=profile_name,
        seed=seed,
        events=events,
        user_labels=user_labels,
        user_communities=user_communities,
        erased_seed_users=erased_seed_users,
        late_retraction_pairs=late_retraction_pairs,
        stats=stats,
    )

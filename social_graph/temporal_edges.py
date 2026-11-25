"""Temporal directed edge lifecycle engine with event-time LWW, blocks, and GDPR erasure."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class TemporalGraphBuildResult:
    temporal_edges: list[dict[str, Any]]
    active_edges: list[tuple[str, str]]
    active_users: list[str]
    erased_users: set[str]
    blocked_pairs: set[tuple[str, str]]
    blocks_and_deletions: list[dict[str, Any]]
    max_event_ts: int


def build_temporal_edges(
    events: list[dict[str, Any]],
    snapshot_version: int = 1,
    as_of_ts: int | None = None,
) -> TemporalGraphBuildResult:
    """Materialize SCD2-style directed temporal edges using strict event-time ordering.

    Key semantics:
    - All mutations are sorted by authoritative `(event_ts, event_id)` — NOT `ingest_ts`.
      Therefore a late-arriving stale `follow` (older `event_ts`, newer `ingest_ts`) can never
      resurrect an edge that was closed by a newer `unfollow`, `block`, or `user_deleted`.
    - Blocking `(a, b)` suppresses directed edges in BOTH directions `(a->b)` and `(b->a)`
      with `is_blocked=True`, `is_active=False`.
    - `user_deleted` (GDPR right-to-erasure) closes every active edge touching `user_id`
      with `is_deleted=True`, `is_active=False` and removes the user from `active_users`.
    """
    filtered = [
        ev for ev in events if as_of_ts is None or int(ev["event_ts"]) <= int(as_of_ts)
    ]
    # Deterministic authoritative event-time order
    ordered = sorted(filtered, key=lambda e: (int(e["event_ts"]), str(e["event_id"])))

    all_users: set[str] = set()
    erased_users: set[str] = set()
    erased_at_ts: dict[str, int] = {}

    # Directional active block set: (blocker, blocked)
    active_blocks: set[tuple[str, str]] = set()
    blocks_and_deletions: list[dict[str, Any]] = []

    # Open edge state: (src, dst) -> current open record dict
    open_edges: dict[tuple[str, str], dict[str, Any]] = {}
    closed_edges: list[dict[str, Any]] = []

    max_event_ts = 0

    def close_edge(pair: tuple[str, str], close_ts: int, deleted: bool, blocked: bool) -> None:
        rec = open_edges.pop(pair, None)
        if rec is None:
            return
        rec["valid_to"] = max(int(rec["valid_from"]), int(close_ts))
        rec["is_active"] = False
        rec["is_deleted"] = bool(deleted)
        rec["is_blocked"] = bool(blocked)
        closed_edges.append(rec)

    def is_pair_blocked(src: str, dst: str) -> bool:
        return (src, dst) in active_blocks or (dst, src) in active_blocks

    for ev in ordered:
        etype = ev["event_type"]
        actor = str(ev["actor_id"])
        target = ev.get("target_id")
        ets = int(ev["event_ts"])
        eid = str(ev["event_id"])
        if ets > max_event_ts:
            max_event_ts = ets

        if etype == "user_created":
            if actor not in erased_users:
                all_users.add(actor)
            continue

        if actor not in erased_users:
            all_users.add(actor)
        if isinstance(target, str) and target not in erased_users:
            all_users.add(target)

        if etype == "follow" and isinstance(target, str):
            if actor in erased_users or target in erased_users:
                continue
            if is_pair_blocked(actor, target):
                continue
            pair = (actor, target)
            if pair not in open_edges:
                open_edges[pair] = {
                    "edge_key": f"{actor}->{target}",
                    "src_user_id": actor,
                    "dst_user_id": target,
                    "edge_type": "follow",
                    "event_id": eid,
                    "valid_from": ets,
                    "valid_to": None,
                    "is_active": True,
                    "is_deleted": False,
                    "is_blocked": False,
                    "snapshot_version": snapshot_version,
                }

        elif etype == "unfollow" and isinstance(target, str):
            close_edge((actor, target), close_ts=ets, deleted=True, blocked=False)

        elif etype == "block" and isinstance(target, str):
            active_blocks.add((actor, target))
            blocks_and_deletions.append(
                {
                    "user_id": actor,
                    "target_user_id": target,
                    "action_type": "block",
                    "effective_ts": ets,
                    "event_id": eid,
                    "snapshot_version": snapshot_version,
                }
            )
            # Suppress edges in both directions immediately
            close_edge((actor, target), close_ts=ets, deleted=False, blocked=True)
            close_edge((target, actor), close_ts=ets, deleted=False, blocked=True)

        elif etype == "unblock" and isinstance(target, str):
            active_blocks.discard((actor, target))
            blocks_and_deletions.append(
                {
                    "user_id": actor,
                    "target_user_id": target,
                    "action_type": "unblock",
                    "effective_ts": ets,
                    "event_id": eid,
                    "snapshot_version": snapshot_version,
                }
            )

        elif etype == "user_deleted":
            erased_users.add(actor)
            erased_at_ts[actor] = ets
            all_users.discard(actor)
            blocks_and_deletions.append(
                {
                    "user_id": actor,
                    "target_user_id": None,
                    "action_type": "user_deleted",
                    "effective_ts": ets,
                    "event_id": eid,
                    "snapshot_version": snapshot_version,
                }
            )
            # Close every open edge touching the erased user
            touching = [p for p in list(open_edges.keys()) if p[0] == actor or p[1] == actor]
            for pair in touching:
                close_edge(pair, close_ts=ets, deleted=True, blocked=False)

    # Final defensive sweep: ensure 0 active edges involve erased users or blocked pairs
    for pair in list(open_edges.keys()):
        if pair[0] in erased_users or pair[1] in erased_users:
            close_edge(pair, close_ts=max_event_ts, deleted=True, blocked=False)
        elif is_pair_blocked(pair[0], pair[1]):
            close_edge(pair, close_ts=max_event_ts, deleted=False, blocked=True)

    temporal_edges = closed_edges + list(open_edges.values())
    temporal_edges.sort(key=lambda r: (r["src_user_id"], r["dst_user_id"], int(r["valid_from"])))

    active_edges = sorted(open_edges.keys())
    active_users = sorted(u for u in all_users if u not in erased_users)

    return TemporalGraphBuildResult(
        temporal_edges=temporal_edges,
        active_edges=active_edges,
        active_users=active_users,
        erased_users=erased_users,
        blocked_pairs=active_blocks,
        blocks_and_deletions=blocks_and_deletions,
        max_event_ts=max_event_ts,
    )

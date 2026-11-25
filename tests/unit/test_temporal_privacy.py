"""Unit and Hypothesis property tests for determinism, temporal edges, retractions, privacy, and redaction."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from social_graph.generator import BASE_EPOCH_MS, generate_social_dataset
from social_graph.observability import redact_value
from social_graph.temporal_edges import build_temporal_edges


@pytest.mark.unit
def test_deterministic_generator_same_seed_exact_match() -> None:
    """Same seed produces byte-for-byte identical event IDs, ordering, and cohort labels."""
    ds1 = generate_social_dataset(profile_name="smoke", seed=123)
    ds2 = generate_social_dataset(profile_name="smoke", seed=123)
    assert ds1.stats == ds2.stats
    assert ds1.user_labels == ds2.user_labels
    assert [e["event_id"] for e in ds1.events] == [e["event_id"] for e in ds2.events]


@pytest.mark.unit
def test_out_of_order_late_follow_cannot_resurrect_closed_unfollow() -> None:
    """Event-time ordering ensures a late-arriving stale follow does not resurrect an unfollowed edge."""
    events = [
        {"event_id": "e1", "event_type": "follow", "actor_id": "usr_000001", "target_id": "usr_000002", "event_ts": BASE_EPOCH_MS + 1000, "ingest_ts": BASE_EPOCH_MS + 1100},
        {"event_id": "e2", "event_type": "unfollow", "actor_id": "usr_000001", "target_id": "usr_000002", "event_ts": BASE_EPOCH_MS + 5000, "ingest_ts": BASE_EPOCH_MS + 5200},
        # Stale follow with earlier event_ts but much later ingest_ts
        {"event_id": "e3", "event_type": "follow", "actor_id": "usr_000001", "target_id": "usr_000002", "event_ts": BASE_EPOCH_MS + 2000, "ingest_ts": BASE_EPOCH_MS + 99_000},
    ]
    res = build_temporal_edges(events)
    assert ("usr_000001", "usr_000002") not in res.active_edges
    assert any(r["is_deleted"] and not r["is_active"] for r in res.temporal_edges)


@pytest.mark.unit
def test_bidirectional_block_suppression() -> None:
    """Blocking a user suppresses directed edges in both directions."""
    events = [
        {"event_id": "e1", "event_type": "follow", "actor_id": "usr_000001", "target_id": "usr_000002", "event_ts": BASE_EPOCH_MS + 1000, "ingest_ts": BASE_EPOCH_MS + 1100},
        {"event_id": "e2", "event_type": "follow", "actor_id": "usr_000002", "target_id": "usr_000001", "event_ts": BASE_EPOCH_MS + 2000, "ingest_ts": BASE_EPOCH_MS + 2100},
        {"event_id": "e3", "event_type": "block", "actor_id": "usr_000001", "target_id": "usr_000002", "event_ts": BASE_EPOCH_MS + 3000, "ingest_ts": BASE_EPOCH_MS + 3100},
    ]
    res = build_temporal_edges(events)
    assert res.active_edges == []
    assert ("usr_000001", "usr_000002") in res.blocked_pairs
    assert all(r["is_blocked"] and not r["is_active"] for r in res.temporal_edges)


@pytest.mark.unit
@settings(max_examples=30, deadline=None)
@given(
    ops=st.lists(
        st.tuples(
            st.sampled_from(["follow", "unfollow", "block", "user_deleted"]),
            st.integers(min_value=1, max_value=10),
            st.integers(min_value=1, max_value=10),
            st.integers(min_value=1, max_value=50_000),
        ),
        min_size=5,
        max_size=60,
    )
)
def test_property_erased_users_have_zero_active_edges(
    ops: list[tuple[str, int, int, int]],
) -> None:
    """Property: any user with a user_deleted tombstone has 0 active edges and is absent from active_users."""
    events = []
    for idx, (etype, a, b, dt) in enumerate(ops):
        if etype != "user_deleted" and a == b:
            b = (b % 10) + 1
        events.append(
            {
                "event_id": f"evt_{idx:05d}",
                "event_type": etype,
                "actor_id": f"usr_{a:06d}",
                "target_id": None if etype == "user_deleted" else f"usr_{b:06d}",
                "event_ts": BASE_EPOCH_MS + dt,
                "ingest_ts": BASE_EPOCH_MS + dt + 50,
            }
        )

    res = build_temporal_edges(events)
    for erased in res.erased_users:
        assert erased not in res.active_users
        for src, dst in res.active_edges:
            assert src != erased and dst != erased


@pytest.mark.unit
def test_log_redaction_strips_pii_and_secrets() -> None:
    """Observability redactor masks emails, IPv4 addresses, and secret keys."""
    payload = {
        "api_key": "super-secret-value",
        "message": "Contact alice@example.com from 192.168.1.45",
        "user_id": "usr_000001",
    }
    redacted = redact_value(payload)
    assert redacted["api_key"] == "[REDACTED]"
    assert "alice@example.com" not in redacted["message"]
    assert "192.168.1.45" not in redacted["message"]
    assert redacted["user_id"] == "usr_000001"

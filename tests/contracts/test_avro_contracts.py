"""Avro v1/v2 schema contract, backward-compatibility, and poison-event rejection tests."""

from __future__ import annotations

import pytest

from social_graph.contracts import SchemaRegistry
from social_graph.generator import BASE_EPOCH_MS


@pytest.mark.contract
def test_avro_v2_is_strictly_backward_compatible_with_v1() -> None:
    """Every v1 field is preserved and every new v2 field supplies a default."""
    registry = SchemaRegistry()
    ok, errors = registry.check_backward_compatibility()
    assert ok, f"Backward compatibility errors: {errors}"


@pytest.mark.contract
def test_avro_v1_payload_readable_by_v2_reader_schema() -> None:
    """Binary Avro serialized with v1 writer schema deserializes cleanly with v2 reader schema."""
    registry = SchemaRegistry()
    v1_record = {
        "event_id": "evt_contract_001",
        "event_type": "follow",
        "actor_id": "usr_000001",
        "target_id": "usr_000002",
        "post_id": None,
        "event_ts": BASE_EPOCH_MS,
        "ingest_ts": BASE_EPOCH_MS + 100,
        "source_system": "social-core-oltp",
        "schema_version": 1,
        "metadata_json": "{}",
    }
    raw = registry.serialize_avro(v1_record, version=1)
    decoded = registry.deserialize_avro(raw, writer_version=1, reader_version=2)
    assert decoded["event_id"] == "evt_contract_001"
    assert decoded["client_asn"] is None
    assert decoded["trace_id"] is None
    assert decoded["privacy_flag"] == "standard"


@pytest.mark.contract
@pytest.mark.parametrize(
    "bad_event,expected_prefix",
    [
        ({"event_id": "", "event_type": "follow", "actor_id": "usr_000001", "target_id": "usr_000002", "event_ts": BASE_EPOCH_MS, "ingest_ts": BASE_EPOCH_MS}, "missing_or_empty_event_id"),
        ({"event_id": "e1", "event_type": "unknown_op", "actor_id": "usr_000001", "target_id": "usr_000002", "event_ts": BASE_EPOCH_MS, "ingest_ts": BASE_EPOCH_MS}, "invalid_event_type"),
        ({"event_id": "e2", "event_type": "follow", "actor_id": "usr_000001", "target_id": "usr_000001", "event_ts": BASE_EPOCH_MS, "ingest_ts": BASE_EPOCH_MS}, "self_loop_disallowed"),
        ({"event_id": "e3", "event_type": "reaction", "actor_id": "usr_000001", "post_id": None, "event_ts": BASE_EPOCH_MS, "ingest_ts": BASE_EPOCH_MS}, "missing_post_id"),
        ({"event_id": "e4", "event_type": "follow", "actor_id": "usr_000001", "target_id": "usr_000002", "event_ts": -10, "ingest_ts": BASE_EPOCH_MS}, "invalid_event_ts"),
    ],
)
def test_poison_events_rejected_cleanly(bad_event: dict, expected_prefix: str) -> None:
    """Schema-invalid and domain-invalid poison records are rejected with clear reason codes."""
    registry = SchemaRegistry()
    is_valid, reason, _ = registry.validate_event(bad_event)
    assert not is_valid
    assert reason is not None and reason.startswith(expected_prefix)

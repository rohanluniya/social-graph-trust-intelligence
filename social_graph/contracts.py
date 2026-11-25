"""Avro schema validation, backward-compatibility checking, and event serialization."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import fastavro

from social_graph.config import REPO_ROOT

DEFAULT_SCHEMAS_DIR = REPO_ROOT / "schemas" / "avro"

VALID_EVENT_TYPES = frozenset(
    {
        "user_created",
        "follow",
        "unfollow",
        "block",
        "unblock",
        "post_created",
        "reaction",
        "comment",
        "share",
        "report",
        "user_deleted",
    }
)


class SchemaRegistry:
    """Loads pinned Avro v1/v2 schemas and validates incoming social graph events."""

    def __init__(self, schemas_dir: Path | None = None) -> None:
        self.schemas_dir = schemas_dir or DEFAULT_SCHEMAS_DIR
        self.v1_raw = json.loads((self.schemas_dir / "social_event_v1.avsc").read_text("utf-8"))
        self.v2_raw = json.loads((self.schemas_dir / "social_event_v2.avsc").read_text("utf-8"))
        self.v1_parsed = fastavro.parse_schema(self.v1_raw)
        self.v2_parsed = fastavro.parse_schema(self.v2_raw)

    def get_schema(self, version: int = 2) -> Any:
        if version == 1:
            return self.v1_parsed
        if version == 2:
            return self.v2_parsed
        raise ValueError(f"Unsupported schema version: {version}")

    def validate_event(self, payload: dict[str, Any]) -> tuple[bool, str | None, dict[str, Any]]:
        """Validate a raw event against Avro + domain invariants.

        Returns (is_valid, rejection_reason, normalized_event).
        """
        if not isinstance(payload, dict):
            return False, "payload_not_object", {}

        event_id = payload.get("event_id")
        if not isinstance(event_id, str) or not event_id.strip():
            return False, "missing_or_empty_event_id", {}

        event_type = payload.get("event_type")
        if event_type not in VALID_EVENT_TYPES:
            return False, f"invalid_event_type:{event_type}", {}

        actor_id = payload.get("actor_id")
        if not isinstance(actor_id, str) or not actor_id.startswith("usr_"):
            return False, f"invalid_actor_id:{actor_id}", {}

        event_ts = payload.get("event_ts")
        ingest_ts = payload.get("ingest_ts")
        if not isinstance(event_ts, int) or event_ts <= 0:
            return False, "invalid_event_ts", {}
        if not isinstance(ingest_ts, int) or ingest_ts <= 0:
            return False, "invalid_ingest_ts", {}

        target_id = payload.get("target_id")
        if event_type in {"follow", "unfollow", "block", "unblock", "report"}:
            if not isinstance(target_id, str) or not target_id.startswith("usr_"):
                return False, f"missing_target_id_for_{event_type}", {}
            if target_id == actor_id:
                return False, f"self_loop_disallowed_for_{event_type}", {}

        post_id = payload.get("post_id")
        if event_type in {"post_created", "reaction", "comment", "share"} and (
            not isinstance(post_id, str) or not post_id.startswith("post_")
        ):
            return False, f"missing_post_id_for_{event_type}", {}

        schema_version = payload.get("schema_version", 2)
        if schema_version not in (1, 2):
            return False, f"unsupported_schema_version:{schema_version}", {}

        normalized: dict[str, Any] = {
            "event_id": event_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "target_id": target_id,
            "post_id": post_id,
            "event_ts": int(event_ts),
            "ingest_ts": int(ingest_ts),
            "source_system": str(payload.get("source_system") or "social-core-oltp"),
            "schema_version": int(schema_version),
            "metadata_json": str(payload.get("metadata_json") or "{}"),
        }
        if schema_version == 2:
            normalized["client_asn"] = payload.get("client_asn")
            normalized["trace_id"] = payload.get("trace_id")
            normalized["privacy_flag"] = str(payload.get("privacy_flag") or "standard")

        target_schema = self.get_schema(int(schema_version))
        try:
            fastavro.validate(normalized, target_schema)
        except Exception as exc:
            return False, f"avro_validation_error:{exc}", {}

        # Canonicalize to v2 envelope for unified downstream Iceberg storage
        if schema_version == 1:
            normalized["client_asn"] = None
            normalized["trace_id"] = None
            normalized["privacy_flag"] = "standard"

        return True, None, normalized

    def serialize_avro(self, record: dict[str, Any], version: int = 2) -> bytes:
        """Encode a single event record to Avro binary bytes."""
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self.get_schema(version), record)
        return buf.getvalue()

    def deserialize_avro(
        self,
        raw_bytes: bytes,
        writer_version: int = 1,
        reader_version: int = 2,
    ) -> dict[str, Any]:
        """Decode Avro bytes using explicit writer and reader schemas to verify compatibility."""
        buf = io.BytesIO(raw_bytes)
        decoded = fastavro.schemaless_reader(
            buf,
            self.get_schema(writer_version),
            self.get_schema(reader_version),
        )
        if not isinstance(decoded, dict):
            raise TypeError(f"Expected Avro record dict, got {type(decoded).__name__}")
        return dict(decoded)

    def check_backward_compatibility(self) -> tuple[bool, list[str]]:
        """Verify that every field added in v2 provides a default and preserves v1 fields."""
        errors: list[str] = []
        v1_fields = {f["name"]: f for f in self.v1_raw["fields"]}
        v2_fields = {f["name"]: f for f in self.v2_raw["fields"]}

        for name, field_def in v1_fields.items():
            if name not in v2_fields:
                errors.append(f"Removed v1 field '{name}' breaks backward compatibility")
            elif v2_fields[name]["type"] != field_def["type"]:
                errors.append(f"Changed field type for '{name}' breaks compatibility")

        for name, field_def in v2_fields.items():
            if name not in v1_fields and "default" not in field_def:
                errors.append(f"New v2 field '{name}' is missing required default value")

        return len(errors) == 0, errors

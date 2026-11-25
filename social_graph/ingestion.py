"""Durable local WAL / topic spool ingestion with Avro validation, DLQ, deduplication, and replay."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from social_graph.contracts import SchemaRegistry
from social_graph.observability import DLQ_EVENTS_TOTAL, EVENTS_INGESTED_TOTAL


@dataclass
class IngestionResult:
    run_id: str
    total_received: int
    valid_events: list[dict[str, Any]]
    dlq_records: list[dict[str, Any]]
    duplicates_dropped: int
    spool_files: list[str]
    elapsed_ms: float
    broker_spool_recovered: int = 0


class DurableTopicSpool:
    """Partitioned durable JSONL WAL emulating Kafka topic partitions + broker-outage buffer."""

    def __init__(self, spool_dir: Path, num_partitions: int = 8) -> None:
        self.spool_dir = spool_dir
        self.num_partitions = num_partitions
        self.topic_dir = spool_dir / "social_events_topic"
        self.outage_buffer = spool_dir / "broker_outage_buffer.jsonl"
        self.offsets_file = spool_dir / "consumer_offsets.json"
        self.topic_dir.mkdir(parents=True, exist_ok=True)

    def _partition_for_key(self, key: str) -> int:
        digest = hashlib.blake2b(key.encode("utf-8"), digest_size=4).digest()
        return int.from_bytes(digest, "big") % self.num_partitions

    def _partition_path(self, partition: int) -> Path:
        return self.topic_dir / f"partition_{partition:02d}.jsonl"

    def clear(self) -> None:
        for path in self.topic_dir.glob("partition_*.jsonl"):
            path.unlink(missing_ok=True)
        self.outage_buffer.unlink(missing_ok=True)
        self.offsets_file.unlink(missing_ok=True)

    def publish_batch(
        self,
        events: list[dict[str, Any]],
        simulate_broker_outage: bool = False,
    ) -> dict[str, int]:
        """Append raw events to partitioned durable WAL or local outage spool buffer."""
        if simulate_broker_outage:
            with self.outage_buffer.open("a", encoding="utf-8") as handle:
                for ev in events:
                    handle.write(json.dumps(ev, default=str) + "\n")
            return {"buffered_outage": len(events), "published_partitions": 0}

        buckets: dict[int, list[str]] = {p: [] for p in range(self.num_partitions)}
        for ev in events:
            key = str(ev.get("actor_id") or ev.get("event_id") or "unknown")
            part = self._partition_for_key(key)
            buckets[part].append(json.dumps(ev, default=str))

        written = 0
        for part, lines in buckets.items():
            if not lines:
                continue
            with self._partition_path(part).open("a", encoding="utf-8") as handle:
                handle.write("\n".join(lines) + "\n")
            written += len(lines)
        return {"buffered_outage": 0, "published_partitions": written}

    def flush_outage_buffer(self) -> int:
        """Drain buffered records from local outage spool back into durable topic partitions."""
        if not self.outage_buffer.exists():
            return 0
        buffered: list[dict[str, Any]] = []
        with self.outage_buffer.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    buffered.append(json.loads(line))
        if buffered:
            self.publish_batch(buffered, simulate_broker_outage=False)
        self.outage_buffer.unlink(missing_ok=True)
        return len(buffered)

    def read_all(self, from_beginning: bool = True) -> list[tuple[int, int, dict[str, Any]]]:
        """Read records across all partitions, supporting full replay from offset 0."""
        offsets: dict[str, int] = {}
        if not from_beginning and self.offsets_file.exists():
            offsets = json.loads(self.offsets_file.read_text("utf-8"))

        records: list[tuple[int, int, dict[str, Any]]] = []
        new_offsets: dict[str, int] = dict(offsets)
        for part in range(self.num_partitions):
            path = self._partition_path(part)
            if not path.exists():
                continue
            start_off = 0 if from_beginning else int(offsets.get(str(part), 0))
            with path.open("r", encoding="utf-8") as handle:
                for line_idx, line in enumerate(handle):
                    if line_idx < start_off:
                        continue
                    line = line.strip()
                    if not line:
                        continue
                    records.append((part, line_idx, json.loads(line)))
                    new_offsets[str(part)] = line_idx + 1

        self.offsets_file.write_text(json.dumps(new_offsets, indent=2), encoding="utf-8")
        return records


def ingest_from_spool(
    spool: DurableTopicSpool,
    registry: SchemaRegistry,
    run_id: str,
    from_beginning: bool = True,
    seen_event_ids: set[str] | None = None,
) -> IngestionResult:
    """Consume from durable topic spool, validate Avro contracts, deduplicate, and route poison events to DLQ."""
    t0 = time.perf_counter()
    recovered = spool.flush_outage_buffer()
    raw_entries = spool.read_all(from_beginning=from_beginning)

    dedup_set = set(seen_event_ids) if seen_event_ids is not None else set()
    valid_events: list[dict[str, Any]] = []
    dlq_records: list[dict[str, Any]] = []
    duplicates_dropped = 0

    for partition, offset, payload in raw_entries:
        is_valid, reason, normalized = registry.validate_event(payload)
        if not is_valid:
            reason_code = (reason or "unknown_validation_error").split(":")[0]
            DLQ_EVENTS_TOTAL.labels(reason=reason_code).inc()
            dlq_records.append(
                {
                    "dlq_id": f"dlq_{partition}_{offset}_{run_id[:8]}",
                    "rejection_reason": reason or "unknown",
                    "source_partition": partition,
                    "source_offset": offset,
                    "raw_payload": json.dumps(payload, default=str),
                    "ingest_ts": int(time.time() * 1000),
                    "run_id": run_id,
                }
            )
            continue

        eid = normalized["event_id"]
        if eid in dedup_set:
            duplicates_dropped += 1
            continue
        dedup_set.add(eid)
        normalized["source_partition"] = partition
        normalized["source_offset"] = offset
        normalized["run_id"] = run_id
        EVENTS_INGESTED_TOTAL.labels(event_type=normalized["event_type"]).inc()
        valid_events.append(normalized)

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    spool_files = sorted(str(p.name) for p in spool.topic_dir.glob("partition_*.jsonl"))
    return IngestionResult(
        run_id=run_id,
        total_received=len(raw_entries),
        valid_events=valid_events,
        dlq_records=dlq_records,
        duplicates_dropped=duplicates_dropped,
        spool_files=spool_files,
        elapsed_ms=round(elapsed_ms, 3),
        broker_spool_recovered=recovered,
    )

"""Kafka 4.x (KRaft) producer/consumer adapter using confluent-kafka with local WAL fallback."""

from __future__ import annotations

import json
import os
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer

from social_graph.contracts import SchemaRegistry
from social_graph.ingestion import DurableTopicSpool


class SocialGraphKafkaClient:
    """Publishes Avro-validated social graph events to Kafka or falls back to local WAL spool."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str = "social.events.v2",
        dlq_topic: str = "social.events.dlq",
        fallback_spool: DurableTopicSpool | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.fallback_spool = fallback_spool
        self.registry = SchemaRegistry()

    def produce_events(self, events: list[dict[str, Any]], use_kafka: bool = False) -> dict[str, int]:
        """Validate and publish events to Kafka (distributed mode) or local WAL (local_native)."""
        if not use_kafka:
            if self.fallback_spool is None:
                raise ValueError("fallback_spool must be provided when use_kafka=False")
            return self.fallback_spool.publish_batch(events)

        producer = Producer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "zstd",
                "linger.ms": 10,
            }
        )
        sent = 0
        dlq = 0
        try:
            for ev in events:
                ok, reason, normalized = self.registry.validate_event(ev)
                if ok:
                    key = str(normalized["actor_id"]).encode("utf-8")
                    payload = self.registry.serialize_avro(normalized, version=2)
                    producer.produce(self.topic, key=key, value=payload)
                    sent += 1
                else:
                    dlq_payload = json.dumps({"reason": reason, "raw": ev}).encode("utf-8")
                    producer.produce(self.dlq_topic, value=dlq_payload)
                    dlq += 1
            producer.flush(timeout=15.0)
        except KafkaException:
            if self.fallback_spool is not None:
                return self.fallback_spool.publish_batch(events, simulate_broker_outage=True)
            raise
        return {"kafka_sent": sent, "kafka_dlq": dlq}

    def consume_batch(
        self,
        group_id: str = "sgti-graph-consumer",
        max_messages: int = 1000,
        timeout_sec: float = 2.0,
    ) -> list[dict[str, Any]]:
        """Consume Avro v2 messages from Kafka with manual offset commit semantics."""
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        records: list[dict[str, Any]] = []
        try:
            consumer.subscribe([self.topic])
            while len(records) < max_messages:
                msg = consumer.poll(timeout_sec)
                if msg is None:
                    break
                if msg.error():
                    continue
                raw_val = msg.value()
                if raw_val is None:
                    continue
                decoded = self.registry.deserialize_avro(
                    raw_val, writer_version=2, reader_version=2
                )
                records.append(decoded)
            if records:
                consumer.commit(asynchronous=False)
        finally:
            consumer.close()
        return records

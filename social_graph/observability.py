"""Structured JSON logging with PII redaction, OpenTelemetry tracing, and Prometheus metrics."""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

SENSITIVE_KEY_PATTERN = re.compile(
    r"(api[_-]?key|token|secret|password|authorization|email|ip_address|phone)",
    re.IGNORECASE,
)
EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
IPV4_PATTERN = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


def redact_value(value: Any) -> Any:
    """Redact email addresses, IPv4 addresses, and secret keys from log payloads."""
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for k, v in value.items():
            if SENSITIVE_KEY_PATTERN.search(str(k)):
                sanitized[k] = "[REDACTED]"
            else:
                sanitized[k] = redact_value(v)
        return sanitized
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, str):
        text = EMAIL_PATTERN.sub("[REDACTED_EMAIL]", value)
        text = IPV4_PATTERN.sub("[REDACTED_IP]", text)
        return text
    return value


class JsonFormatter(logging.Formatter):
    """Emit structured single-line JSON logs with automatic redaction."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_value(record.getMessage()),
        }
        extra_fields = getattr(record, "structured", None)
        if isinstance(extra_fields, dict):
            payload.update(redact_value(extra_fields))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str, log_file: Path | None = None) -> logging.Logger:
    """Return a structured JSON logger with optional file sink."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(JsonFormatter())
        logger.addHandler(stream_handler)
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        resolved = str(log_file.resolve())
        existing = [
            h for h in logger.handlers if isinstance(h, logging.FileHandler) and h.baseFilename == resolved
        ]
        if not existing:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(JsonFormatter())
            logger.addHandler(fh)
    logger.propagate = False
    return logger


def log_structured(logger: logging.Logger, level: int, message: str, **kwargs: Any) -> None:
    """Write a structured JSON log record with trace/run correlation fields."""
    logger.log(level, message, extra={"structured": kwargs})


class JsonlSpanExporter(SpanExporter):
    """Persist OpenTelemetry spans to a local JSONL file for auditable trace inspection."""

    def __init__(self, trace_file: Path) -> None:
        self.trace_file = trace_file
        self.trace_file.parent.mkdir(parents=True, exist_ok=True)
        self.exported_count = 0

    def export(self, spans: Any) -> SpanExportResult:
        try:
            with self.trace_file.open("a", encoding="utf-8") as handle:
                for span in spans:
                    record = _span_to_dict(span)
                    handle.write(json.dumps(record, default=str) + "\n")
                    self.exported_count += 1
            return SpanExportResult.SUCCESS
        except Exception:
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        return None


def _span_to_dict(span: ReadableSpan) -> dict[str, Any]:
    ctx = span.get_span_context()
    return {
        "name": span.name,
        "trace_id": format(ctx.trace_id, "032x") if ctx else None,
        "span_id": format(ctx.span_id, "016x") if ctx else None,
        "start_time_ns": span.start_time,
        "end_time_ns": span.end_time,
        "duration_ms": round(((span.end_time or 0) - (span.start_time or 0)) / 1_000_000, 3),
        "attributes": redact_value(dict(span.attributes or {})),
        "status": str(span.status.status_code),
    }


_TRACER_CONFIGURED = False
_ACTIVE_EXPORTER: JsonlSpanExporter | None = None


def configure_tracing(trace_file: Path) -> trace.Tracer:
    """Configure OpenTelemetry SDK with local JSONL exporter."""
    global _TRACER_CONFIGURED, _ACTIVE_EXPORTER
    if not _TRACER_CONFIGURED:
        resource = Resource.create({"service.name": "social-graph-trust-intelligence"})
        provider = TracerProvider(resource=resource)
        _ACTIVE_EXPORTER = JsonlSpanExporter(trace_file)
        provider.add_span_processor(SimpleSpanProcessor(_ACTIVE_EXPORTER))
        trace.set_tracer_provider(provider)
        _TRACER_CONFIGURED = True
    elif _ACTIVE_EXPORTER is not None:
        trace_file.parent.mkdir(parents=True, exist_ok=True)
        _ACTIVE_EXPORTER.trace_file = trace_file
    return trace.get_tracer("social_graph.tracer")


@contextmanager
def traced_span(
    name: str,
    trace_file: Path | None = None,
    attributes: dict[str, Any] | None = None,
) -> Iterator[dict[str, Any]]:
    """Run a code block inside an OpenTelemetry span and yield correlation metadata."""
    tracer = (
        configure_tracing(trace_file)
        if trace_file is not None
        else trace.get_tracer("social_graph.tracer")
    )
    t0 = time.perf_counter()
    with tracer.start_as_current_span(name) as span:
        if attributes:
            for key, val in redact_value(attributes).items():
                span.set_attribute(key, str(val))
        ctx = span.get_span_context()
        meta: dict[str, Any] = {
            "trace_id": format(ctx.trace_id, "032x") if ctx and ctx.trace_id else uuid.uuid4().hex,
            "span_name": name,
        }
        yield meta
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        span.set_attribute("duration_ms", round(elapsed_ms, 3))
        meta["duration_ms"] = round(elapsed_ms, 3)


def append_audit_event(audit_file: Path, event: dict[str, Any]) -> None:
    """Append an immutable redacted security or privacy audit event."""
    audit_file.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "audit_id": f"aud_{uuid.uuid4().hex[:12]}",
        "timestamp": datetime.now(UTC).isoformat(),
        **redact_value(event),
    }
    with audit_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, default=str) + "\n")


# Dedicated Prometheus metrics registry
METRICS_REGISTRY = CollectorRegistry()

EVENTS_INGESTED_TOTAL = Counter(
    "sgti_events_ingested_total",
    "Total validated social graph events ingested",
    ["event_type"],
    registry=METRICS_REGISTRY,
)
DLQ_EVENTS_TOTAL = Counter(
    "sgti_dlq_events_total",
    "Total poison or schema-invalid events quarantined to DLQ",
    ["reason"],
    registry=METRICS_REGISTRY,
)
ACTIVE_NODES_GAUGE = Gauge(
    "sgti_active_nodes",
    "Current number of active non-erased nodes in the social graph",
    registry=METRICS_REGISTRY,
)
ACTIVE_EDGES_GAUGE = Gauge(
    "sgti_active_edges",
    "Current number of active non-blocked non-deleted directed edges",
    registry=METRICS_REGISTRY,
)
PARTITION_SKEW_RATIO_GAUGE = Gauge(
    "sgti_partition_skew_ratio",
    "Partition max/mean edge skew ratio by partitioning strategy",
    ["strategy"],
    registry=METRICS_REGISTRY,
)
ERASURE_LATENCY_MS = Histogram(
    "sgti_erasure_propagation_ms",
    "End-to-end GDPR right-to-erasure tombstone propagation latency in milliseconds",
    buckets=(1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0),
    registry=METRICS_REGISTRY,
)
DQ_CHECKS_FAILED_GAUGE = Gauge(
    "sgti_dq_checks_failed",
    "Number of failing data quality checks in latest pipeline run",
    registry=METRICS_REGISTRY,
)
API_REQUESTS_TOTAL = Counter(
    "sgti_api_requests_total",
    "Total HTTP requests served by the Social Graph & Trust API",
    ["method", "path", "status"],
    registry=METRICS_REGISTRY,
)
API_LATENCY_SECONDS = Histogram(
    "sgti_api_request_latency_seconds",
    "HTTP request latency in seconds",
    ["path"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
    registry=METRICS_REGISTRY,
)
TRUST_FLAGGED_USERS_GAUGE = Gauge(
    "sgti_trust_flagged_users",
    "Count of users flagged as high or critical risk tier",
    ["risk_tier"],
    registry=METRICS_REGISTRY,
)


def render_prometheus_metrics() -> bytes:
    """Serialize Prometheus metrics for the /metrics endpoint."""
    return generate_latest(METRICS_REGISTRY)

# Compatibility & Version Pinning Matrix

All runtime dependencies, Python libraries, and container images in `social-graph-trust-intelligence` are strictly pinned with exact versions and verified open-source licenses. Zero external pretrained ML model weights are downloaded.

## 1. Host Environment & Native Runtime Verification

| Component | Verified Version | License | Notes |
| --- | --- | --- | --- |
| Host OS / Arch | macOS Darwin 24.6.0 (`arm64`, Apple M4 Pro, 14 cores, 48 GiB RAM) | — | Also portable to Linux `x86_64` / `arm64` |
| Python | `3.12.8` (via `uv 0.12.3`) | PSF-2.0 | Locked in `pyproject.toml` and `uv.lock` |
| PyIceberg | `0.12.0` (`sql-sqlite`, `pyiceberg-core`) | Apache-2.0 | Native Apache Iceberg V2 table format + time travel + compaction |
| DuckDB | `2.0.0` | MIT | Analytical serving index materialized from Iceberg Parquet files |
| Apache Arrow (`pyarrow`) | `25.0.1` | Apache-2.0 | Columnar interchange across Iceberg, DuckDB, and NumPy |
| SciPy / NumPy | `scipy==1.18.1`, `numpy==2.5.5` | BSD-3-Clause | Sparse CSR/CSC graph algorithms (PageRank, triangles, components) |
| FastAvro | `1.13.1` | MIT | Avro v1/v2 contract validation & backward-compatibility decoding |
| FastAPI / Uvicorn | `fastapi==0.163.2`, `uvicorn==0.54.0` | MIT / BSD-3-Clause | RBAC Graph & Trust API + Control Tower UI on `127.0.0.1:8050` |
| OpenTelemetry SDK | `1.45.2` | Apache-2.0 | Local JSONL span exporter (`.local_data/traces/spans.jsonl`) |
| Prometheus Client | `0.24.1` | Apache-2.0 | Live `/metrics` exposition |
| Hypothesis / Pytest | `hypothesis==6.170.15`, `pytest==9.2.0` | MPL-2.0 / MIT | Property-based graph invariants, E2E, and 7 chaos drills |

## 2. Distributed Container Stack (`docker-compose.yml`)

| Service | Pinned Image Tag | Profile(s) | Arch Compatibility |
| --- | --- | --- | --- |
| Apache Kafka (KRaft) | `apache/kafka:4.3.2` | `core`, `full` | Multi-arch (`arm64` / `amd64`) |
| Apicurio Registry | `apicurio/apicurio-registry:3.3.5` | `core`, `full` | Multi-arch (`arm64` / `amd64`) |
| RustFS (S3-compatible) | `rustfs/rustfs:1.0.6` | `core`, `full` | Multi-arch (`arm64` / `amd64`) |
| Lakekeeper Iceberg REST | `quay.io/lakekeeper/catalog:v0.15.1` | `core`, `full` | Multi-arch (`arm64` / `amd64`) |
| PostgreSQL + PostGIS | `imresamu/postgis:17-3.6.1-alpine3.23` | `core`, `full` | Multi-arch (`arm64` / `amd64`) |
| Apache Spark + GraphFrames | `apache/spark:4.1.4-scala2.13-java17-python3-ubuntu` | `analytics`, `full` | Multi-arch (`arm64` / `amd64`) |
| Trino | `trinodb/trino:484` | `analytics`, `full` | Multi-arch (`arm64` / `amd64`) |
| Apache Airflow | `apache/airflow:slim-3.4.0-python3.12` | `analytics`, `full` | Multi-arch (`arm64` / `amd64`) |
| Prometheus | `prom/prometheus:v3.16.0` | `observability`, `full` | Multi-arch (`arm64` / `amd64`) |
| Grafana | `grafana/grafana:13.0.14` | `observability`, `full` | Multi-arch (`arm64` / `amd64`) |
| Grafana Loki | `grafana/loki:3.6.22` | `observability`, `full` | Multi-arch (`arm64` / `amd64`) |
| OpenTelemetry Collector | `otel/opentelemetry-collector-contrib:0.166.0-${OTEL_ARCH:-arm64}` | `observability`, `full` | Explicit arch suffix (`arm64` / `amd64`) |
| Marquez (OpenLineage) | `marquezproject/marquez:0.51.1` | `observability`, `full` | Pinned `platform: linux/amd64` (emulation on ARM64) |
| Standalone API Container | `python:3.12.8-slim-bookworm` + `ghcr.io/astral-sh/uv:0.12.3` | `core`, `standalone` | Multi-arch (`arm64` / `amd64`) |

## 3. Host Execution Constraint Disclosure

On this verification workstation (macOS `arm64`), Docker, Podman, Node.js, and host JVM runtimes are unavailable. Accordingly:
- All benchmarks, tests, chaos drills, Iceberg V2 medallion tables, DuckDB serving indexes, and live Control Tower UI screenshots were executed natively in `local_native` mode (Python 3.12 + PyIceberg + DuckDB + SciPy Sparse CSR + FastAPI).
- All distributed artifacts (`docker-compose.yml`, `processing/spark/graphframes_social_job.py`, `processing/airflow/dags/social_graph_dags.py`, `sql/trino/social_graph_analytics.sql`, `infra/kubernetes/`) are statically validated (YAML/JSON/Python syntax and pinning audit) and ready to run on any Docker/Kubernetes host, while explicitly not claimed as live-executed on this Dockerless host.

# Security Threat Model & Privacy Controls

## 1. Assets & Trust Boundaries

| Asset | Sensitivity | Storage Location | Control |
| --- | --- | --- | --- |
| Social Graph Edges & User Features | High (behavioral & privacy-sensitive) | Iceberg V2 (`silver.*`, `gold.*`), DuckDB serving | RBAC API keys, GDPR Article 17 erasure propagation |
| Security & Privacy Audit Logs | High (compliance evidence) | `.local_data/logs/audit.jsonl` | Append-only JSONL, automatic PII/secret redaction |
| API Keys & Service Credentials | Critical | Environment variables / Kubernetes `Secret` | Never committed; static audit blocks secret patterns |
| Poison / Malformed Payloads | Medium (untrusted input) | `bronze.quarantine_dlq` | Schema validation isolates payloads before processing |

## 2. STRIDE Threat Analysis & Mitigations

| Threat | Attack Vector | Implemented Mitigation | Production Hardening Gap (Explicitly Disclosed) |
| --- | --- | --- | --- |
| **Spoofing** | Unauthenticated calls to `/v1/*` APIs | `X-API-Key` RBAC middleware returns `401` and writes audit event | Replace static dev API keys with OIDC/JWT + short-lived mTLS service certs |
| **Tampering / Elevation** | Viewer invoking `POST /v1/privacy/erase/{id}` or cache rebuild | Role hierarchy (`viewer < analyst < admin`) enforces `403 Forbidden` | Integrate fine-grained policy engine (OPA/Cedar) + dual-approval for bulk erasures |
| **Data Poisoning** | Malformed events, negative timestamps, self-loops | Avro v1/v2 `fastavro` validator quarantines 100% of bad events to `bronze.quarantine_dlq` | Add per-producer rate limits and signed event envelopes at Kafka ingress |
| **Information Disclosure** | PII (emails, IPv4s, keys) leaking into logs or traces | `redact_value()` strips emails, IPs, and secret keys from logs, OTel spans, and audits | Enable envelope encryption / KMS-managed per-user crypto-shredding keys on Bronze raw storage |
| **Denial of Service (Skew)** | Viral celebrity supernode or bot fan-out exhausting single partition | Skew-aware vertex-cut salting + fixed-memory HLL/CMS sketches | Add API gateway rate limiting and Spark dynamic executor autoscaling |
| **Repudiation** | Unrecorded GDPR erasure or unauthorized access attempts | Immutable `audit.jsonl` records `audit_id`, timestamp, role, action, and zero-leak proof | Ship audit stream to WORM S3 Object Lock / SIEM |

## 3. Supply Chain & Container Hygiene

- **Zero floating container tags:** `scripts/static_audit.py` enforces exact pinned image versions across `docker-compose.yml`, `Dockerfile.standalone`, and Kubernetes manifests.
- **Non-root container execution:** `Dockerfile.standalone` and `infra/kubernetes/api-deployment.yaml` run as UID `10001` with `allowPrivilegeEscalation: false` and `capabilities.drop: [ALL]`.
- **SBOM & License Policy:** `scripts/generate_sbom.py` produces a CycloneDX 1.5 SBOM (`results/security/sbom.json`); `scripts/check_licenses.py` verifies permissive open-source licenses across all 70 installed packages with zero external pretrained model weights.

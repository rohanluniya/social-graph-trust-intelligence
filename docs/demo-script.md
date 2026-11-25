# Live Walkthrough & Demo Script

Follow this 5-minute scripted demo to showcase every engineering capability live.

## 1. Materialize the Iceberg V2 Social Graph Medallion
```bash
uv run python scripts/run_pipeline.py --profile local --seed 42
```
Highlight: `47,789` events ingested -> `936` duplicates dropped, `40` poison records quarantined to `bronze.quarantine_dlq`, `32,380` active directed edges, `9/9` DQ checks passed, `0.0` exact incremental parity diff (`40.2×` structural speedup), `-23.34%` supernode skew reduction, and `0/8` celebrity false positives.

## 2. Launch Control Tower & API (`127.0.0.1:8050`)
```bash
uv run uvicorn services.api.main:app --host 127.0.0.1 --port 8050
```
Open `http://127.0.0.1:8050/` in a browser:
- Walk through the 6 KPI cards and the 16-shard **Naive Hash vs. Skew-Aware Vertex-Cut** SVG chart.
- Click **Celebrity Hub** (`usr_000001`) in the Interactive Node Inspector -> observe high in-degree (`>1,100`) and **LOW risk score (`0.0`)** proving zero celebrity false positives.
- Click **Top Risk Alert** (`usr_000012`) -> inspect explainable feature attributions (`coordinated_ring_jaccard`, `low_reciprocity_fanout`, `outbound_burst_velocity`) and exact vs. HyperLogLog 2-hop reach (`937` vs `967`).

## 3. Demonstrate Live RBAC & GDPR Right-to-Erasure
```bash
# Unauthenticated call -> 401 Unauthorized
curl -i http://127.0.0.1:8050/v1/overview

# Viewer role attempting GDPR erasure -> 403 Forbidden
curl -i -X POST -H "X-API-Key: dev-viewer-key-local-only" \
  http://127.0.0.1:8050/v1/privacy/erase/usr_000012

# Admin role executing GDPR Article 17 erasure -> 200 OK with zero-leak proof
curl -s -X POST -H "X-API-Key: dev-admin-key-local-only" \
  http://127.0.0.1:8050/v1/privacy/erase/usr_000012 | python3 -m json.tool
```

## 4. Inspect Prometheus Metrics & Chaos Evidence
```bash
curl -s http://127.0.0.1:8050/metrics | grep sgti_
cat results/chaos/chaos_results.json
```

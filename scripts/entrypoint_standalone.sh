#!/usr/bin/env bash
set -euo pipefail

PROFILE="${SGTI_PROFILE:-local}"
SEED="${SGTI_SEED:-42}"
HOST="${SGTI_API_HOST:-0.0.0.0}"
PORT="${SGTI_API_PORT:-8050}"

echo "[sgti] Materializing Iceberg V2 social graph medallion (profile=${PROFILE}, seed=${SEED})..."
/app/.venv/bin/python /app/scripts/run_pipeline.py --profile "${PROFILE}" --seed "${SEED}"

echo "[sgti] Launching Social Graph & Trust Intelligence API on ${HOST}:${PORT}..."
exec /app/.venv/bin/uvicorn services.api.main:app --host "${HOST}" --port "${PORT}"

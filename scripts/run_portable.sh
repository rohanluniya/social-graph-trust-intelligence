#!/usr/bin/env bash
# Zero-Docker one-command portable runner for macOS ARM64 & Linux x86_64/ARM64.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_DIR}"

PROFILE="${1:-local}"
SEED="${2:-42}"
HOST="${SGTI_API_HOST:-127.0.0.1}"
PORT="${SGTI_API_PORT:-8050}"

if ! command -v uv >/dev/null 2>&1; then
  echo "Error: 'uv' is required. Install via: curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
  exit 1
fi

echo "[1/3] Syncing pinned Python 3.12 environment..."
uv sync --python 3.12 --extra dev

echo "[2/3] Running end-to-end Iceberg V2 social graph & trust pipeline (profile=${PROFILE}, seed=${SEED})..."
uv run python scripts/run_pipeline.py --profile "${PROFILE}" --seed "${SEED}"

echo "[3/3] Starting Control Tower & API at http://${HOST}:${PORT} (Ctrl+C to stop)..."
exec uv run uvicorn services.api.main:app --host "${HOST}" --port "${PORT}"

.PHONY: setup pipeline serve test test-fast chaos benchmark lint typecheck security-audit clean

setup:
	uv sync --python 3.12 --extra dev

pipeline:
	uv run python scripts/run_pipeline.py --profile local --seed 42

serve:
	uv run uvicorn services.api.main:app --host 127.0.0.1 --port 8050

test-fast:
	uv run pytest -m "unit or contract"

test:
	uv run pytest --junitxml=results/tests/junit.xml

chaos:
	uv run pytest tests/chaos/test_chaos_drills.py -v

benchmark:
	uv run python benchmarks/run_benchmarks.py

lint:
	uv run ruff check .

typecheck:
	uv run mypy social_graph models services ingestion

security-audit:
	uv run python scripts/static_audit.py
	uv run python scripts/check_licenses.py
	uv run python scripts/generate_sbom.py

clean:
	rm -rf .local_data .pytest_cache .mypy_cache .ruff_cache .hypothesis

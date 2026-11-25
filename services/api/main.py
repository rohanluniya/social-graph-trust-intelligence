"""FastAPI Social Graph & Trust Intelligence API with RBAC, Prometheus metrics, and Control Tower UI."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from social_graph.config import REPO_ROOT, PlatformConfig, get_config
from social_graph.iceberg_store import IcebergLakehouse
from social_graph.observability import (
    API_LATENCY_SECONDS,
    API_REQUESTS_TOTAL,
    append_audit_event,
    get_logger,
    log_structured,
    render_prometheus_metrics,
    traced_span,
)
from social_graph.pipeline import run_full_pipeline
from social_graph.privacy import propagate_user_erasure
from social_graph.serving_store import GraphServingStore

DASHBOARD_DIR = REPO_ROOT / "apps" / "graph-dashboard"

ROLE_RANK = {"viewer": 1, "analyst": 2, "admin": 3}


class AppState:
    def __init__(self, config: PlatformConfig) -> None:
        self.config = config
        self.config.ensure_dirs()
        self.started_at = time.monotonic()
        self.logger = get_logger("social_graph.api", config.logs_dir / "api.jsonl")
        self.audit_file = config.logs_dir / "audit.jsonl"
        self.trace_file = config.traces_dir / "spans.jsonl"
        self.lakehouse = IcebergLakehouse(config.catalog_db, config.warehouse_dir)
        self.serving = GraphServingStore(config.serving_db, self.lakehouse)
        self.summary_path = config.state_dir / "latest_run_summary.json"

    def ensure_initialized(self) -> None:
        if not self.summary_path.exists():
            run_full_pipeline(profile="local", seed=42, config=self.config, clean_spool=True)
        self.serving.ensure_loaded()

    def load_summary(self) -> dict[str, Any]:
        self.ensure_initialized()
        return dict(json.loads(self.summary_path.read_text("utf-8")))

    def role_for_api_key(self, api_key: str | None) -> str | None:
        if not api_key:
            return None
        if api_key == self.config.admin_api_key:
            return "admin"
        if api_key == self.config.analyst_api_key:
            return "analyst"
        if api_key == self.config.viewer_api_key:
            return "viewer"
        return None


def create_app(config: PlatformConfig | None = None) -> FastAPI:
    cfg = config or get_config()
    state = AppState(cfg)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        state.ensure_initialized()
        log_structured(
            state.logger,
            logging.INFO,
            "Social Graph & Trust Intelligence API initialized",
            port=cfg.api_port,
            runtime_mode=cfg.runtime_mode,
        )
        yield

    app = FastAPI(
        title="Social Graph & Trust Intelligence Platform API",
        version="0.1.0",
        description=(
            "Distributed social interaction graph analytics, temporal Iceberg V2 edge lakehouse, "
            "explainable trust/abuse signals, and GDPR right-to-erasure propagation API."
        ),
        lifespan=lifespan,
    )
    app.state.sgti = state

    if DASHBOARD_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(DASHBOARD_DIR)), name="static")

    @app.middleware("http")
    async def observability_middleware(request: Request, call_next: Any) -> Response:
        t0 = time.perf_counter()
        response: Response = await call_next(request)
        elapsed = time.perf_counter() - t0
        path_label = request.url.path
        if path_label.startswith("/v1/graph/node/"):
            path_label = "/v1/graph/node/{user_id}"
        elif path_label.startswith("/v1/privacy/erase/"):
            path_label = "/v1/privacy/erase/{user_id}"
        API_REQUESTS_TOTAL.labels(
            method=request.method,
            path=path_label,
            status=str(response.status_code),
        ).inc()
        API_LATENCY_SECONDS.labels(path=path_label).observe(elapsed)
        return response

    def require_role(min_role: str):
        def _checker(
            request: Request,
            x_api_key: str | None = Header(default=None, alias="X-API-Key"),
        ) -> str:
            role = state.role_for_api_key(x_api_key)
            if role is None:
                append_audit_event(
                    state.audit_file,
                    {
                        "action": "auth_rejected_unauthorized",
                        "path": request.url.path,
                        "method": request.method,
                        "status": 401,
                    },
                )
                raise HTTPException(
                    status_code=401,
                    detail="Unauthorized: valid X-API-Key header required.",
                )
            if ROLE_RANK[role] < ROLE_RANK[min_role]:
                append_audit_event(
                    state.audit_file,
                    {
                        "action": "auth_rejected_forbidden",
                        "actor_role": role,
                        "required_role": min_role,
                        "path": request.url.path,
                        "method": request.method,
                        "status": 403,
                    },
                )
                raise HTTPException(
                    status_code=403,
                    detail=f"Forbidden: role '{role}' lacks required '{min_role}' scope.",
                )
            return role

        return _checker

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def index() -> Response:
        html_path = DASHBOARD_DIR / "index.html"
        if not html_path.exists():
            return HTMLResponse("<h1>Social Graph & Trust Intelligence API</h1>")
        return FileResponse(str(html_path), media_type="text/html")

    @app.get("/health")
    async def health() -> dict[str, Any]:
        summary = state.load_summary()
        counts = state.serving.get_live_counts()
        mat_iso = summary.get("last_materialized_at")
        freshness_sec = 0.0
        if mat_iso:
            dt = datetime.fromisoformat(mat_iso)
            freshness_sec = round(max(0.0, (datetime.now(UTC) - dt).total_seconds()), 2)
        return {
            "status": "healthy",
            "service": "social-graph-trust-intelligence",
            "runtime_mode": state.config.runtime_mode,
            "snapshot_version": summary["snapshot_version"],
            "active_nodes": counts["active_nodes"],
            "active_edges": counts["active_edges"],
            "erased_users": counts["erased_users"],
            "freshness_seconds": freshness_sec,
            "last_materialized_at": mat_iso,
            "uptime_seconds": round(time.monotonic() - state.started_at, 2),
        }

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(
            content=render_prometheus_metrics(),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.get("/v1/overview")
    async def overview(_role: str = Depends(require_role("viewer"))) -> dict[str, Any]:
        with traced_span("api.overview", trace_file=state.trace_file):
            summary = state.load_summary()
            counts = state.serving.get_live_counts()
            mat_iso = summary.get("last_materialized_at")
            freshness_sec = 0.0
            if mat_iso:
                dt = datetime.fromisoformat(mat_iso)
                freshness_sec = round(max(0.0, (datetime.now(UTC) - dt).total_seconds()), 2)
            # Reflect live post-erasure counts dynamically
            out = dict(summary)
            out["active_nodes"] = counts["active_nodes"]
            out["active_edges"] = counts["active_edges"]
            out["erased_users"] = counts["erased_users"]
            out["freshness_seconds"] = freshness_sec
            return out

    @app.get("/v1/graph/node/{user_id}")
    async def get_node(
        user_id: str,
        _role: str = Depends(require_role("viewer")),
    ) -> dict[str, Any]:
        with traced_span(
            "api.get_node", trace_file=state.trace_file, attributes={"user_id": user_id}
        ):
            detail = state.serving.get_node_detail(user_id)
            if detail is None:
                raise HTTPException(status_code=404, detail=f"User '{user_id}' not found.")
            return detail

    @app.get("/v1/graph/nodes")
    async def list_nodes(
        risk_tier: str | None = Query(default=None),
        cohort: str | None = Query(default=None),
        is_supernode: bool | None = Query(default=None),
        search: str | None = Query(default=None),
        limit: int = Query(default=40, ge=1, le=200),
        _role: str = Depends(require_role("viewer")),
    ) -> dict[str, Any]:
        nodes = state.serving.list_nodes(
            risk_tier=risk_tier,
            cohort=cohort,
            is_supernode=is_supernode,
            search=search,
            limit=limit,
        )
        return {"count": len(nodes), "nodes": nodes}

    @app.get("/v1/graph/communities")
    async def communities(
        limit: int = Query(default=20, ge=1, le=100),
        _role: str = Depends(require_role("viewer")),
    ) -> dict[str, Any]:
        summary = state.load_summary()
        data = state.serving.get_communities_summary(limit=limit)
        return {
            "snapshot_version": summary["snapshot_version"],
            "component_count": summary["component_count"],
            "largest_component_size": summary["largest_component_size"],
            "community_count": summary["community_count"],
            "modularity_q": summary["modularity_q"],
            **data,
        }

    @app.get("/v1/trust/alerts")
    async def trust_alerts(
        limit: int = Query(default=30, ge=1, le=200),
        min_score: float = Query(default=45.0, ge=0.0, le=100.0),
        _role: str = Depends(require_role("viewer")),
    ) -> dict[str, Any]:
        summary = state.load_summary()
        alerts = state.serving.get_trust_alerts(limit=limit, min_score=min_score)
        return {
            "snapshot_version": summary["snapshot_version"],
            "alert_count": len(alerts),
            "evaluation": summary["trust_evaluation"],
            "risk_tier_counts": summary.get("risk_tier_counts", {}),
            "alerts": alerts,
        }

    @app.post("/v1/privacy/erase/{user_id}")
    async def erase_user(
        user_id: str,
        role: str = Depends(require_role("admin")),
    ) -> dict[str, Any]:
        with traced_span(
            "api.privacy_erase",
            trace_file=state.trace_file,
            attributes={"user_id": user_id, "role": role},
        ):
            receipt = propagate_user_erasure(
                user_id=user_id,
                lakehouse=state.lakehouse,
                serving=state.serving,
                audit_file=state.audit_file,
                actor_role=role,
            )
            # Refresh snapshot_version and counts inside latest_run_summary.json
            summary = state.load_summary()
            counts = state.serving.get_live_counts()
            summary["snapshot_version"] = receipt["snapshot_version"]
            summary["active_nodes"] = counts["active_nodes"]
            summary["active_edges"] = counts["active_edges"]
            summary["erased_users"] = counts["erased_users"]
            summary["last_materialized_at"] = receipt["erased_at"]
            state.summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
            return receipt

    @app.get("/v1/privacy/audit")
    async def privacy_audit(
        limit: int = Query(default=25, ge=1, le=100),
        _role: str = Depends(require_role("viewer")),
    ) -> dict[str, Any]:
        events: list[dict[str, Any]] = []
        if state.audit_file.exists():
            lines = state.audit_file.read_text("utf-8").splitlines()
            for line in reversed(lines[-limit:]):
                if line.strip():
                    events.append(json.loads(line))
        return {"count": len(events), "events": events}

    @app.get("/v1/dq")
    async def data_quality(_role: str = Depends(require_role("viewer"))) -> dict[str, Any]:
        summary = state.load_summary()
        dq_data = state.serving.get_dq_and_dlq()
        ol_path = state.config.state_dir / "openlineage_events.json"
        lineage = json.loads(ol_path.read_text("utf-8")) if ol_path.exists() else {"events": []}
        return {
            "run_id": summary["run_id"],
            "dq_passed": summary["dq_passed"],
            "dq_total": summary["dq_total"],
            "iceberg_tables": state.lakehouse.table_inventory(),
            "lineage_events_count": len(lineage.get("events", [])),
            **dq_data,
        }

    @app.post("/v1/admin/rebuild-serving")
    async def rebuild_serving(role: str = Depends(require_role("admin"))) -> dict[str, Any]:
        with traced_span("api.rebuild_serving", trace_file=state.trace_file):
            res = state.serving.rebuild_from_iceberg()
            append_audit_event(
                state.audit_file,
                {"action": "serving_cache_rebuilt_from_iceberg", "actor_role": role, **res},
            )
            return res

    return app


app = create_app()

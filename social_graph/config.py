"""Environment-driven settings for the Social Graph & Trust Intelligence Platform."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class PlatformConfig:
    """Central runtime configuration loaded from environment variables."""

    runtime_mode: str = field(
        default_factory=lambda: os.getenv("SGTI_RUNTIME_MODE", "local_native")
    )
    data_dir: Path = field(
        default_factory=lambda: Path(
            os.getenv("SGTI_DATA_DIR", str(REPO_ROOT / ".local_data"))
        ).resolve()
    )
    api_host: str = field(default_factory=lambda: os.getenv("SGTI_API_HOST", "127.0.0.1"))
    api_port: int = field(default_factory=lambda: int(os.getenv("SGTI_API_PORT", "8050")))
    log_level: str = field(default_factory=lambda: os.getenv("SGTI_LOG_LEVEL", "INFO"))

    # Local dev RBAC API keys (injected via secret manager in production deployments)
    viewer_api_key: str = field(
        default_factory=lambda: os.getenv("SGTI_VIEWER_API_KEY", "dev-viewer-key-local-only")
    )
    analyst_api_key: str = field(
        default_factory=lambda: os.getenv("SGTI_ANALYST_API_KEY", "dev-analyst-key-local-only")
    )
    admin_api_key: str = field(
        default_factory=lambda: os.getenv("SGTI_ADMIN_API_KEY", "dev-admin-key-local-only")
    )

    # Graph partitioning & skew mitigation
    num_partitions: int = field(
        default_factory=lambda: int(os.getenv("SGTI_NUM_PARTITIONS", "16"))
    )
    supernode_degree_threshold: int = field(
        default_factory=lambda: int(os.getenv("SGTI_SUPERNODE_DEGREE_THRESHOLD", "80"))
    )
    supernode_salt_buckets: int = field(
        default_factory=lambda: int(os.getenv("SGTI_SUPERNODE_SALT_BUCKETS", "8"))
    )

    # Approximate sketches
    hll_precision: int = field(default_factory=lambda: int(os.getenv("SGTI_HLL_PRECISION", "10")))
    cms_depth: int = field(default_factory=lambda: int(os.getenv("SGTI_CMS_DEPTH", "5")))
    cms_width: int = field(default_factory=lambda: int(os.getenv("SGTI_CMS_WIDTH", "2048")))

    @property
    def warehouse_dir(self) -> Path:
        return self.data_dir / "warehouse"

    @property
    def catalog_db(self) -> Path:
        return self.data_dir / "catalog" / "iceberg_catalog.sqlite"

    @property
    def spool_dir(self) -> Path:
        return self.data_dir / "spool"

    @property
    def state_dir(self) -> Path:
        return self.data_dir / "state"

    @property
    def logs_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def traces_dir(self) -> Path:
        return self.data_dir / "traces"

    @property
    def serving_db(self) -> Path:
        return self.data_dir / "serving" / "graph_serving.duckdb"

    @property
    def schemas_dir(self) -> Path:
        return REPO_ROOT / "schemas" / "avro"

    def ensure_dirs(self) -> None:
        for path in (
            self.data_dir,
            self.warehouse_dir,
            self.catalog_db.parent,
            self.spool_dir,
            self.state_dir,
            self.logs_dir,
            self.traces_dir,
            self.serving_db.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)


def get_config(data_dir: Path | str | None = None) -> PlatformConfig:
    """Return a PlatformConfig, optionally overriding data_dir for isolated tests."""
    if data_dir is not None:
        return PlatformConfig(data_dir=Path(data_dir).resolve())
    return PlatformConfig()

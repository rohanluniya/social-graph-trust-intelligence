#!/usr/bin/env python3
"""Generate CycloneDX 1.5 JSON Software Bill of Materials (SBOM) for installed Python dependencies."""

from __future__ import annotations

import importlib.metadata as md
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    components = []
    for dist in sorted(md.distributions(), key=lambda d: (d.metadata["Name"] or "").lower()):
        name = dist.metadata["Name"]
        version = dist.version
        components.append(
            {
                "type": "library",
                "name": name,
                "version": version,
                "purl": f"pkg:pypi/{name.lower()}@{version}",
            }
        )

    sbom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{uuid.uuid5(uuid.NAMESPACE_DNS, 'social-graph-trust-intelligence:0.1.0')}",
        "version": 1,
        "metadata": {
            "timestamp": datetime.now(UTC).isoformat(),
            "component": {
                "type": "application",
                "name": "social-graph-trust-intelligence",
                "version": "0.1.0",
                "licenses": [{"license": {"id": "Apache-2.0"}}],
            },
        },
        "components": components,
    }

    out_path = REPO_ROOT / "results" / "security" / "sbom.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(sbom, indent=2), encoding="utf-8")
    print(f"SBOM written: {len(components)} components -> {out_path}")


if __name__ == "__main__":
    main()

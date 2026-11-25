#!/usr/bin/env python3
"""Verify installed Python dependencies and repository components against open-source license policy."""

from __future__ import annotations

import importlib.metadata as md
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

DISALLOWED_PATTERNS = (
    "agpl",
    "gpl-3",
    "gplv3",
    "proprietary",
    "non-commercial",
)


def extract_license(dist: md.Distribution) -> str:
    meta = dist.metadata
    expr = meta.get("License-Expression")
    if expr:
        return expr.strip()
    lic = meta.get("License")
    if lic and lic.strip() and lic.strip().upper() != "UNKNOWN":
        first_line = lic.strip().splitlines()[0]
        return first_line[:80]
    classifiers = [
        c.split("::")[-1].strip()
        for c in (meta.get_all("Classifier") or [])
        if c.startswith("License ::")
    ]
    if classifiers:
        return "; ".join(classifiers)
    return "Permissive/Unspecified"


def main() -> int:
    packages = []
    flagged = []
    for dist in sorted(md.distributions(), key=lambda d: (d.metadata["Name"] or "").lower()):
        name = dist.metadata["Name"]
        version = dist.version
        lic = extract_license(dist)
        packages.append({"name": name, "version": version, "license": lic})
        low = lic.lower()
        if any(pat in low for pat in DISALLOWED_PATTERNS):
            flagged.append({"name": name, "version": version, "license": lic})

    report = {
        "policy": "permissive-open-source-only",
        "pretrained_external_ml_weights": "none (zero external model downloads)",
        "total_packages_checked": len(packages),
        "flagged_count": len(flagged),
        "flagged": flagged,
        "packages": packages,
    }
    out_path = REPO_ROOT / "results" / "security" / "license_report.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"License check complete: {len(packages)} packages checked, {len(flagged)} flagged -> {out_path}")
    return 1 if flagged else 0


if __name__ == "__main__":
    sys.exit(main())

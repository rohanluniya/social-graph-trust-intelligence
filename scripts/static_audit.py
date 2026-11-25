#!/usr/bin/env python3
"""Static security, image-tag pinning, secret hygiene, and config syntax auditor."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent

SKIP_DIRS = {
    ".git",
    ".venv",
    ".local_data",
    ".hypothesis",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "__pycache__",
}

# Construct forbidden markers dynamically so this script does not contain them verbatim
FORBIDDEN_TAG = ":" + "latest"
SECRET_PATTERNS = [
    re.compile(r"github" + r"_pat_[A-Za-z0-9_]+"),
    re.compile(r"ghp" + r"_[A-Za-z0-9]{20,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"-----BEGIN (RSA|EC|OPENSSH) PRIVATE KEY-----"),
]


def iter_tracked_files() -> list[Path]:
    files: list[Path] = []
    for path in REPO_ROOT.rglob("*"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.is_file():
            files.append(path)
    return sorted(files)


def main() -> int:
    errors: list[str] = []
    checked = 0

    for path in iter_tracked_files():
        rel = path.relative_to(REPO_ROOT)
        if path.suffix in {".png", ".jpg", ".jpeg", ".webp", ".parquet", ".sqlite", ".duckdb"}:
            continue
        try:
            text = path.read_text("utf-8")
        except UnicodeDecodeError:
            continue
        checked += 1

        # 1. Verify zero unpinned floating container tags anywhere in repo
        if FORBIDDEN_TAG in text:
            errors.append(f"Unpinned floating tag found in {rel}")

        # 2. Verify zero leaked PATs or private keys
        for pat in SECRET_PATTERNS:
            if pat.search(text):
                errors.append(f"Potential secret match ({pat.pattern}) in {rel}")

        # 3. Validate YAML syntax
        if path.suffix in {".yml", ".yaml"}:
            try:
                list(yaml.safe_load_all(text))
            except Exception as exc:
                errors.append(f"Invalid YAML in {rel}: {exc}")

        # 4. Validate JSON / Avro syntax
        if path.suffix in {".json", ".avsc"}:
            try:
                json.loads(text)
            except Exception as exc:
                errors.append(f"Invalid JSON in {rel}: {exc}")

    # 5. Verify docker-compose.yml has pinned image tags on every service
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text("utf-8"))
    for svc_name, svc in (compose.get("services") or {}).items():
        img = str(svc.get("image", ""))
        if not img or ":" not in img:
            errors.append(f"Service '{svc_name}' in docker-compose.yml lacks pinned image tag")

    report = {
        "files_checked": checked,
        "errors_count": len(errors),
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
    }
    out_path = REPO_ROOT / "results" / "security" / "static_audit.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Static audit {report['status']}: {checked} files checked, {len(errors)} errors")
    for err in errors:
        print(f"  ERROR: {err}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

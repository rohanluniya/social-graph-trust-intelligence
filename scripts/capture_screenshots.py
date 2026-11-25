#!/usr/bin/env python3
"""Capture live headless Chrome screenshots of the Control Tower UI using the shared concurrency lock."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CHROME_BIN = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
LOCK_DIR = "/tmp/chrome_screenshot_lock"


def main() -> int:
    url = os.getenv("SGTI_UI_URL", "http://127.0.0.1:8050/")
    assets_dir = REPO_ROOT / "docs" / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    top_png = assets_dir / "graph_control_tower_top.png"
    full_png = assets_dir / "graph_control_tower_full.png"

    if not Path(CHROME_BIN).exists():
        print(f"Chrome binary not found at {CHROME_BIN}", file=sys.stderr)
        return 1

    # Acquire cross-agent Chrome concurrency lock
    while True:
        try:
            os.mkdir(LOCK_DIR)
            break
        except FileExistsError:
            time.sleep(1.0)

    try:
        cmd_top = [
            CHROME_BIN,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            "--hide-scrollbars",
            "--virtual-time-budget=6000",
            "--window-size=1440,920",
            f"--screenshot={top_png}",
            url,
        ]
        subprocess.run(cmd_top, check=True, capture_output=True)
        time.sleep(1.5)

        cmd_full = [
            CHROME_BIN,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            "--hide-scrollbars",
            "--virtual-time-budget=6000",
            "--window-size=1440,2520",
            f"--screenshot={full_png}",
            url,
        ]
        subprocess.run(cmd_full, check=True, capture_output=True)
    finally:
        try:
            os.rmdir(LOCK_DIR)
        except OSError:
            pass

    print(f"Captured screenshots:\n  - {top_png} ({top_png.stat().st_size} bytes)\n  - {full_png} ({full_png.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

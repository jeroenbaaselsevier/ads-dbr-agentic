#!/usr/bin/env python3
"""
tools/databricks/deploy_and_run.py
===================================
Deploy a local Python notebook to Databricks and submit a one-time run.

Usage:
    python tools/databricks/deploy_and_run.py <notebook_path> [--cluster-id ID]

Stdout (JSON):
    {"run_id": "...", "notebook_path_remote": "...", "status": "started"}

Exit codes:
    0 — deploy + run started
    1 — deploy or run submission failed

This wraps ./deploy.sh for a stable, scriptable interface.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CLUSTER = "0107-154653-j5wd510m"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("notebook_path", help="Relative path to .py notebook")
    parser.add_argument("--cluster-id", default=DEFAULT_CLUSTER)
    args = parser.parse_args()

    notebook = Path(args.notebook_path)
    if not notebook.exists():
        print(json.dumps({"error": f"Notebook not found: {args.notebook_path}"}))
        return 1

    deploy_script = REPO_ROOT / "deploy.sh"
    if not deploy_script.exists():
        print(json.dumps({"error": "deploy.sh not found at repo root"}))
        return 1

    cmd = [str(deploy_script), str(notebook), "--run"]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))

    if result.returncode != 0:
        print(
            json.dumps(
                {
                    "error": "Deploy or run start failed",
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }
            )
        )
        return 1

    # Extract run_id from deploy.sh output (printed as "Run ID: <id>")
    run_id = None
    for line in result.stdout.splitlines():
        if "run" in line.lower() and "id" in line.lower():
            parts = line.split()
            for part in parts:
                if part.isdigit():
                    run_id = part
                    break

    print(
        json.dumps(
            {
                "run_id": run_id,
                "notebook_path_remote": str(notebook),
                "status": "started",
                "raw_output": result.stdout,
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

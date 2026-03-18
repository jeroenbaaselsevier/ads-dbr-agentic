#!/usr/bin/env python3
"""
tools/local/postprocess.py
===========================
Thin launcher for local post-processing scripts.

Activates .venv, resolves the active project (if present), and runs the
specified script with correct output_dir and tmp paths.

Usage:
    python tools/local/postprocess.py <script_path> \
        [--s3-input s3://rads-projects/...] \
        [--output-dir projects/2026_NLD_journal_trend/output]

If --output-dir is not supplied, the tool checks .agent-state/active_project.json
and defaults to the active project's output folder, falling back to ./output/.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
AGENT_STATE = REPO_ROOT / ".agent-state" / "active_project.json"


def _get_active_project() -> dict | None:
    """Read .agent-state/active_project.json if it exists."""
    if AGENT_STATE.exists():
        data = json.loads(AGENT_STATE.read_text())
        if isinstance(data, dict):
            return data
    return None


def _resolve_output_dir(explicit: str | None) -> str:
    """Resolve the output directory: explicit > active project > ./output."""
    if explicit:
        return explicit
    project = _get_active_project()
    if project:
        return os.path.join(project.get("local_root", ""), "output")
    return "./output"


def _resolve_tmp_dir() -> str:
    """Resolve the tmp directory from active project or fallback."""
    project = _get_active_project()
    if project:
        return os.path.join(project.get("local_root", ""), "tmp")
    return "./tmp"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local post-processing script")
    parser.add_argument("script_path", help="Path to the Python script to run")
    parser.add_argument("--s3-input", default=None, help="S3 URI for input parquet")
    parser.add_argument("--output-dir", default=None, help="Override output directory")
    args = parser.parse_args()

    script = Path(args.script_path)
    if not script.exists():
        print(json.dumps({"error": f"Script not found: {args.script_path}"}))
        return 1

    output_dir = _resolve_output_dir(args.output_dir)
    tmp_dir = _resolve_tmp_dir()

    # Ensure dirs exist
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(tmp_dir, exist_ok=True)

    # Build environment
    env = os.environ.copy()
    env["OUTPUT_DIR"] = output_dir
    env["TMP_DIR"] = tmp_dir
    if args.s3_input:
        env["S3_INPUT"] = args.s3_input

    # Activate venv
    venv_python = REPO_ROOT / ".venv" / "bin" / "python3"
    python = str(venv_python) if venv_python.exists() else sys.executable

    result = subprocess.run(
        [python, str(script)],
        env=env,
        cwd=str(REPO_ROOT),
    )
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())

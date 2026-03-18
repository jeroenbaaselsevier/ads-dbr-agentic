#!/usr/bin/env python3
"""
activate_project.py
===================
Activate an existing project by reading its manifest and writing
.agent-state/active_project.json.

Use this when resuming work on an existing project without recreating it.

Usage:
    python scripts/activate_project.py \
        --project-id 2026_NLD_journal_trend \
        [--session-id 20260318T1620]

Output (JSON):
    {
      "project_id": "2026_NLD_journal_trend",
      "session_id": "20260318T1620",
      "active_state_path": ".agent-state/active_project.json",
      "local_root": "projects/2026_NLD_journal_trend",
      "s3_root": "s3://rads-projects/short_term/2026/2026_NLD_journal_trend",
      "dbfs_root": "/mnt/els/rads-projects/short_term/2026/2026_NLD_journal_trend",
      "databricks_workspace_root": "/Workspace/rads/projects/2026_NLD_journal_trend",
      "defaults": { ... }
    }
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PROJECTS_DIR = REPO_ROOT / "projects"
AGENT_STATE_DIR = REPO_ROOT / ".agent-state"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Activate an existing project"
    )
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--session-id", default=None,
                        help="Session ID (default: auto-generated from UTC time)")
    args = parser.parse_args()

    project_dir = PROJECTS_DIR / args.project_id
    manifest_path = project_dir / "project.yaml"

    if not manifest_path.exists():
        print(f"ERROR: manifest not found: {manifest_path}", file=sys.stderr)
        return 1

    manifest = yaml.safe_load(manifest_path.read_text())
    if not manifest or not isinstance(manifest, dict):
        print(f"ERROR: invalid manifest: {manifest_path}", file=sys.stderr)
        return 1

    session_id = args.session_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M")

    # Read paths from manifest, deriving any missing ones
    paths = manifest.get("paths", {})
    local_root = paths.get("local_root", f"projects/{args.project_id}")
    s3_root = paths.get("s3_root", "")
    dbfs_root = paths.get("dbfs_root", "")
    ws_root = paths.get("databricks_workspace_root", "")

    # Write active state
    AGENT_STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_file = AGENT_STATE_DIR / "active_project.json"
    state = {
        "project_id": args.project_id,
        "session_id": session_id,
        "started_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "local_root": local_root,
        "manifest_path": f"{local_root}/project.yaml",
        "s3_root": s3_root,
        "dbfs_root": dbfs_root,
        "databricks_workspace_root": ws_root,
    }
    state_file.write_text(json.dumps(state, indent=2) + "\n")

    # Build result with defaults from manifest
    defaults = manifest.get("defaults", {})
    folders = manifest.get("folders", {})

    result = {
        "project_id": args.project_id,
        "session_id": session_id,
        "active_state_path": ".agent-state/active_project.json",
        "local_root": local_root,
        "s3_root": s3_root,
        "dbfs_root": dbfs_root,
        "databricks_workspace_root": ws_root,
        "defaults": defaults,
        "folders": folders,
    }
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

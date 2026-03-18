#!/usr/bin/env python3
"""
init_project.py
===============
Create or reuse a project scaffold under projects/<project_id>/.

Generates the local folder structure, project.yaml manifest, context stubs,
and prints canonical paths as JSON for agent consumption.

Usage:
    python scripts/init_project.py \
        --year 2026 \
        --iso3 NLD \
        --short-name journal_trend \
        --display-name "Journal trend analysis for client X" \
        [--ani-stamp 20260301] \
        [--create-remote]

Output (JSON):
    {
      "created": true,
      "project_id": "2026_NLD_journal_trend",
      "local_root": "projects/2026_NLD_journal_trend",
      "s3_root": "s3://rads-projects/short_term/2026/2026_NLD_journal_trend",
      "dbfs_root": "/mnt/els/rads-projects/short_term/2026/2026_NLD_journal_trend",
      "databricks_workspace_root": "/Workspace/rads/projects/2026_NLD_journal_trend"
    }
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PROJECTS_DIR = REPO_ROOT / "projects"

# S3 bucket (no trailing slash)
S3_BUCKET = "rads-projects"
S3_PREFIX = "short_term"

# Databricks mount and workspace prefixes
DBFS_MOUNT = "/mnt/els/rads-projects/short_term"
WORKSPACE_PREFIX = "/Workspace/rads/projects"

# Valid ISO3 codes are 3 uppercase letters or the special value INTERNAL
ISO3_PATTERN = re.compile(r"^[A-Z]{3}$")
SHORT_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,60}$")


def validate_args(year: int, iso3: str, short_name: str) -> list[str]:
    """Return a list of validation errors (empty if all OK)."""
    errors = []
    if year < 2020 or year > 2099:
        errors.append(f"year must be 2020–2099, got {year}")
    if iso3 != "INTERNAL" and not ISO3_PATTERN.match(iso3):
        errors.append(
            f"iso3 must be 3 uppercase letters or INTERNAL, got '{iso3}'"
        )
    if not SHORT_NAME_PATTERN.match(short_name):
        errors.append(
            f"short-name must be lowercase snake_case (2-61 chars), got '{short_name}'"
        )
    return errors


def derive_paths(year: int, project_id: str) -> dict:
    """Derive all canonical paths from year + project_id."""
    return {
        "local_root": f"projects/{project_id}",
        "s3_root": f"s3://{S3_BUCKET}/{S3_PREFIX}/{year}/{project_id}",
        "dbfs_root": f"{DBFS_MOUNT}/{year}/{project_id}",
        "databricks_workspace_root": f"{WORKSPACE_PREFIX}/{project_id}",
    }


def derive_folders(year: int, project_id: str) -> dict:
    """Derive standard subfolder paths."""
    local_root = f"projects/{project_id}"
    remote_base = f"{DBFS_MOUNT}/{year}/{project_id}"
    s3_base = f"s3://{S3_BUCKET}/{S3_PREFIX}/{year}/{project_id}"
    return {
        "local_output": f"{local_root}/output",
        "local_tmp": f"{local_root}/tmp",
        "spark_cache": f"{remote_base}/cache",
        "s3_output": f"{s3_base}/output",
        "s3_cache": f"{s3_base}/cache",
    }


def build_manifest(
    project_id: str,
    display_name: str,
    year: int,
    iso3: str,
    short_name: str,
    ani_stamp: str | None,
    paths: dict,
    folders: dict,
) -> str:
    """Render project.yaml content."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        f"id: {project_id}",
        f'display_name: "{display_name}"',
        f"year: {year}",
        f"iso3: {iso3}",
        f"short_name: {short_name}",
        "status: active",
        "owner: analyst-agent",
        f"created_at: {now}",
    ]
    if ani_stamp:
        lines.append(f'ani_stamp: "{ani_stamp}"')

    lines.append("")
    lines.append("paths:")
    for k, v in paths.items():
        lines.append(f"  {k}: {v}")

    lines.append("")
    lines.append("folders:")
    for k, v in folders.items():
        lines.append(f"  {k}: {v}")

    lines.append("")
    lines.append("defaults:")
    lines.append(
        f"  spark_notebook: projects/{project_id}/notebooks/spark/{short_name}.py"
    )
    lines.append(
        f"  local_postprocess: projects/{project_id}/scripts/local/postprocess.py"
    )
    return "\n".join(lines) + "\n"


def scaffold_project(project_dir: Path, project_id: str, display_name: str) -> None:
    """Create all subdirectories and stub files."""
    subdirs = [
        "notebooks/spark",
        "notebooks/exploratory",
        "scripts/local",
        "context/session-notes",
        "context/lessons",
        "output",
        "tmp",
    ]
    for d in subdirs:
        (project_dir / d).mkdir(parents=True, exist_ok=True)

    # README
    readme = project_dir / "README.md"
    if not readme.exists():
        readme.write_text(
            f"# {display_name}\n\n"
            f"Project ID: `{project_id}`\n\n"
            "## Overview\n\n_TODO: describe the analysis._\n"
        )

    # Context stubs
    stubs = {
        "context/brief.md": f"# Brief — {project_id}\n\n_TODO: describe the research question._\n",
        "context/decisions.md": f"# Decisions — {project_id}\n\n_Record key decisions here._\n",
        "context/open-questions.md": f"# Open Questions — {project_id}\n\n_Track unresolved questions._\n",
        "context/summary.md": f"# Summary — {project_id}\n\n_Updated at project closeout._\n",
        "context/deliverables.yaml": "# Deliverables manifest — updated at closeout\ndeliverables: []\n",
    }
    for rel, content in stubs.items():
        f = project_dir / rel
        if not f.exists():
            f.write_text(content)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create or reuse a project scaffold"
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--iso3", type=str, required=True)
    parser.add_argument("--short-name", type=str, required=True)
    parser.add_argument("--display-name", type=str, required=True)
    parser.add_argument("--ani-stamp", type=str, default=None)
    parser.add_argument(
        "--create-remote",
        action="store_true",
        help="Print S3 mkdir commands (does not execute them)",
    )
    args = parser.parse_args()

    errors = validate_args(args.year, args.iso3, args.short_name)
    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        return 1

    project_id = f"{args.year}_{args.iso3}_{args.short_name}"
    project_dir = PROJECTS_DIR / project_id
    paths = derive_paths(args.year, project_id)
    folders = derive_folders(args.year, project_id)

    already_exists = project_dir.exists() and (project_dir / "project.yaml").exists()

    if already_exists:
        result = {
            "created": False,
            "project_id": project_id,
            **paths,
        }
        print(json.dumps(result, indent=2))
        return 0

    # Create scaffold
    project_dir.mkdir(parents=True, exist_ok=True)
    scaffold_project(project_dir, project_id, args.display_name)

    # Write manifest
    manifest = build_manifest(
        project_id,
        args.display_name,
        args.year,
        args.iso3,
        args.short_name,
        args.ani_stamp,
        paths,
        folders,
    )
    (project_dir / "project.yaml").write_text(manifest)

    if args.create_remote:
        s3_root = paths["s3_root"]
        print("# Remote S3 folders to create:", file=sys.stderr)
        for sub in ["cache", "output", "exports", "logs"]:
            print(
                f"aws s3api put-object --bucket {S3_BUCKET} "
                f"--key {S3_PREFIX}/{args.year}/{project_id}/{sub}/",
                file=sys.stderr,
            )

    result = {
        "created": True,
        "project_id": project_id,
        **paths,
    }
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

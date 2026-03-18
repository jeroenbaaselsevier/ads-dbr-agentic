#!/usr/bin/env python3
"""
closeout_project.py
===================
Finalize a project session: write session summary, deliverables manifest,
lessons file, and improvement intake record.

Usage:
    python scripts/closeout_project.py \
        --project-id 2026_NLD_journal_trend \
        --session-id 20260318T1620 \
        --status completed \
        [--summary "Produced journal trend charts for 2015-2025"] \
        [--deliverables output/trend.png,output/trend.csv]

Outputs:
    projects/<project_id>/context/session-notes/<session_id>.md
    projects/<project_id>/context/lessons/<session_id>.yaml  (if lessons provided)
    projects/<project_id>/context/deliverables.yaml          (updated)
    agent-improvement/inbox/<date>_<project_id>_<session_id>.yaml (if lessons with scope != project)
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PROJECTS_DIR = REPO_ROOT / "projects"
INBOX_DIR = REPO_ROOT / "agent-improvement" / "inbox"
AGENT_STATE_DIR = REPO_ROOT / ".agent-state"


def write_session_summary(
    project_dir: Path,
    session_id: str,
    project_id: str,
    status: str,
    summary: str | None,
    deliverable_paths: list[str],
) -> Path:
    """Write session-notes/<session_id>.md."""
    notes_dir = project_dir / "context" / "session-notes"
    notes_dir.mkdir(parents=True, exist_ok=True)
    out = notes_dir / f"{session_id}.md"

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        f"# Session {session_id} — {project_id}",
        "",
        f"**Status:** {status}",
        f"**Timestamp:** {now}",
        "",
        "## Summary",
        "",
        summary or "_Fill in what was accomplished._",
        "",
        "## Data sources used",
        "",
        "_List tables and stamps used._",
        "",
        "## Outputs produced",
        "",
    ]
    if deliverable_paths:
        for p in deliverable_paths:
            lines.append(f"- `{p}`")
    else:
        lines.append("_None recorded._")

    lines.extend(["", "## Open items", "", "_None._", ""])
    out.write_text("\n".join(lines))
    return out


def update_deliverables(
    project_dir: Path,
    deliverable_paths: list[str],
    session_id: str,
) -> Path:
    """Append to context/deliverables.yaml."""
    deliverables_file = project_dir / "context" / "deliverables.yaml"
    deliverables_file.parent.mkdir(parents=True, exist_ok=True)

    existing: dict = {}
    if deliverables_file.exists():
        text = deliverables_file.read_text()
        if text.strip():
            existing = yaml.safe_load(text) or {}

    entries = existing.get("deliverables", [])
    if not isinstance(entries, list):
        entries = []

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for p in deliverable_paths:
        entries.append({"path": p, "session": session_id, "added_at": now})

    existing["deliverables"] = entries
    deliverables_file.write_text(yaml.dump(existing, default_flow_style=False, sort_keys=False))
    return deliverables_file


def write_lessons_stub(
    project_dir: Path,
    session_id: str,
    project_id: str,
) -> Path:
    """Write an empty lessons stub for manual filling."""
    lessons_dir = project_dir / "context" / "lessons"
    lessons_dir.mkdir(parents=True, exist_ok=True)
    out = lessons_dir / f"{session_id}.yaml"
    if not out.exists():
        stub = {
            "session_id": session_id,
            "project_id": project_id,
            "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lessons": [],
        }
        out.write_text(
            "# Session lessons — fill in or let the agent populate\n"
            + yaml.dump(stub, default_flow_style=False, sort_keys=False)
        )
    return out


def write_intake_record(
    session_id: str,
    project_id: str,
    lessons_file: Path,
) -> Path | None:
    """
    Copy lessons with scope != 'project' into agent-improvement/inbox/.
    Returns the intake file path, or None if no cross-project lessons exist.
    """
    if not lessons_file.exists():
        return None

    data = yaml.safe_load(lessons_file.read_text())
    if not data or not data.get("lessons"):
        return None

    cross_project = [
        l for l in data["lessons"] if l.get("scope", "project") != "project"
    ]
    if not cross_project:
        return None

    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    intake_file = INBOX_DIR / f"{date_str}_{project_id}_{session_id}.yaml"

    record = {
        "source_project": project_id,
        "source_session": session_id,
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lessons": cross_project,
    }
    intake_file.write_text(yaml.dump(record, default_flow_style=False, sort_keys=False))
    return intake_file


def update_project_status(project_dir: Path, status: str) -> None:
    """Update the status field in project.yaml."""
    manifest = project_dir / "project.yaml"
    if not manifest.exists():
        return
    data = yaml.safe_load(manifest.read_text())
    if data and isinstance(data, dict):
        data["status"] = status
        manifest.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


def main() -> int:
    parser = argparse.ArgumentParser(description="Finalize a project session")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--session-id", required=True)
    # --status kept as alias for backward compat; prefer --session-status
    parser.add_argument(
        "--status",
        choices=["completed", "paused", "blocked"],
        help="Session status (alias for --session-status, kept for compat)",
    )
    parser.add_argument(
        "--session-status",
        choices=["completed", "paused", "blocked"],
        help="Session status: completed | paused | blocked",
    )
    parser.add_argument(
        "--project-status",
        choices=["active", "completed", "archived"],
        default=None,
        help="Project lifecycle status (only set when the entire project is done)",
    )
    parser.add_argument("--summary", default=None)
    parser.add_argument(
        "--deliverables",
        default="",
        help="Comma-separated list of output paths",
    )
    args = parser.parse_args()

    # Resolve session status: --session-status takes priority over --status
    session_status = args.session_status or args.status
    if not session_status:
        print("ERROR: provide --session-status or --status", file=sys.stderr)
        return 1

    project_dir = PROJECTS_DIR / args.project_id
    if not project_dir.exists():
        print(f"ERROR: project dir not found: {project_dir}", file=sys.stderr)
        return 1

    deliverable_list = [
        d.strip() for d in args.deliverables.split(",") if d.strip()
    ]

    # 1. Session summary
    summary_file = write_session_summary(
        project_dir,
        args.session_id,
        args.project_id,
        session_status,
        args.summary,
        deliverable_list,
    )

    # 2. Deliverables manifest
    if deliverable_list:
        update_deliverables(project_dir, deliverable_list, args.session_id)

    # 3. Lessons stub
    lessons_file = write_lessons_stub(
        project_dir, args.session_id, args.project_id
    )

    # 4. Intake record (cross-project lessons only)
    intake_file = write_intake_record(
        args.session_id, args.project_id, lessons_file
    )

    # 5. Update project status if explicitly set, or infer from session
    if args.project_status:
        update_project_status(project_dir, args.project_status)
    elif session_status == "completed" and not args.project_status:
        # Session completed does NOT auto-set project to completed;
        # a project may span multiple sessions.
        pass

    # 6. Clear active project state
    active_state = AGENT_STATE_DIR / "active_project.json"
    if active_state.exists():
        active_state.unlink()

    result = {
        "session_summary": str(summary_file.relative_to(REPO_ROOT)),
        "lessons_file": str(lessons_file.relative_to(REPO_ROOT)),
        "deliverables_updated": bool(deliverable_list),
        "intake_record": (
            str(intake_file.relative_to(REPO_ROOT)) if intake_file else None
        ),
    }
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

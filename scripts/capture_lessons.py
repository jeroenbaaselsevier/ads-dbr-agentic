#!/usr/bin/env python3
"""
capture_lessons.py
==================
Normalize raw session lessons into structured YAML records and write them
to the project's context/lessons/ folder and optionally to the improvement
inbox.

Can be called interactively (with --interactive) to collect lessons from
stdin, or programmatically by passing a JSON array of lesson dicts.

Usage:
    # Interactive — prompts for each lesson
    python scripts/capture_lessons.py \
        --project-id 2026_NLD_journal_trend \
        --session-id 20260318T1620 \
        --interactive

    # Programmatic — pass JSON on stdin
    echo '[{"scope":"global","memory_type":"semantic",...}]' | \
    python scripts/capture_lessons.py \
        --project-id 2026_NLD_journal_trend \
        --session-id 20260318T1620 \
        --from-json

Output:
    projects/<project_id>/context/lessons/<session_id>.yaml  (updated)
    agent-improvement/inbox/<date>_<project_id>_<session_id>.yaml  (if cross-project lessons)
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PROJECTS_DIR = REPO_ROOT / "projects"
INBOX_DIR = REPO_ROOT / "agent-improvement" / "inbox"

VALID_SCOPES = {"project", "client", "user", "global"}
VALID_MEMORY_TYPES = {"semantic", "procedural", "episodic"}
VALID_CATEGORIES = {
    "schema", "query", "workflow", "output_style", "tooling",
    "bug", "dashboard", "naming", "sharing", "profile",
}
VALID_CONFIDENCE = {"low", "medium", "high"}
VALID_IMPACT = {"low", "medium", "high"}


def generate_lesson_id(session_id: str, index: int) -> str:
    """Generate a deterministic lesson ID."""
    date_part = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"LES-{date_part}-{index:03d}"


def validate_lesson(lesson: dict) -> list[str]:
    """Return validation errors for a lesson dict."""
    errors = []
    for field in ("scope", "memory_type", "category", "summary", "confidence"):
        if field not in lesson:
            errors.append(f"missing required field: {field}")

    if lesson.get("scope") and lesson["scope"] not in VALID_SCOPES:
        errors.append(f"invalid scope: {lesson['scope']}")
    if lesson.get("memory_type") and lesson["memory_type"] not in VALID_MEMORY_TYPES:
        errors.append(f"invalid memory_type: {lesson['memory_type']}")
    if lesson.get("category") and lesson["category"] not in VALID_CATEGORIES:
        errors.append(f"invalid category: {lesson['category']}")
    if lesson.get("confidence") and lesson["confidence"] not in VALID_CONFIDENCE:
        errors.append(f"invalid confidence: {lesson['confidence']}")
    if lesson.get("impact") and lesson["impact"] not in VALID_IMPACT:
        errors.append(f"invalid impact: {lesson['impact']}")
    return errors


def prompt_lesson(index: int) -> dict | None:
    """Interactively prompt for one lesson. Returns None if user skips."""
    print(f"\n--- Lesson #{index} ---")
    summary = input("Summary (empty to stop): ").strip()
    if not summary:
        return None

    scope = input(f"Scope ({'/'.join(VALID_SCOPES)}) [project]: ").strip() or "project"
    memory_type = input(f"Memory type ({'/'.join(VALID_MEMORY_TYPES)}) [procedural]: ").strip() or "procedural"
    category = input(f"Category ({'/'.join(VALID_CATEGORIES)}) [workflow]: ").strip() or "workflow"
    confidence = input(f"Confidence ({'/'.join(VALID_CONFIDENCE)}) [medium]: ").strip() or "medium"
    impact = input(f"Impact ({'/'.join(VALID_IMPACT)}) [medium]: ").strip() or "medium"
    recurrence = input("Recurrence (first_time/repeated/systematic) [first_time]: ").strip() or "first_time"
    target = input("Suggested target file (empty to skip): ").strip() or None

    lesson = {
        "scope": scope,
        "memory_type": memory_type,
        "category": category,
        "summary": summary,
        "confidence": confidence,
        "impact": impact,
        "recurrence_hint": recurrence,
        "status": "captured",
    }
    if target:
        lesson["suggested_action"] = {"target": target, "change_type": "doc_update"}
    return lesson


def write_lessons_file(
    project_dir: Path,
    session_id: str,
    project_id: str,
    lessons: list[dict],
) -> Path:
    """Write or merge into context/lessons/<session_id>.yaml."""
    lessons_dir = project_dir / "context" / "lessons"
    lessons_dir.mkdir(parents=True, exist_ok=True)
    out = lessons_dir / f"{session_id}.yaml"

    existing_lessons = []
    if out.exists():
        data = yaml.safe_load(out.read_text())
        if data and isinstance(data, dict):
            existing_lessons = data.get("lessons", [])

    # Assign IDs to new lessons
    start_idx = len(existing_lessons) + 1
    for i, lesson in enumerate(lessons, start=start_idx):
        lesson["id"] = generate_lesson_id(session_id, i)
        lesson["project_id"] = project_id
        lesson["captured_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if "status" not in lesson:
            lesson["status"] = "captured"

    all_lessons = existing_lessons + lessons

    record = {
        "session_id": session_id,
        "project_id": project_id,
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lessons": all_lessons,
    }
    out.write_text(yaml.dump(record, default_flow_style=False, sort_keys=False))
    return out


def write_intake(
    session_id: str,
    project_id: str,
    lessons: list[dict],
) -> Path | None:
    """Write cross-project lessons to agent-improvement/inbox/."""
    cross_project = [l for l in lessons if l.get("scope", "project") != "project"]
    if not cross_project:
        return None

    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    intake = INBOX_DIR / f"{date_str}_{project_id}_{session_id}.yaml"

    record = {
        "source_project": project_id,
        "source_session": session_id,
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lessons": cross_project,
    }
    intake.write_text(yaml.dump(record, default_flow_style=False, sort_keys=False))
    return intake


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture structured session lessons")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--session-id", required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--interactive", action="store_true")
    group.add_argument("--from-json", action="store_true")
    args = parser.parse_args()

    project_dir = PROJECTS_DIR / args.project_id
    if not project_dir.exists():
        print(f"ERROR: project dir not found: {project_dir}", file=sys.stderr)
        return 1

    lessons: list[dict] = []

    if args.interactive:
        idx = 1
        while True:
            lesson = prompt_lesson(idx)
            if lesson is None:
                break
            errs = validate_lesson(lesson)
            if errs:
                print(f"  Validation errors: {', '.join(errs)}", file=sys.stderr)
                continue
            lessons.append(lesson)
            idx += 1
    else:
        raw = sys.stdin.read().strip()
        if not raw:
            print("ERROR: no JSON input on stdin", file=sys.stderr)
            return 1
        lessons = json.loads(raw)
        for i, l in enumerate(lessons, 1):
            errs = validate_lesson(l)
            if errs:
                print(f"ERROR: lesson #{i}: {', '.join(errs)}", file=sys.stderr)
                return 1

    if not lessons:
        print("No lessons captured.")
        return 0

    # Write lessons
    lessons_file = write_lessons_file(
        project_dir, args.session_id, args.project_id, lessons
    )

    # Write intake for cross-project lessons
    intake_file = write_intake(args.session_id, args.project_id, lessons)

    result = {
        "lessons_captured": len(lessons),
        "lessons_file": str(lessons_file.relative_to(REPO_ROOT)),
        "intake_record": (
            str(intake_file.relative_to(REPO_ROOT)) if intake_file else None
        ),
    }
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

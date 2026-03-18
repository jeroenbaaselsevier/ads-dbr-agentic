#!/usr/bin/env python3
"""
promote_lessons.py
==================
Draft or apply promotions from the agent-improvement inbox to canonical
files in agent-core/.

Phase 1 (current): draft mode only — reads inbox lessons, classifies them,
proposes target files and actions, and writes a promotion record.

Phase 2 (future): --apply for safe, narrow classes of changes (profiles,
references, runbooks, recipes, manifests). Core-rules edits remain
human-reviewed.

Usage:
    # Draft mode — create a promotion record
    python scripts/promote_lessons.py \\
        --from-inbox agent-improvement/inbox/2026-03-18_foo.yaml \\
        --output agent-improvement/promotions/PRO-2026-03-18-001.yaml

    # Draft from project lessons
    python scripts/promote_lessons.py \\
        --from-project 2026_NLD_journal_trend \\
        --output agent-improvement/promotions/PRO-2026-03-18-002.yaml

    # Apply mode (Phase 2 — safe categories only)
    python scripts/promote_lessons.py \\
        --from-inbox agent-improvement/inbox/2026-03-18_foo.yaml \\
        --output agent-improvement/promotions/PRO-2026-03-18-001.yaml \\
        --apply

Output:
    agent-improvement/promotions/<promotion_id>.yaml
    Optional: agent-improvement/reports/<promotion_id>.md (with --report)
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
INBOX_DIR = REPO_ROOT / "agent-improvement" / "inbox"
PROMOTIONS_DIR = REPO_ROOT / "agent-improvement" / "promotions"
PROJECTS_DIR = REPO_ROOT / "projects"
REPORTS_DIR = REPO_ROOT / "agent-improvement" / "reports"

# File categories that are safe to auto-apply in Phase 2
SAFE_APPLY_CATEGORIES = {
    "profile",      # agent-core/profiles/
    "reference",    # agent-core/references/
    "runbook",      # agent-core/runbooks/
    "recipe",       # agent-core/recipes/
    "manifest",     # agent-core/catalog/tables/
}

# Target routing based on scope and category
TARGET_ROUTING = {
    ("project", None): "projects/<id>/context/",
    ("client", None): "agent-core/profiles/clients/<client>.md",
    ("user", None): "agent-core/profiles/users/<user>.md",
    ("global", "schema"): "agent-core/catalog/tables/ + agent-core/references/",
    ("global", "query"): "agent-core/catalog/tables/ or agent-core/recipes/",
    ("global", "workflow"): "agent-core/recipes/",
    ("global", "output_style"): "agent-core/rules/output-contract.md",
    ("global", "tooling"): "agent-core/tool-contract/",
    ("global", "bug"): "agent-core/evals/tasks/",
    ("global", "naming"): "agent-core/rules/core-rules.md",
}


def load_lessons_from_inbox(inbox_path: Path) -> list[dict]:
    """Load lessons from an inbox YAML file."""
    if not inbox_path.exists():
        print(f"ERROR: Inbox file not found: {inbox_path}", file=sys.stderr)
        sys.exit(1)

    data = yaml.safe_load(inbox_path.read_text())
    if not data or not isinstance(data, dict):
        print(f"ERROR: Invalid inbox file: {inbox_path}", file=sys.stderr)
        sys.exit(1)

    lessons = data.get("lessons", [])
    for lesson in lessons:
        lesson["_source_file"] = str(inbox_path.relative_to(REPO_ROOT))
        lesson["_source_project"] = data.get("source_project", "unknown")
    return lessons


def load_lessons_from_project(project_id: str) -> list[dict]:
    """Load all lesson files from a project's context/lessons/ directory."""
    project_dir = PROJECTS_DIR / project_id / "context" / "lessons"
    if not project_dir.exists():
        print(f"ERROR: No lessons directory for project: {project_id}", file=sys.stderr)
        sys.exit(1)

    all_lessons = []
    for f in sorted(project_dir.glob("*.yaml")):
        data = yaml.safe_load(f.read_text())
        if not data:
            continue
        # Handle both list and dict formats
        if isinstance(data, list):
            lessons = data
        elif isinstance(data, dict):
            lessons = data.get("lessons", [data])
        else:
            continue
        for lesson in lessons:
            lesson["_source_file"] = str(f.relative_to(REPO_ROOT))
            lesson["_source_project"] = project_id
            all_lessons.append(lesson)
    return all_lessons


def suggest_targets(lesson: dict) -> list[str]:
    """Suggest canonical target files for a lesson based on scope and category."""
    scope = lesson.get("scope", "project")
    category = lesson.get("category", "")
    suggested = lesson.get("suggested_action", {})

    # If the lesson already has a suggested target, use it
    if isinstance(suggested, dict) and suggested.get("target"):
        return [suggested["target"]]

    # Route by scope first
    if scope == "project":
        project = lesson.get("_source_project", "unknown")
        return [f"projects/{project}/context/lessons/"]

    if scope == "client":
        return ["agent-core/profiles/clients/<client>.md"]

    if scope == "user":
        return ["agent-core/profiles/users/<user>.md"]

    # Global scope — route by category
    category_targets = {
        "schema": [
            "agent-core/catalog/tables/<table>.yaml",
            "agent-core/references/<table>.md",
        ],
        "query": ["agent-core/catalog/tables/<table>.yaml", "agent-core/recipes/"],
        "workflow": ["agent-core/recipes/"],
        "output_style": ["agent-core/rules/output-contract.md"],
        "tooling": ["agent-core/tool-contract/"],
        "bug": ["agent-core/evals/tasks/"],
        "dashboard": ["agent-core/recipes/"],
        "naming": ["agent-core/rules/core-rules.md"],
        "sharing": ["agent-core/rules/output-contract.md"],
        "profile": ["agent-core/profiles/"],
    }

    return category_targets.get(category, ["agent-core/"])


def classify_apply_safety(targets: list[str]) -> str:
    """Determine if a promotion's targets are safe for auto-apply."""
    for target in targets:
        if "core-rules.md" in target:
            return "requires_review"
        if "rules/" in target and "output-contract" not in target:
            return "requires_review"
    return "safe"


def build_promotion_record(
    lessons: list[dict],
    promotion_id: str,
    action_type: str = "draft",
) -> dict:
    """Build a promotion record from a set of lessons."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Collect all source lesson IDs
    source_ids = []
    for lesson in lessons:
        lid = lesson.get("id", lesson.get("_source_file", "unknown"))
        source_ids.append(lid)

    # Determine overall scope (widest scope wins)
    scope_priority = {"project": 0, "client": 1, "user": 2, "global": 3}
    widest_scope = "project"
    for lesson in lessons:
        s = lesson.get("scope", "project")
        if scope_priority.get(s, 0) > scope_priority.get(widest_scope, 0):
            widest_scope = s

    # Collect all suggested targets
    all_targets = []
    for lesson in lessons:
        targets = suggest_targets(lesson)
        all_targets.extend(targets)
    # Deduplicate while preserving order
    seen = set()
    unique_targets = []
    for t in all_targets:
        if t not in seen:
            seen.add(t)
            unique_targets.append(t)

    # Build summaries
    summaries = [
        lesson.get("summary", "no summary") for lesson in lessons
    ]
    change_summary = "; ".join(s.strip() for s in summaries[:5])
    if len(summaries) > 5:
        change_summary += f" (and {len(summaries) - 5} more)"

    safety = classify_apply_safety(unique_targets)

    record = {
        "promotion_id": promotion_id,
        "created_at": now,
        "source_lessons": source_ids,
        "scope": widest_scope,
        "target_files": unique_targets,
        "action_type": action_type,
        "status": "drafted",
        "change_summary": change_summary,
        "apply_safety": safety,
        "validations_run": [],
        "evals_run": [],
        "adapters_rebuilt": False,
        "reviewer": None,
        "notes": None,
    }

    return record


def generate_report(record: dict, lessons: list[dict]) -> str:
    """Generate a markdown report for a promotion record."""
    lines = [
        f"# Promotion Report: {record['promotion_id']}",
        "",
        f"**Created:** {record['created_at']}",
        f"**Scope:** {record['scope']}",
        f"**Action:** {record['action_type']}",
        f"**Status:** {record['status']}",
        f"**Apply safety:** {record.get('apply_safety', '?')}",
        "",
        "## Source lessons",
        "",
    ]
    for lesson in lessons:
        lid = lesson.get("id", "?")
        summary = lesson.get("summary", "no summary").strip()
        scope = lesson.get("scope", "?")
        confidence = lesson.get("confidence", "?")
        impact = lesson.get("impact", "?")
        lines.append(f"- **{lid}** [{scope}/{confidence}/{impact}]: {summary}")
    lines.append("")

    lines.append("## Proposed targets")
    lines.append("")
    for target in record["target_files"]:
        lines.append(f"- `{target}`")
    lines.append("")

    lines.append("## Change summary")
    lines.append("")
    lines.append(record.get("change_summary", ""))
    lines.append("")

    if record.get("apply_safety") == "requires_review":
        lines.append("## ⚠ Requires manual review")
        lines.append("")
        lines.append(
            "One or more target files (`core-rules.md` or broad rule files) "
            "require human review before applying."
        )
        lines.append("")

    lines.append("## Next steps")
    lines.append("")
    if record["action_type"] == "draft":
        lines.append("1. Review the proposed targets and change summary.")
        lines.append("2. Edit target files manually or re-run with `--apply`.")
        lines.append("3. Run: `python scripts/validate_agent_core.py`")
        lines.append("4. Run: `python scripts/build_agent_adapters.py`")
        lines.append("5. Run: `python scripts/validate_platform_outputs.py`")
        lines.append("6. Run relevant evals if behaviour changed.")
    else:
        lines.append("1. Verify the applied changes are correct.")
        lines.append("2. Run evals if behaviour changed.")
        lines.append("3. Commit all changes together.")
    lines.append("")

    return "\n".join(lines)


def generate_promotion_id() -> str:
    """Generate a promotion ID like PRO-2026-03-18-001."""
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    # Find existing promotions for today to determine sequence
    PROMOTIONS_DIR.mkdir(parents=True, exist_ok=True)
    existing = list(PROMOTIONS_DIR.glob(f"PRO-{date_str}-*.yaml"))
    seq = len(existing) + 1
    return f"PRO-{date_str}-{seq:03d}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Draft or apply promotions from inbox to canonical files"
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--from-inbox",
        type=str,
        help="Path to an inbox YAML file",
    )
    source.add_argument(
        "--from-project",
        type=str,
        help="Project ID to read lessons from",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path for the promotion record (default: auto-generated)",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Also generate a markdown report",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply safe changes (Phase 2 — profiles, references, runbooks, recipes, manifests)",
    )
    args = parser.parse_args()

    # Load lessons
    if args.from_inbox:
        inbox_path = Path(args.from_inbox)
        if not inbox_path.is_absolute():
            inbox_path = REPO_ROOT / inbox_path
        lessons = load_lessons_from_inbox(inbox_path)
    else:
        lessons = load_lessons_from_project(args.from_project)

    if not lessons:
        print("No lessons found — nothing to promote.")
        return 0

    print(f"Loaded {len(lessons)} lesson(s).")

    # Generate promotion ID and record
    promotion_id = generate_promotion_id()
    action_type = "apply" if args.apply else "draft"
    record = build_promotion_record(lessons, promotion_id, action_type)

    # Determine output path
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = REPO_ROOT / output_path
    else:
        PROMOTIONS_DIR.mkdir(parents=True, exist_ok=True)
        output_path = PROMOTIONS_DIR / f"{promotion_id}.yaml"

    # Write promotion record
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        yaml.dump(record, f, default_flow_style=False, sort_keys=False)

    print(f"Wrote promotion record: {output_path.relative_to(REPO_ROOT)}")

    # Generate report if requested
    if args.report:
        report_text = generate_report(record, lessons)
        report_path = REPORTS_DIR / f"{promotion_id}.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report_text)
        print(f"Wrote report: {report_path.relative_to(REPO_ROOT)}")

    # In apply mode, warn about unsafe targets
    if args.apply:
        if record.get("apply_safety") == "requires_review":
            print(
                "\n⚠  Some targets require manual review (core-rules.md or broad "
                "rule files). These were NOT auto-applied."
            )
            record["status"] = "drafted"
            record["notes"] = "Auto-apply skipped for unsafe targets."
            with open(output_path, "w") as f:
                yaml.dump(record, f, default_flow_style=False, sort_keys=False)
        else:
            print(
                "\nApply mode: changes would be applied to safe targets. "
                "(Phase 2 — not yet implemented. Record saved as draft.)"
            )

    # Print summary
    print(f"\nPromotion {promotion_id}:")
    print(f"  Scope: {record['scope']}")
    print(f"  Status: {record['status']}")
    print(f"  Targets: {len(record['target_files'])} file(s)")
    print(f"  Safety: {record.get('apply_safety', 'unknown')}")

    return 0


if __name__ == "__main__":
    try:
        import yaml  # noqa: F811
    except ImportError:
        print("ERROR: PyYAML not installed. Run: pip install pyyaml", file=sys.stderr)
        sys.exit(1)
    sys.exit(main())

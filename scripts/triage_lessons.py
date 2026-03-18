#!/usr/bin/env python3
"""
triage_lessons.py
=================
Scan agent-improvement/inbox/, group duplicates by category/scope, and
produce a triage report. Optionally generate issue drafts.

Usage:
    python scripts/triage_lessons.py [--output-report agent-improvement/reports/triage.md]
    python scripts/triage_lessons.py --generate-issues  # print issue drafts to stdout
"""

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
INBOX_DIR = REPO_ROOT / "agent-improvement" / "inbox"
REPORTS_DIR = REPO_ROOT / "agent-improvement" / "reports"


def load_inbox() -> list[dict]:
    """Load all YAML files from the inbox and return flattened lesson list."""
    all_lessons = []
    if not INBOX_DIR.exists():
        return all_lessons

    for f in sorted(INBOX_DIR.glob("*.yaml")):
        data = yaml.safe_load(f.read_text())
        if not data or not isinstance(data, dict):
            continue
        lessons = data.get("lessons", [])
        for lesson in lessons:
            lesson["_source_file"] = str(f.relative_to(REPO_ROOT))
            lesson["_source_project"] = data.get("source_project", "unknown")
            lesson["_source_session"] = data.get("source_session", "unknown")
            all_lessons.append(lesson)
    return all_lessons


def group_lessons(lessons: list[dict]) -> dict[str, list[dict]]:
    """Group by (scope, category) tuple."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for l in lessons:
        key = f"{l.get('scope', 'unknown')}/{l.get('category', 'unknown')}"
        groups[key].append(l)
    return dict(sorted(groups.items()))


def find_duplicates(lessons: list[dict]) -> list[tuple[dict, dict]]:
    """Find potential duplicates by similar summary text."""
    dupes = []
    for i, a in enumerate(lessons):
        for b in lessons[i + 1 :]:
            sa = (a.get("summary") or "").lower().strip()
            sb = (b.get("summary") or "").lower().strip()
            if sa and sb and (sa in sb or sb in sa):
                dupes.append((a, b))
    return dupes


def generate_report(
    lessons: list[dict],
    groups: dict[str, list[dict]],
    duplicates: list[tuple[dict, dict]],
) -> str:
    """Produce a markdown triage report."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"# Triage Report — {now}",
        "",
        f"**Total inbox lessons:** {len(lessons)}",
        f"**Groups:** {len(groups)}",
        f"**Potential duplicates:** {len(duplicates)}",
        "",
    ]

    # Summary by scope
    scope_counts: dict[str, int] = defaultdict(int)
    for l in lessons:
        scope_counts[l.get("scope", "unknown")] += 1
    lines.append("## By scope")
    lines.append("")
    for scope, count in sorted(scope_counts.items()):
        lines.append(f"- **{scope}**: {count}")
    lines.append("")

    # Grouped lessons
    lines.append("## Grouped lessons")
    lines.append("")
    for key, group in groups.items():
        lines.append(f"### {key} ({len(group)} items)")
        lines.append("")
        for l in group:
            summary = l.get("summary", "no summary").strip()
            confidence = l.get("confidence", "?")
            impact = l.get("impact", "?")
            project = l.get("_source_project", "?")
            lines.append(
                f"- [{confidence}/{impact}] {summary} _(from {project})_"
            )
        lines.append("")

    # Duplicates
    if duplicates:
        lines.append("## Potential duplicates")
        lines.append("")
        for a, b in duplicates:
            lines.append(
                f"- **A:** {a.get('summary', '?')[:80]} "
                f"_(from {a.get('_source_project', '?')})_"
            )
            lines.append(
                f"  **B:** {b.get('summary', '?')[:80]} "
                f"_(from {b.get('_source_project', '?')})_"
            )
            lines.append("")

    # Promotion candidates
    promotable = [
        l for l in lessons
        if l.get("scope") == "global"
        and l.get("confidence") in ("high", "medium")
        and l.get("impact") in ("high", "medium")
    ]
    if promotable:
        lines.append("## Promotion candidates")
        lines.append("")
        for l in promotable:
            target = (l.get("suggested_action") or {}).get("target", "unspecified")
            lines.append(
                f"- {l.get('summary', '?')[:80]} → **{target}**"
            )
        lines.append("")

    return "\n".join(lines)


def generate_issue_drafts(lessons: list[dict]) -> str:
    """Generate GitHub issue body drafts for promotable lessons."""
    promotable = [
        l for l in lessons
        if l.get("scope") == "global"
        and l.get("confidence") in ("high", "medium")
        and l.get("impact") in ("high", "medium")
    ]
    if not promotable:
        return "No lessons meet promotion criteria."

    drafts = []
    for l in promotable:
        target = (l.get("suggested_action") or {}).get("target", "TBD")
        change_type = (l.get("suggested_action") or {}).get("change_type", "TBD")
        drafts.append(
            f"## Issue: {l.get('summary', 'Untitled')[:80]}\n"
            f"\n"
            f"**Category:** {l.get('category', '?')}\n"
            f"**Scope:** {l.get('scope', '?')}\n"
            f"**Memory type:** {l.get('memory_type', '?')}\n"
            f"**Confidence:** {l.get('confidence', '?')}\n"
            f"**Impact:** {l.get('impact', '?')}\n"
            f"**Recurrence:** {l.get('recurrence_hint', '?')}\n"
            f"\n"
            f"**Target file:** `{target}`\n"
            f"**Change type:** {change_type}\n"
            f"\n"
            f"**Evidence:**\n"
            f"- Source project: {l.get('_source_project', '?')}\n"
            f"- Source file: {l.get('_source_file', '?')}\n"
            f"\n---\n"
        )
    return "\n".join(drafts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Triage inbox lessons")
    parser.add_argument(
        "--output-report",
        default=None,
        help="Write report to this path (default: print to stdout)",
    )
    parser.add_argument(
        "--generate-issues",
        action="store_true",
        help="Print GitHub issue drafts instead of the triage report",
    )
    args = parser.parse_args()

    lessons = load_inbox()
    if not lessons:
        print("Inbox is empty — nothing to triage.")
        return 0

    if args.generate_issues:
        print(generate_issue_drafts(lessons))
        return 0

    groups = group_lessons(lessons)
    duplicates = find_duplicates(lessons)
    report = generate_report(lessons, groups, duplicates)

    if args.output_report:
        out = Path(args.output_report)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report)
        print(f"Report written to {out}")
    else:
        print(report)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

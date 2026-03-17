#!/usr/bin/env python3
"""
build_agent_adapters.py
=======================
Generates all platform-specific agent adapter files from canonical agent-core/
content and Jinja2 templates in platform/.

Usage:
    python scripts/build_agent_adapters.py [--check]

Flags:
    --check   Dry-run: exit 1 if any generated file differs from what's on disk
              (use in CI to detect drift).

Generated files:
    .github/copilot-instructions.md
    .github/agents/analyst.agent.md
    CLAUDE.md
    .claude/skills/ads-analyst/SKILL.md
    .claude/agents/analyst.md
    .claude/agents/schema-explorer.md
    .claude/agents/reviewer.md
    AGENTS.md
    .agents/skills/ads-analyst/SKILL.md
    .codex/agents/analyst.toml
    .codex/agents/schema-explorer.toml
    .codex/agents/reviewer.toml
"""

import argparse
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Dependencies — standard library only, no jinja2 needed for simple templates
# We use a minimal {{var}} substitution to avoid adding a build dependency.
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Workspace configuration — edit this section when environment details change
# ---------------------------------------------------------------------------

CONFIG = {
    "workspace_name": "ads-dbr-agentic",
    "databricks_workspace": "https://elsevier-dev.cloud.databricks.com",
    "databricks_cli_version": "v0.282.0",
    "cluster_name": "rads-private-unity",
    "cluster_id": "0107-154653-j5wd510m",
    "spark_library_path": "/Workspace/rads/library/",
    "ani_stamp": "20260301",
    "year": "2026",
}

# ---------------------------------------------------------------------------
# Role specs — used to generate subagent files
# ---------------------------------------------------------------------------

ROLES = [
    {
        "role_name": "analyst",
        "role_file": "analyst-orchestrator.md",
        "role_title": "Analyst Orchestrator",
        "role_description": (
            "Orchestrates research analyses: plans work, delegates to specialist "
            "roles, synthesises answers for the user."
        ),
        "agent_name": "analyst",
        "agent_description": (
            "Full analytics orchestrator for Scopus/bibliometric research. "
            "Plans, delegates, and synthesises results."
        ),
    },
    {
        "role_name": "schema-explorer",
        "role_file": "schema-explorer.md",
        "role_title": "Schema Explorer",
        "role_description": (
            "Looks up table schemas, join keys, type conversions, and coverage "
            "caveats. Returns a structured query contract."
        ),
        "agent_name": "schema-explorer",
        "agent_description": (
            "Read-only schema and catalog explorer. Returns query contracts "
            "with exact column names, join keys, and coverage warnings."
        ),
    },
    {
        "role_name": "reviewer",
        "role_file": "reviewer.md",
        "role_title": "Reviewer",
        "role_description": (
            "Quality-checks notebooks and results: verifies nopp(), join "
            "directions, path conventions, and output correctness."
        ),
        "agent_name": "reviewer",
        "agent_description": (
            "Post-analysis quality checker. Verifies nopp(), LEFT JOIN usage, "
            "output paths, and coverage warnings."
        ),
    },
]

# ---------------------------------------------------------------------------
# Template rendering — minimal {{var}} substitution
# ---------------------------------------------------------------------------


def render(template_path: Path, context: dict) -> str:
    """Render a template file by substituting {{key}} placeholders."""
    text = template_path.read_text()
    # Strip Jinja2 comment blocks (lines starting with {# ... #})
    import re

    text = re.sub(r"\{#.*?#\}\n?", "", text, flags=re.DOTALL)
    for key, value in context.items():
        text = text.replace("{{ " + key + " }}", str(value))
        text = text.replace("{{" + key + "}}", str(value))
    return text


def write_generated(dest: Path, content: str, check_mode: bool) -> bool:
    """Write content to dest. In check mode, return True if file differs."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if check_mode:
        if not dest.exists():
            print(f"  MISSING  {dest.relative_to(REPO_ROOT)}")
            return True
        existing = dest.read_text()
        if existing != content:
            print(f"  DRIFT    {dest.relative_to(REPO_ROOT)}")
            return True
        print(f"  OK       {dest.relative_to(REPO_ROOT)}")
        return False
    else:
        dest.write_text(content)
        print(f"  WROTE    {dest.relative_to(REPO_ROOT)}")
        return False


# ---------------------------------------------------------------------------
# Build targets
# ---------------------------------------------------------------------------


def build_all(check_mode: bool = False) -> int:
    drift_count = 0
    platform = REPO_ROOT / "platform"

    # 1. Copilot: copilot-instructions.md
    tmpl = platform / "copilot/templates/copilot-instructions.md.j2"
    content = render(tmpl, CONFIG)
    drift_count += write_generated(
        REPO_ROOT / ".github/copilot-instructions.md", content, check_mode
    )

    # 2. Copilot: analyst.agent.md
    tmpl = platform / "copilot/templates/analyst.agent.md.j2"
    content = render(tmpl, CONFIG)
    drift_count += write_generated(
        REPO_ROOT / ".github/agents/analyst.agent.md", content, check_mode
    )

    # 3. Claude: CLAUDE.md
    tmpl = platform / "claude/templates/CLAUDE.md.j2"
    content = render(tmpl, CONFIG)
    drift_count += write_generated(REPO_ROOT / "CLAUDE.md", content, check_mode)

    # 4. Claude: skill SKILL.md
    tmpl = platform / "claude/templates/skill.SKILL.md.j2"
    content = render(tmpl, CONFIG)
    drift_count += write_generated(
        REPO_ROOT / ".claude/skills/ads-analyst/SKILL.md", content, check_mode
    )

    # 5. Claude subagents
    tmpl = platform / "claude/templates/subagent.md.j2"
    for role in ROLES:
        context = {**CONFIG, **role}
        content = render(tmpl, context)
        drift_count += write_generated(
            REPO_ROOT / f".claude/agents/{role['role_name']}.md", content, check_mode
        )

    # 6. Codex: AGENTS.md
    tmpl = platform / "codex/templates/AGENTS.md.j2"
    content = render(tmpl, CONFIG)
    drift_count += write_generated(REPO_ROOT / "AGENTS.md", content, check_mode)

    # 7. Codex: skill SKILL.md
    tmpl = platform / "codex/templates/skill.SKILL.md.j2"
    content = render(tmpl, CONFIG)
    drift_count += write_generated(
        REPO_ROOT / ".agents/skills/ads-analyst/SKILL.md", content, check_mode
    )

    # 8. Codex: custom agent toml files
    tmpl = platform / "codex/templates/custom-agent.toml.j2"
    for role in ROLES:
        context = {**CONFIG, **role}
        content = render(tmpl, context)
        drift_count += write_generated(
            REPO_ROOT / f".codex/agents/{role['agent_name']}.toml",
            content,
            check_mode,
        )

    return drift_count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build agent platform adapters")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check mode: exit 1 if any file differs (use in CI)",
    )
    args = parser.parse_args()

    print(f"{'Checking' if args.check else 'Building'} platform adapter files...\n")
    drift = build_all(check_mode=args.check)

    if args.check and drift > 0:
        print(f"\n{drift} file(s) have drift. Run: python scripts/build_agent_adapters.py")
        sys.exit(1)
    elif not args.check:
        print(f"\nDone. Generated all platform adapter files.")
    sys.exit(0)

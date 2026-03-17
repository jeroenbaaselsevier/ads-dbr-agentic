#!/usr/bin/env python3
"""
scripts/validate_platform_outputs.py
=========================================
Verify that all generated platform adapter files exist and carry the
AUTO-GENERATED banner (i.e. they haven't been hand-edited).

Usage:
    python scripts/validate_platform_outputs.py

Exit codes:
    0 — all files present and unmodified
    1 — missing files or missing banner
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

GENERATED_FILES = [
    # (relative_path, expected_banner_substring)
    (".github/copilot-instructions.md", "AUTO-GENERATED"),
    (".github/agents/analyst.agent.md", "AUTO-GENERATED"),
    ("CLAUDE.md", "AUTO-GENERATED"),
    ("AGENTS.md", "AUTO-GENERATED"),
    (".claude/skills/ads-analyst/SKILL.md", "AUTO-GENERATED"),
    (".claude/agents/analyst.md", "AUTO-GENERATED"),
    (".claude/agents/schema-explorer.md", "AUTO-GENERATED"),
    (".claude/agents/reviewer.md", "AUTO-GENERATED"),
    (".agents/skills/ads-analyst/SKILL.md", "AUTO-GENERATED"),
    (".codex/agents/analyst.toml", "AUTO-GENERATED"),
    (".codex/agents/schema-explorer.toml", "AUTO-GENERATED"),
    (".codex/agents/reviewer.toml", "AUTO-GENERATED"),
]


def main() -> int:
    errors = []

    for rel_path, banner in GENERATED_FILES:
        path = REPO_ROOT / rel_path
        if not path.exists():
            errors.append(f"MISSING  {rel_path}")
            continue
        content = path.read_text()
        if banner not in content:
            errors.append(f"NO BANNER (may be hand-edited)  {rel_path}")

    if errors:
        print(f"Platform output validation FAILED ({len(errors)} issue(s)):\n")
        for e in errors:
            print(f"  ✗ {e}")
        print(
            "\nRun `python scripts/build_agent_adapters.py` to regenerate missing files."
        )
        return 1

    print(f"Platform output validation PASSED ({len(GENERATED_FILES)} files OK)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

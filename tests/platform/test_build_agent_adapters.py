"""Tests for scripts/build_agent_adapters.py."""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
BUILD_SCRIPT = REPO_ROOT / "scripts" / "build_agent_adapters.py"


def test_build_runs_without_error():
    """build_agent_adapters.py should complete successfully."""
    result = subprocess.run(
        [sys.executable, str(BUILD_SCRIPT)],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    assert result.returncode == 0, f"Build failed:\n{result.stdout}\n{result.stderr}"


def test_check_mode_passes_after_build():
    """After a build, --check should report no drift."""
    # Build first
    subprocess.run(
        [sys.executable, str(BUILD_SCRIPT)],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    # Check
    result = subprocess.run(
        [sys.executable, str(BUILD_SCRIPT), "--check"],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    assert result.returncode == 0, f"Drift detected:\n{result.stdout}"


def test_project_resources_wrappers_generated():
    """Project-resources skill/agent files should exist after build."""
    subprocess.run(
        [sys.executable, str(BUILD_SCRIPT)],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )

    expected = [
        ".github/agents/project-resources.agent.md",
        ".claude/skills/project-resources/SKILL.md",
        ".agents/skills/project-resources/SKILL.md",
    ]
    for rel in expected:
        path = REPO_ROOT / rel
        assert path.exists(), f"Missing generated file: {rel}"
        content = path.read_text()
        assert "AUTO-GENERATED" in content, f"Missing banner in: {rel}"

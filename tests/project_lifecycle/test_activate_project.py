"""Tests for scripts/activate_project.py."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ACTIVATE_SCRIPT = REPO_ROOT / "scripts" / "activate_project.py"
INIT_SCRIPT = REPO_ROOT / "scripts" / "init_project.py"
TEST_PROJECTS = REPO_ROOT / "projects"
AGENT_STATE = REPO_ROOT / ".agent-state"


@pytest.fixture(autouse=True)
def setup_and_cleanup():
    """Create a test project, then clean up after test."""
    subprocess.run(
        [sys.executable, str(INIT_SCRIPT),
         "--year", "2099", "--iso3", "TST", "--short-name", "test_activate",
         "--display-name", "Test activate", "--no-activate"],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    yield
    test_dir = TEST_PROJECTS / "2099_TST_test_activate"
    if test_dir.exists():
        shutil.rmtree(test_dir)
    state_file = AGENT_STATE / "active_project.json"
    if state_file.exists():
        state_file.unlink()


def _run(args: list[str]) -> tuple[int, str, str]:
    result = subprocess.run(
        [sys.executable, str(ACTIVATE_SCRIPT)] + args,
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    return result.returncode, result.stdout, result.stderr


def test_activate_existing_project():
    rc, stdout, _ = _run(["--project-id", "2099_TST_test_activate"])
    assert rc == 0
    data = json.loads(stdout)
    assert data["project_id"] == "2099_TST_test_activate"
    assert data["active_state_path"] == ".agent-state/active_project.json"

    state_file = AGENT_STATE / "active_project.json"
    assert state_file.exists()
    state = json.loads(state_file.read_text())
    assert state["project_id"] == "2099_TST_test_activate"


def test_activate_with_session_id():
    rc, stdout, _ = _run(["--project-id", "2099_TST_test_activate", "--session-id", "SES123"])
    assert rc == 0
    data = json.loads(stdout)
    assert data["session_id"] == "SES123"


def test_activate_nonexistent_project():
    rc, _, stderr = _run(["--project-id", "2099_TST_nonexistent"])
    assert rc == 1
    assert "not found" in stderr.lower()

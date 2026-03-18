"""Tests for scripts/closeout_project.py."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CLOSEOUT_SCRIPT = REPO_ROOT / "scripts" / "closeout_project.py"
INIT_SCRIPT = REPO_ROOT / "scripts" / "init_project.py"
TEST_PROJECTS = REPO_ROOT / "projects"
AGENT_STATE = REPO_ROOT / ".agent-state"


@pytest.fixture(autouse=True)
def setup_and_cleanup():
    """Create a test project, then clean up after."""
    subprocess.run(
        [sys.executable, str(INIT_SCRIPT),
         "--year", "2099", "--iso3", "TST", "--short-name", "test_closeout",
         "--display-name", "Test closeout"],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    yield
    test_dir = TEST_PROJECTS / "2099_TST_test_closeout"
    if test_dir.exists():
        shutil.rmtree(test_dir)
    state_file = AGENT_STATE / "active_project.json"
    if state_file.exists():
        state_file.unlink()


def _run(args: list[str]) -> tuple[int, str, str]:
    result = subprocess.run(
        [sys.executable, str(CLOSEOUT_SCRIPT)] + args,
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    return result.returncode, result.stdout, result.stderr


def test_closeout_with_session_status():
    rc, stdout, _ = _run([
        "--project-id", "2099_TST_test_closeout",
        "--session-id", "SES001",
        "--session-status", "completed",
        "--summary", "Test completed",
    ])
    assert rc == 0
    data = json.loads(stdout)
    assert "session_summary" in data

    # Session notes written
    proj = TEST_PROJECTS / "2099_TST_test_closeout"
    notes = proj / "context" / "session-notes" / "SES001.md"
    assert notes.exists()
    assert "completed" in notes.read_text()


def test_closeout_backward_compat_status():
    """--status still works as backward-compatible alias."""
    rc, stdout, _ = _run([
        "--project-id", "2099_TST_test_closeout",
        "--session-id", "SES002",
        "--status", "paused",
    ])
    assert rc == 0


def test_closeout_clears_active_state():
    """Active project state should be cleared on closeout."""
    # Verify state exists after init
    state_file = AGENT_STATE / "active_project.json"
    assert state_file.exists()

    _run([
        "--project-id", "2099_TST_test_closeout",
        "--session-id", "SES003",
        "--session-status", "completed",
    ])
    assert not state_file.exists()


def test_closeout_with_project_status():
    _run([
        "--project-id", "2099_TST_test_closeout",
        "--session-id", "SES004",
        "--session-status", "completed",
        "--project-status", "completed",
    ])
    import yaml
    manifest = TEST_PROJECTS / "2099_TST_test_closeout" / "project.yaml"
    data = yaml.safe_load(manifest.read_text())
    assert data["status"] == "completed"


def test_session_completed_does_not_auto_complete_project():
    """Session completed should NOT auto-set project to completed."""
    _run([
        "--project-id", "2099_TST_test_closeout",
        "--session-id", "SES005",
        "--session-status", "completed",
    ])
    import yaml
    manifest = TEST_PROJECTS / "2099_TST_test_closeout" / "project.yaml"
    data = yaml.safe_load(manifest.read_text())
    assert data["status"] == "active"

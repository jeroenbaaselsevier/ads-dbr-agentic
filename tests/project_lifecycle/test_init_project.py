"""Tests for scripts/init_project.py."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT = REPO_ROOT / "scripts" / "init_project.py"
TEST_PROJECTS = REPO_ROOT / "projects"
AGENT_STATE = REPO_ROOT / ".agent-state"


@pytest.fixture(autouse=True)
def cleanup_test_project():
    """Remove the test project dir and agent state after each test."""
    yield
    test_dir = TEST_PROJECTS / "2099_TST_test_init"
    if test_dir.exists():
        shutil.rmtree(test_dir)
    state_file = AGENT_STATE / "active_project.json"
    if state_file.exists():
        state_file.unlink()


def _run(args: list[str]) -> tuple[int, str, str]:
    result = subprocess.run(
        [sys.executable, str(SCRIPT)] + args,
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    return result.returncode, result.stdout, result.stderr


def test_creates_new_project():
    rc, stdout, _ = _run([
        "--year", "2099", "--iso3", "TST", "--short-name", "test_init",
        "--display-name", "Test init project",
    ])
    assert rc == 0
    data = json.loads(stdout)
    assert data["created"] is True
    assert data["project_id"] == "2099_TST_test_init"
    assert "session_id" in data
    assert "defaults" in data
    assert data["defaults"]["spark_notebook"].endswith("test_init.py")

    # Verify scaffold
    proj = TEST_PROJECTS / "2099_TST_test_init"
    assert (proj / "project.yaml").exists()
    assert (proj / "README.md").exists()
    assert (proj / "notebooks" / "spark").is_dir()
    assert (proj / "context" / "session-notes").is_dir()


def test_active_state_written():
    _run([
        "--year", "2099", "--iso3", "TST", "--short-name", "test_init",
        "--display-name", "Test init project",
    ])
    state_file = AGENT_STATE / "active_project.json"
    assert state_file.exists()
    state = json.loads(state_file.read_text())
    assert state["project_id"] == "2099_TST_test_init"
    assert "session_id" in state
    assert "databricks_workspace_root" in state


def test_no_activate_flag():
    _run([
        "--year", "2099", "--iso3", "TST", "--short-name", "test_init",
        "--display-name", "Test", "--no-activate",
    ])
    state_file = AGENT_STATE / "active_project.json"
    assert not state_file.exists()


def test_reuse_existing_project():
    # Create first
    _run([
        "--year", "2099", "--iso3", "TST", "--short-name", "test_init",
        "--display-name", "Test", "--no-activate",
    ])
    # Reuse
    rc, stdout, _ = _run([
        "--year", "2099", "--iso3", "TST", "--short-name", "test_init",
        "--display-name", "Test",
    ])
    assert rc == 0
    data = json.loads(stdout)
    assert data["created"] is False
    assert data["project_id"] == "2099_TST_test_init"


def test_validation_rejects_bad_iso3():
    rc, _, stderr = _run([
        "--year", "2099", "--iso3", "XX", "--short-name", "test_init",
        "--display-name", "Test",
    ])
    assert rc == 1
    assert "iso3" in stderr.lower()


def test_session_id_passthrough():
    rc, stdout, _ = _run([
        "--year", "2099", "--iso3", "TST", "--short-name", "test_init",
        "--display-name", "Test", "--session-id", "MYSESSION42",
    ])
    assert rc == 0
    data = json.loads(stdout)
    assert data["session_id"] == "MYSESSION42"

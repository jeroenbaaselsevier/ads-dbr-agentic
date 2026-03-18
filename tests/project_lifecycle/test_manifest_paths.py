"""Tests for project manifest path consistency."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
INIT_SCRIPT = REPO_ROOT / "scripts" / "init_project.py"
TEST_PROJECTS = REPO_ROOT / "projects"
AGENT_STATE = REPO_ROOT / ".agent-state"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    test_dir = TEST_PROJECTS / "2099_TST_test_paths"
    if test_dir.exists():
        shutil.rmtree(test_dir)
    state_file = AGENT_STATE / "active_project.json"
    if state_file.exists():
        state_file.unlink()


def test_manifest_paths_consistent():
    """All paths in project.yaml should follow the naming convention."""
    result = subprocess.run(
        [sys.executable, str(INIT_SCRIPT),
         "--year", "2099", "--iso3", "TST", "--short-name", "test_paths",
         "--display-name", "Path test", "--no-activate"],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    assert result.returncode == 0

    manifest = TEST_PROJECTS / "2099_TST_test_paths" / "project.yaml"
    data = yaml.safe_load(manifest.read_text())

    assert data["id"] == "2099_TST_test_paths"
    assert "paths" in data
    assert data["paths"]["local_root"] == "projects/2099_TST_test_paths"
    assert "s3://rads-projects/short_term/2099/2099_TST_test_paths" == data["paths"]["s3_root"]
    assert "/Workspace/rads/projects/2099_TST_test_paths" == data["paths"]["databricks_workspace_root"]

    # Folders
    assert "folders" in data
    assert data["folders"]["s3_exports"].endswith("/exports")
    assert data["folders"]["s3_logs"].endswith("/logs")
    assert data["folders"]["databricks_notebooks_root"].endswith("/notebooks")

    # Defaults
    assert "defaults" in data
    assert data["defaults"]["spark_notebook"].endswith("test_paths.py")
    assert "databricks_spark_notebook" in data["defaults"]


def test_json_output_matches_manifest():
    """JSON output from init should match the written manifest."""
    result = subprocess.run(
        [sys.executable, str(INIT_SCRIPT),
         "--year", "2099", "--iso3", "TST", "--short-name", "test_paths",
         "--display-name", "Path test", "--no-activate"],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    output = json.loads(result.stdout)
    manifest = yaml.safe_load(
        (TEST_PROJECTS / "2099_TST_test_paths" / "project.yaml").read_text()
    )

    assert output["local_root"] == manifest["paths"]["local_root"]
    assert output["s3_root"] == manifest["paths"]["s3_root"]
    assert output["databricks_workspace_root"] == manifest["paths"]["databricks_workspace_root"]

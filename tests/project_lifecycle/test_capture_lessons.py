"""Tests for scripts/capture_lessons.py."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CAPTURE_SCRIPT = REPO_ROOT / "scripts" / "capture_lessons.py"
INIT_SCRIPT = REPO_ROOT / "scripts" / "init_project.py"
TEST_PROJECTS = REPO_ROOT / "projects"


@pytest.fixture(autouse=True)
def setup_and_cleanup():
    subprocess.run(
        [sys.executable, str(INIT_SCRIPT),
         "--year", "2099", "--iso3", "TST", "--short-name", "test_lessons",
         "--display-name", "Test lessons", "--no-activate"],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    yield
    test_dir = TEST_PROJECTS / "2099_TST_test_lessons"
    if test_dir.exists():
        shutil.rmtree(test_dir)


def test_capture_from_json():
    lessons = json.dumps([{
        "scope": "project",
        "memory_type": "procedural",
        "category": "workflow",
        "summary": "Test lesson captured",
        "confidence": "high",
    }])
    result = subprocess.run(
        [sys.executable, str(CAPTURE_SCRIPT),
         "--project-id", "2099_TST_test_lessons",
         "--session-id", "SES001",
         "--from-json"],
        input=lessons,
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    assert result.returncode == 0
    data = json.loads(result.stdout)
    assert data["lessons_captured"] == 1

    lessons_file = Path(data["lessons_file"])
    assert (REPO_ROOT / lessons_file).exists()


def test_cross_project_lessons_create_intake():
    lessons = json.dumps([{
        "scope": "global",
        "memory_type": "semantic",
        "category": "schema",
        "summary": "Global lesson for intake",
        "confidence": "high",
    }])
    result = subprocess.run(
        [sys.executable, str(CAPTURE_SCRIPT),
         "--project-id", "2099_TST_test_lessons",
         "--session-id", "SES002",
         "--from-json"],
        input=lessons,
        capture_output=True, text=True, cwd=str(REPO_ROOT),
    )
    assert result.returncode == 0
    data = json.loads(result.stdout)
    assert data["intake_record"] is not None
    # Clean up intake file
    intake = REPO_ROOT / data["intake_record"]
    if intake.exists():
        intake.unlink()

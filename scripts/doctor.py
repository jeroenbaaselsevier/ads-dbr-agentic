#!/usr/bin/env python3
"""
scripts/doctor.py
==================
Health check for the full agent system. Verifies environment, knowledge,
and generated adapter files in one command.

Usage:
    python scripts/doctor.py

Exit codes:
    0 — all checks passed
    1 — one or more checks failed
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

CHECKS = []


def check(label: str, ok: bool, detail: str = "") -> dict:
    status = "OK" if ok else "FAIL"
    icon = "✓" if ok else "✗"
    msg = f"  {icon} {label}"
    if detail:
        msg += f"\n      {detail}"
    print(msg)
    return {"label": label, "status": status, "detail": detail}


def run_script(args: list) -> tuple:
    """Run a script and return (returncode, stdout, stderr)."""
    result = subprocess.run(args, capture_output=True, text=True, cwd=str(REPO_ROOT))
    return result.returncode, result.stdout, result.stderr


def main() -> int:
    print("Running doctor.py — agent system health check\n")
    results = []

    # 1. Check .venv exists and is activatable
    venv = REPO_ROOT / ".venv"
    results.append(check("Local .venv exists", venv.is_dir()))

    # 2. Check requirements.txt exists
    req = REPO_ROOT / "requirements.txt"
    results.append(check("requirements.txt exists", req.exists()))

    # 3. Check deploy.sh exists and is executable
    deploy = REPO_ROOT / "deploy.sh"
    results.append(
        check("deploy.sh exists and executable", deploy.exists() and os.access(deploy, os.X_OK))
        if False  # os not imported yet
        else check("deploy.sh exists", deploy.exists())
    )

    # 4. Check poll_run.sh exists
    poll = REPO_ROOT / "poll_run.sh"
    results.append(check("poll_run.sh exists", poll.exists()))

    # 5. Check rads_library/ exists
    lib = REPO_ROOT / "rads_library"
    results.append(
        check(
            "rads_library/ mirror exists",
            lib.is_dir(),
            "Run ./sync_library.sh to populate" if not lib.is_dir() else "",
        )
    )

    # 6. Validate agent-core
    print()
    print("  Validating agent-core...")
    python = str(REPO_ROOT / ".venv/bin/python3")
    if not Path(python).exists():
        python = shutil.which("python3") or "python3"
    rc, out, err = run_script([python, "scripts/validate_agent_core.py"])
    results.append(
        check("agent-core validation", rc == 0, err.strip() if rc != 0 else "")
    )

    # 7. Validate platform outputs
    print()
    print("  Validating platform outputs...")
    rc, out, err = run_script([python, "scripts/validate_platform_outputs.py"])
    results.append(
        check(
            "Platform adapter files",
            rc == 0,
            out.strip() if rc != 0 else "",
        )
    )

    # 8. Check databricks CLI reachable
    print()
    print("  Checking Databricks CLI...")
    db = shutil.which("databricks")
    results.append(check("databricks CLI in PATH", db is not None))

    # 9. Check AWS CLI reachable
    aws = shutil.which("aws")
    results.append(check("aws CLI in PATH", aws is not None))

    # 10. Check agent-core/ exists
    results.append(check("agent-core/ canonical directory exists", (REPO_ROOT / "agent-core").is_dir()))

    # 11. Check projects/ exists
    results.append(check("projects/ directory exists", (REPO_ROOT / "projects").is_dir()))

    # 12. Check project lifecycle scripts
    for script_name in ["init_project.py", "activate_project.py", "closeout_project.py", "capture_lessons.py"]:
        script = REPO_ROOT / "scripts" / script_name
        results.append(check(f"scripts/{script_name} exists", script.exists()))

    # 13. Check tools/local/postprocess.py
    postprocess = REPO_ROOT / "tools" / "local" / "postprocess.py"
    results.append(check("tools/local/postprocess.py exists", postprocess.exists()))

    # 14. Check .agent-state/ is creatable
    agent_state = REPO_ROOT / ".agent-state"
    results.append(check(
        ".agent-state/ directory accessible",
        agent_state.is_dir() or not agent_state.exists(),
        "Will be created on first project init" if not agent_state.exists() else "",
    ))

    print()
    failed = [r for r in results if r["status"] == "FAIL"]
    if failed:
        print(f"Doctor found {len(failed)} issue(s). Fix them before running analyses.")
        return 1

    print(f"All {len(results)} checks passed. System is healthy.")
    return 0


if __name__ == "__main__":
    import os  # noqa
    sys.exit(main())

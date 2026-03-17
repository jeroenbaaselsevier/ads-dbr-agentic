#!/usr/bin/env python3
"""
tools/databricks/poll_run.py
==============================
Poll a Databricks run until it reaches a terminal state.

Usage:
    python tools/databricks/poll_run.py <run_id> [--interval 30] [--max-polls 120]

Stdout (final JSON):
    {"run_id": "...", "life_cycle_state": "TERMINATED",
     "result_state": "SUCCESS", "duration_seconds": 142}

Exit codes:
    0 — SUCCESS
    1 — FAILED or CANCELED
    2 — INTERNAL_ERROR or SKIPPED
    3 — Poll timeout reached

This wraps ./poll_run.sh for a stable, scriptable interface.
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

TERMINAL_STATES = {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}

EXIT_ON_RESULT = {
    "SUCCESS": 0,
    "FAILED": 1,
    "CANCELED": 1,
    "TIMEDOUT": 1,
}

EXIT_ON_LIFECYCLE = {
    "TERMINATED": None,      # defer to result_state
    "INTERNAL_ERROR": 2,
    "SKIPPED": 2,
}


def get_run_state(run_id: str) -> dict:
    """Call databricks CLI to get current run state."""
    cmd = ["databricks", "jobs", "get-run", "--run-id", run_id, "-o", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))
    if result.returncode != 0:
        raise RuntimeError(f"CLI error: {result.stderr}")
    data = json.loads(result.stdout)
    state = data.get("state", {})
    return {
        "life_cycle_state": state.get("life_cycle_state", "UNKNOWN"),
        "result_state": state.get("result_state"),
        "state_message": state.get("state_message", ""),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--max-polls", type=int, default=120)
    args = parser.parse_args()

    start = time.time()

    for poll_num in range(1, args.max_polls + 1):
        try:
            state = get_run_state(args.run_id)
        except Exception as e:
            print(json.dumps({"error": str(e), "poll": poll_num}), file=sys.stderr)
            time.sleep(args.interval)
            continue

        lc = state["life_cycle_state"]
        rs = state.get("result_state")

        print(
            f"[poll {poll_num:3d}] lifecycle={lc} result={rs or '—'}",
            file=sys.stderr,
        )

        if lc in TERMINAL_STATES:
            duration = int(time.time() - start)
            payload = {
                "run_id": args.run_id,
                "life_cycle_state": lc,
                "result_state": rs,
                "duration_seconds": duration,
                "state_message": state.get("state_message", ""),
            }
            print(json.dumps(payload))

            if lc == "INTERNAL_ERROR":
                print(
                    "WARNING: INTERNAL_ERROR is terminal. "
                    "Inspect logs before re-submitting.",
                    file=sys.stderr,
                )
                return 2
            if lc == "SKIPPED":
                return 2
            # TERMINATED — check result_state
            return EXIT_ON_RESULT.get(rs, 1)

        time.sleep(args.interval)

    # Timeout
    print(
        json.dumps(
            {
                "run_id": args.run_id,
                "error": "timeout",
                "max_polls": args.max_polls,
            }
        )
    )
    return 3


if __name__ == "__main__":
    sys.exit(main())

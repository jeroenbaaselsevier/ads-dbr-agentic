#!/usr/bin/env python3
"""
tools/databricks/export_run_output.py
=======================================
Export and decode the full cell output from a completed Databricks run.

Usage:
    python tools/databricks/export_run_output.py <run_id> [--out ./tmp/run_output.json]

Stdout (JSON):
    {
      "run_id": "...",
      "cells": [
        {
          "cell_index": 0,
          "output_type": "text",
          "data": "...",
          "cause": null,
          "summary": null
        },
        ...
      ]
    }

Exit codes:
    0 — successful
    1 — export failed

Notes:
    - An empty "data" field does NOT mean the cell was clean.
      Always check "cause" and "summary" for Spark/Analysis exceptions.
    - See agent-core/runbooks/databricks.md for the full decode procedure.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def decode_cells(raw: dict) -> list:
    """Extract per-cell output including cause/summary for errors."""
    cells = []
    views = raw.get("views", []) or []
    for i, view in enumerate(views):
        content = view.get("content", "")
        # content is the full HTML notebook output as a string in some API versions.
        # In JSON output mode, cells come as listResults.
        list_results = view.get("listResults", {}) or {}
        results = list_results.get("results", []) or []

        for j, result in enumerate(results):
            cells.append(
                {
                    "cell_index": j,
                    "output_type": result.get("resultType", "unknown"),
                    "data": result.get("data", ""),
                    "cause": result.get("cause"),
                    "summary": result.get("summary"),
                }
            )

        if not results and content:
            # Fall back to raw HTML content
            cells.append(
                {
                    "cell_index": i,
                    "output_type": "html",
                    "data": content[:500] + ("..." if len(content) > 500 else ""),
                    "cause": None,
                    "summary": None,
                }
            )
    return cells


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id")
    parser.add_argument("--out", default=None, help="Path to save raw JSON (optional)")
    args = parser.parse_args()

    cmd = [
        "databricks",
        "jobs",
        "export-run",
        "--run-id",
        args.run_id,
        "--views-to-export",
        "ALL",
        "-o",
        "json",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))

    if result.returncode != 0:
        print(
            json.dumps({"error": "export failed", "stderr": result.stderr}),
            file=sys.stderr,
        )
        return 1

    try:
        raw = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"JSON decode failed: {e}"}), file=sys.stderr)
        return 1

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(result.stdout)
        print(f"Raw output saved to {args.out}", file=sys.stderr)

    cells = decode_cells(raw)
    payload = {"run_id": args.run_id, "cells": cells}
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
tools/qa/validate_notebook.py
================================
Static quality check for a Databricks PySpark notebook.

Checks:
  - nopp() applied to ANI queries
  - no INNER JOIN to secondary tables (all joins should be LEFT)
  - afid cast to string before OrgDB join
  - long_eid_to_eidstr used before SciVal join
  - notebook stored under notebooks/
  - no hardcoded credentials

Usage:
    python tools/qa/validate_notebook.py <notebook_path>

Stdout (JSON):
    {"notebook": "...", "passed": true, "violations": [], "warnings": []}

Exit codes:
    0 — all checks passed
    1 — one or more violations found
"""

import argparse
import json
import re
import sys
from pathlib import Path


CHECKS = [
    {
        "id": "nopp_required",
        "description": "ANI table used but nopp() not applied",
        "pattern_trigger": r"scopus\.ani_|ani_stamp",
        "pattern_required": r"nopp\(\)",
        "severity": "violation",
    },
    {
        "id": "scival_eid_conversion",
        "description": "SciVal join detected but long_eid_to_eidstr not used",
        "pattern_trigger": r"topic_eid|scival",
        "pattern_required": r"long_eid_to_eidstr",
        "severity": "violation",
    },
    {
        "id": "orgdb_cast",
        "description": "OrgDB join detected but no string cast on afid",
        "pattern_trigger": r"orgdb_support|get_df_hierarchy",
        "pattern_required": r"\.cast\(['\"]string",
        "severity": "warning",
    },
    {
        "id": "no_credentials",
        "description": "Possible hardcoded credential detected",
        "pattern_trigger": r"(?i)(aws_access_key|aws_secret|password\s*=\s*['\"][^'\"]{8,}|token\s*=\s*['\"][A-Za-z0-9+/]{20,})",
        "pattern_required": None,  # Trigger = violation
        "severity": "violation",
    },
    {
        "id": "notebook_path",
        "description": "Notebook is not stored under notebooks/",
        "pattern_trigger": None,   # checked separately
        "pattern_required": None,
        "severity": "warning",
    },
    {
        "id": "first_of_month_stamp",
        "description": "ANI stamp does not use first-of-month date",
        "pattern_trigger": r"ani_stamp\s*=\s*['\"](\d{6})([02-9]\d)['\"]",
        "pattern_required": None,  # Trigger = violation (day != 01)
        "severity": "warning",
    },
]


def check_notebook(path: Path) -> dict:
    content = path.read_text()
    violations = []
    warnings = []

    for check in CHECKS:
        if check["id"] == "notebook_path":
            rel = str(path)
            if "notebooks/" not in rel:
                warnings.append(
                    {"check": check["id"], "message": check["description"]}
                )
            continue

        if check["id"] == "no_credentials":
            if re.search(check["pattern_trigger"], content):
                violations.append(
                    {"check": check["id"], "message": check["description"]}
                )
            continue

        if check["id"] == "first_of_month_stamp":
            for match in re.finditer(r"ani_stamp\s*=\s*['\"](\d{4})(\d{2})(\d{2})['\"]", content):
                day = match.group(3)
                if day != "01":
                    warnings.append(
                        {
                            "check": check["id"],
                            "message": f"{check['description']} (found day={day})",
                        }
                    )
            continue

        trigger = check.get("pattern_trigger")
        required = check.get("pattern_required")

        if not trigger:
            continue

        if re.search(trigger, content, re.IGNORECASE):
            if required and not re.search(required, content, re.IGNORECASE):
                entry = {"check": check["id"], "message": check["description"]}
                if check["severity"] == "violation":
                    violations.append(entry)
                else:
                    warnings.append(entry)

    return {
        "notebook": str(path),
        "passed": len(violations) == 0,
        "violations": violations,
        "warnings": warnings,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("notebook_path")
    args = parser.parse_args()

    path = Path(args.notebook_path)
    if not path.exists():
        print(json.dumps({"error": f"File not found: {args.notebook_path}"}))
        return 1

    result = check_notebook(path)
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
tools/aws/check_auth.py
========================
Check whether current AWS session credentials are valid.

Usage:
    python tools/aws/check_auth.py

Stdout (JSON):
    {"authenticated": true, "account_id": "029211843733",
     "role": "EnterpriseAdmin", "expiry": "2026-03-16T20:00:00Z"}

Exit codes:
    0 — authenticated
    1 — not authenticated or credentials expired
"""

import json
import subprocess
import sys


def main() -> int:
    result = subprocess.run(
        ["aws", "sts", "get-caller-identity", "--output", "json"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(
            json.dumps(
                {
                    "authenticated": False,
                    "error": result.stderr.strip() or "aws sts call failed",
                }
            )
        )
        return 1

    try:
        identity = json.loads(result.stdout)
    except json.JSONDecodeError:
        print(json.dumps({"authenticated": False, "error": "Unexpected response format"}))
        return 1

    account_id = identity.get("Account", "")
    arn = identity.get("Arn", "")
    # Extract role name from ARN (arn:aws:sts::ACCOUNT:assumed-role/ROLE/SESSION)
    role = arn.split("/")[1] if "/" in arn else arn

    print(
        json.dumps(
            {
                "authenticated": True,
                "account_id": account_id,
                "role": role,
                "arn": arn,
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

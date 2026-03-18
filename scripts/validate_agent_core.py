#!/usr/bin/env python3
"""
scripts/validate_agent_core.py
================================
Static validator for the agent-core canonical knowledge base.

Checks:
  1. Every reference path in knowledge-index.yaml exists on disk
  2. Every recipe declares tables that exist in the catalog
  3. All table YAML manifests are present for tables in the index
  4. All role files exist
  5. All tool contract files exist
  6. All runbooks exist

Usage:
    python scripts/validate_agent_core.py

Exit codes:
    0 — all checks passed
    1 — one or more errors found
"""

import json
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
AGENT_CORE = REPO_ROOT / "agent-core"

REQUIRED_ROLES = [
    "analyst-orchestrator.md",
    "schema-explorer.md",
    "notebook-implementer.md",
    "results-packager.md",
    "reviewer.md",
    "project-resources.md",
]

REQUIRED_TOOL_CONTRACTS = [
    "databricks.yaml",
    "aws.yaml",
    "local-python.yaml",
    "export.yaml",
]

REQUIRED_RUNBOOKS = [
    "databricks.md",
    "local-python.md",
    "aws-and-s3.md",
]


def load_yaml(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def check_file_exists(path: Path, label: str, errors: list) -> bool:
    if not path.exists():
        errors.append(f"MISSING {label}: {path.relative_to(REPO_ROOT)}")
        return False
    return True


def validate(verbose: bool = True) -> list:
    errors = []

    # 1. Load knowledge index
    index_path = AGENT_CORE / "catalog/knowledge-index.yaml"
    if not check_file_exists(index_path, "knowledge-index", errors):
        return errors

    index = load_yaml(index_path)

    # 2. Check every reference file exists
    table_names = set()
    for table in index.get("tables", []):
        table_names.add(table["name"].lower())
        ref = table.get("reference")
        if ref:
            ref_path = REPO_ROOT / ref
            check_file_exists(ref_path, f"reference for {table['name']}", errors)

    lib_ref = index.get("library", {}).get("reference")
    if lib_ref:
        check_file_exists(REPO_ROOT / lib_ref, "library reference", errors)

    # 3. Check per-table YAML manifests
    for name in table_names:
        manifest = AGENT_CORE / f"catalog/tables/{name}.yaml"
        check_file_exists(manifest, f"table manifest for {name}", errors)

    # 4. Check recipe required_tables against index
    recipes_dir = AGENT_CORE / "recipes"
    catalog_names_upper = {t["name"].upper() for t in index.get("tables", [])}
    for recipe_file in recipes_dir.glob("*.md"):
        content = recipe_file.read_text()
        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) >= 3:
                try:
                    meta = yaml.safe_load(parts[1])
                    for req_table in meta.get("required_tables", []):
                        if req_table.upper() not in catalog_names_upper:
                            errors.append(
                                f"Recipe '{recipe_file.name}' requires unknown table '{req_table}'"
                            )
                except Exception as e:
                    errors.append(f"Recipe '{recipe_file.name}' has invalid frontmatter: {e}")

    # 5. Check role files
    for role_file in REQUIRED_ROLES:
        check_file_exists(AGENT_CORE / "roles" / role_file, f"role file {role_file}", errors)

    # 6. Check tool contract files
    for contract_file in REQUIRED_TOOL_CONTRACTS:
        check_file_exists(
            AGENT_CORE / "tool-contract" / contract_file,
            f"tool contract {contract_file}",
            errors,
        )

    # 7. Check runbooks
    for runbook in REQUIRED_RUNBOOKS:
        check_file_exists(
            AGENT_CORE / "runbooks" / runbook, f"runbook {runbook}", errors
        )

    # 8. Check profiles directories exist
    for profile_dir in ["clients", "users"]:
        d = AGENT_CORE / "profiles" / profile_dir
        if not d.exists():
            errors.append(f"MISSING profiles directory: {d.relative_to(REPO_ROOT)}")

    # 9. Check agent-improvement structure
    improvement_dir = REPO_ROOT / "agent-improvement"
    for sub in ["inbox", "triage", "reports", "schemas"]:
        d = improvement_dir / sub
        if not d.exists():
            errors.append(f"MISSING agent-improvement directory: {d.relative_to(REPO_ROOT)}")
    for schema_file in ["lesson.yaml", "project.yaml"]:
        check_file_exists(
            improvement_dir / "schemas" / schema_file,
            f"improvement schema {schema_file}",
            errors,
        )

    # 10. Check project lifecycle scripts exist
    for script in ["init_project.py", "closeout_project.py", "capture_lessons.py", "triage_lessons.py"]:
        check_file_exists(
            REPO_ROOT / "scripts" / script,
            f"project lifecycle script {script}",
            errors,
        )

    return errors


def main() -> int:
    errors = validate()

    if errors:
        print(f"agent-core validation FAILED ({len(errors)} error(s)):\n")
        for e in errors:
            print(f"  ✗ {e}")
        print()
        print(json.dumps({"passed": False, "errors": errors}))
        return 1

    print("agent-core validation PASSED\n")
    print(json.dumps({"passed": True, "errors": []}))
    return 0


if __name__ == "__main__":
    try:
        import yaml  # noqa
    except ImportError:
        print("ERROR: PyYAML not installed. Run: pip install pyyaml", file=sys.stderr)
        sys.exit(1)
    sys.exit(main())

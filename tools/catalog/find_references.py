#!/usr/bin/env python3
"""
tools/catalog/find_references.py
==================================
Look up which reference files and recipes are relevant for a given topic.

Usage:
    python tools/catalog/find_references.py <topic>
    python tools/catalog/find_references.py "fwci citation"

Stdout (JSON):
    {
      "query": "fwci citation",
      "matches": [
        {"type": "table", "name": "ADS", "reference": "agent-core/references/ads-derived/README.md"},
        {"type": "recipe", "name": "fwci-citation-analysis", "file": "agent-core/recipes/fwci-citation-analysis.md"}
      ]
    }

Exit codes:
    0 — matches found
    1 — no matches
"""

import argparse
import json
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
KNOWLEDGE_INDEX = REPO_ROOT / "agent-core/catalog/knowledge-index.yaml"
RECIPES_DIR = REPO_ROOT / "agent-core/recipes"


def load_index() -> dict:
    with open(KNOWLEDGE_INDEX) as f:
        return yaml.safe_load(f)


def load_recipe_triggers() -> list:
    """Load name + triggers from recipe YAML frontmatter."""
    recipes = []
    for recipe_file in RECIPES_DIR.glob("*.md"):
        content = recipe_file.read_text()
        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) >= 3:
                try:
                    meta = yaml.safe_load(parts[1])
                    recipes.append(
                        {
                            "name": meta.get("name", recipe_file.stem),
                            "triggers": meta.get("triggers", []),
                            "file": str(recipe_file.relative_to(REPO_ROOT)),
                        }
                    )
                except Exception:
                    pass
    return recipes


def score_match(query_tokens: list, keywords: list) -> int:
    return sum(1 for t in query_tokens if any(t in kw.lower() for kw in keywords))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("topic", nargs="+")
    args = parser.parse_args()

    query = " ".join(args.topic).lower()
    tokens = query.split()

    index = load_index()
    recipes = load_recipe_triggers()
    matches = []

    # Search tables
    for table in index.get("tables", []):
        keywords = [k.lower() for k in table.get("keywords", [])]
        score = score_match(tokens, keywords)
        if score > 0:
            matches.append(
                {
                    "type": "table",
                    "name": table["name"],
                    "reference": table.get("reference", ""),
                    "score": score,
                    "summary": table.get("summary", ""),
                }
            )

    # Search library
    lib = index.get("library", {})
    lib_keywords = [k.lower() for k in lib.get("keywords", [])]
    if score_match(tokens, lib_keywords) > 0:
        matches.append(
            {
                "type": "library",
                "name": "library",
                "reference": lib.get("reference", ""),
                "score": score_match(tokens, lib_keywords),
            }
        )

    # Search recipes
    for recipe in recipes:
        triggers = [t.lower() for t in recipe.get("triggers", [])]
        score = score_match(tokens, triggers)
        if score > 0:
            matches.append(
                {
                    "type": "recipe",
                    "name": recipe["name"],
                    "file": recipe["file"],
                    "score": score,
                }
            )

    matches.sort(key=lambda x: -x["score"])

    # Remove score from output
    for m in matches:
        m.pop("score", None)

    print(json.dumps({"query": query, "matches": matches}, indent=2))
    return 0 if matches else 1


if __name__ == "__main__":
    # yaml may not be in PATH — try to import from .venv
    try:
        import yaml  # noqa: F401 (already imported above)
    except ImportError:
        print(
            json.dumps(
                {"error": "PyYAML not installed. Run: pip install pyyaml"}
            ),
            file=sys.stderr,
        )
        sys.exit(1)
    sys.exit(main())

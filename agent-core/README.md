# Agent Core

This directory is the **single source of truth** for all agent knowledge and
behaviour in this repository. Nothing in here is platform-specific.

## Architecture overview

```
agent-core/          ← YOU ARE HERE — canonical, always hand-edited
platform/            ← thin adapter templates (Jinja2), one sub-dir per platform
.github/agents/      ← Copilot adapter (generated from platform/copilot/templates/)
.claude/             ← Claude Code adapter (generated from platform/claude/templates/)
.agents/ + AGENTS.md ← Codex adapter (generated from platform/codex/templates/)
scripts/             ← build, validate, doctor scripts
tools/               ← stable CLI tool layer (Databricks, AWS, local, QA)
```

**Rule:** edit knowledge here; run `scripts/build_agent_adapters.py` to regenerate
platform files. **Never hand-edit generated platform files** — they carry an
`AUTO-GENERATED` banner at the top.

## Directory map

| Path | Contents |
|---|---|
| `rules/core-rules.md` | Non-negotiable behavioural rules (18 rules) |
| `rules/output-contract.md` | Output path and format conventions |
| `rules/security-and-auth.md` | Credential, auth, and security rules |
| `catalog/knowledge-index.yaml` | Topic → reference-file routing index |
| `catalog/tables/*.yaml` | Per-table manifests (schema, join rules, validators) |
| `references/*.md` | Deep per-table schema docs |
| `references/ads-derived/` | ADS-derived metric table docs |
| `recipes/*.md` | Copy-paste notebook templates with YAML frontmatter |
| `runbooks/databricks.md` | Databricks deploy/run/monitor/decode procedures |
| `runbooks/local-python.md` | Local Python / parquet / DuckDB procedures |
| `runbooks/aws-and-s3.md` | AWS authentication and S3 access procedures |
| `roles/*.md` | Portable subagent role definitions |
| `tool-contract/*.yaml` | Stable CLI tool specs (inputs, outputs, exit codes) |
| `evals/tasks/*.yaml` | Benchmark analytical task definitions |
| `evals/goldens/` | Golden output artifacts for regression testing |
| `evals/rubrics/` | LLM-judge rubrics for qualitative answer evaluation |

## How to update knowledge

1. Edit the relevant file in `agent-core/`.
2. Run `python scripts/validate_agent_core.py` to check for broken references.
3. Run `python scripts/build_agent_adapters.py` to regenerate platform adapters.
4. Commit both the `agent-core/` change and the regenerated adapter files together.

## How to add a new table

1. Add a row in `catalog/knowledge-index.yaml` with `name`, `reference`, `spark_table`, `primary_key`, `keywords`, and `summary`.
2. Create `catalog/tables/<tablename>.yaml` with the full table manifest.
3. Create `references/<tablename>.md` with schema details, sample columns, and caveats.
4. Regenerate adapters.

## How to add a new recipe

1. Create `recipes/<name>.md` with YAML frontmatter (`name`, `triggers`, `required_tables`, etc.).
2. Add the recipe to the relevant table entries in `catalog/knowledge-index.yaml`.
3. Regenerate adapters.

## How to port to a new agent platform

1. Create `platform/<platform>/templates/` with Jinja2 templates.
2. Add a handler in `scripts/build_agent_adapters.py`.
3. Generate and test the output files.
4. Add the generated file paths to `scripts/validate_platform_outputs.py`.

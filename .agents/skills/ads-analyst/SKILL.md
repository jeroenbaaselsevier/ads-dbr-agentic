---
name: ads-analyst
description: >
  Scopus/bibliometric analytics workflow skill for Codex.
  Covers PySpark notebook generation, Databricks deployment, and local post-processing.
applyTo: "**"
---

# ADS Analyst Skill

> AUTO-GENERATED. Canonical knowledge lives in `agent-core/`.

## Session start checklist

1. Read `agent-core/rules/core-rules.md`
2. Read `agent-core/catalog/knowledge-index.yaml`
3. Run `[[ -d rads_library ]] || ./sync_library.sh`
4. If S3 access needed: run `~/go-aws-sso` first

## Workflow steps

1. Clarify (one round only if needed)
2. Plan with todo list
3. Lookup schema: `agent-core/catalog/knowledge-index.yaml` → `agent-core/references/`
4. Check recipe: `agent-core/recipes/`
5. Write notebook (see `agent-core/roles/notebook-implementer.md`)
6. Deploy: `./deploy.sh notebooks/<name>.py --run`
7. Monitor: `./poll_run.sh <run_id>`
8. Decode results: `agent-core/runbooks/databricks.md`
9. Local work: `agent-core/runbooks/local-python.md`
10. Review: `agent-core/roles/reviewer.md`

## Critical rules (short form)

- `nopp()` on ANI — always
- LEFT JOIN all secondary tables — always
- Cast `afid` long → string for OrgDB
- `long_eid_to_eidstr()` for SciVal joins
- First-of-month stamps only
- Code in `notebooks/<shortname>/`
- Parquet for Spark → local handover
- Deliverables in `./output/`, temps in `./tmp/`

---
name: ads-analyst
description: >
  Full analytics workflow for Scopus/bibliometric research: PySpark notebook
  generation, Databricks deployment, result decoding, and local post-processing.
  Built on the agent-core canonical knowledge system.
---

# ADS Analyst Skill

> AUTO-GENERATED. Canonical knowledge lives in `agent-core/`.

## At conversation start

Read these before doing anything else:

1. `agent-core/rules/core-rules.md`
2. `agent-core/catalog/knowledge-index.yaml`

Also check the library mirror:
```bash
[[ -d rads_library ]] || ./sync_library.sh
```

## Workflow

1. **Define project ID/folder** — propose `YYYY_ISO3_shortname`, map to `projects/<project_id>/`, confirm with user
2. **Clarify** — one round only if needed
3. **Plan** — use todo list
4. **Schema lookup** — `agent-core/catalog/knowledge-index.yaml` → relevant `agent-core/references/*.md`
5. **Recipe check** — `agent-core/recipes/` for common patterns
6. **Generate notebook** — mandatory boilerplate from `agent-core/roles/notebook-implementer.md`
7. **Deploy** — `./deploy.sh projects/<project_id>/notebooks/spark/<name>.py --run`
8. **Monitor** — `./poll_run.sh <run_id>`
9. **Decode results** — `agent-core/runbooks/databricks.md`
10. **Local post-processing** — `agent-core/runbooks/local-python.md`
11. **Review** — run `agent-core/roles/reviewer.md` checklist

## Reference files

All schema and pattern knowledge:
- `agent-core/references/` — per-table schemas
- `agent-core/recipes/` — copy-paste notebook templates
- `agent-core/runbooks/` — execution procedures
- `agent-core/roles/` — role definitions (including `project-resources.md`)
- `agent-core/tool-contract/` — tool specs
- `agent-core/profiles/` — client and user preferences

## Project resources

For any task creating code, outputs, or S3 assets:
- Bootstrap with `python scripts/init_project.py`
- New work goes in `projects/<project_id>/` (not `notebooks/`)
- Run closeout at session end: `python scripts/closeout_project.py`
- See `agent-core/roles/project-resources.md` for the full workflow

## Self-improvement discipline

- Never directly edit `core-rules.md` or any always-on prompt file
- Capture lessons via `scripts/capture_lessons.py`
- Improvement pipeline: session → inbox → triage → PR → merge

## Join caveats quick-reference

| Source | Join key | Caveat |
|---|---|---|
| ANI | `Eid` (long) | Primary — always apply `nopp()` |
| APR | `auid` long | LEFT JOIN — partial coverage |
| OrgDB | `org_id` string | Cast `afid` long → string |
| Source | `id` long ↔ `source.srcid` | LEFT JOIN — 6.8% match |
| SciVal | `EidString` string | Convert with `long_eid_to_eidstr()` |
| SDG | `eid` long | LEFT JOIN — 22–25% coverage |
| Patents | `eid` long | LEFT JOIN — 15% coverage |
| ADS | `EID` long | LEFT JOIN — ADS broader than nopp() |

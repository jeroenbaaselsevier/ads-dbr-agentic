---
name: project-resources
description: >
  Project lifecycle management: bootstrap, path derivation, scaffolding,
  session closeout, and lesson capture for analytics projects.
applyTo: "projects/**"
---

# Project Resources Skill

> Canonical role definition: `agent-core/roles/project-resources.md`

## Quick reference

### Bootstrap a new project
```bash
python scripts/init_project.py \
    --year 2026 --iso3 NLD --short-name journal_trend \
    --display-name "Journal trend analysis for client X" \
    --ani-stamp 20260301
```

### Path derivation (from project_id)

| Path | Formula |
|---|---|
| Local root | `projects/<project_id>` |
| S3 root | `s3://rads-projects/short_term/<year>/<project_id>` |
| DBFS root | `/mnt/els/rads-projects/short_term/<year>/<project_id>` |
| Workspace | `/Workspace/rads/projects/<project_id>` |
| Output | `projects/<project_id>/output` |
| Tmp | `projects/<project_id>/tmp` |

### Session closeout
```bash
python scripts/closeout_project.py \
    --project-id 2026_NLD_journal_trend \
    --session-id 20260318T1620 \
    --status completed \
    --summary "Produced trend charts" \
    --deliverables output/trend.png,output/trend.csv
```

### Capture lessons
```bash
echo '[{"scope":"global","memory_type":"semantic","category":"schema","summary":"...","confidence":"high"}]' | \
python scripts/capture_lessons.py \
    --project-id 2026_NLD_journal_trend \
    --session-id 20260318T1620 --from-json
```

### Triage inbox
```bash
python scripts/triage_lessons.py
python scripts/triage_lessons.py --generate-issues
```

## Project folder structure
```
projects/<project_id>/
  project.yaml
  README.md
  notebooks/spark/
  notebooks/exploratory/
  scripts/local/
  context/
    brief.md
    decisions.md
    open-questions.md
    summary.md
    deliverables.yaml
    session-notes/
    lessons/
  output/           # gitignored
  tmp/              # gitignored
```

## Self-improvement flow

```
session lesson → project/context/lessons/
  ↓ (scope != project)
agent-improvement/inbox/
  ↓ (weekly triage)
agent-improvement/triage/
  ↓ (accepted)
agent-core/ or profiles/ (via PR)
```

## Rules
- Always propose project ID before creating code
- Use `projects/` not `notebooks/` for new work
- Run closeout at end of every session
- Never append raw lessons to core-rules.md

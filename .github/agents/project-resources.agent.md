---
name: project-resources
description: >
  Manages project lifecycle: bootstrap, path derivation, active project state,
  scaffolding, and session closeout with lesson capture.
tools:
  - vscode/memory
  - execute/*
  - read/*
  - edit/*
  - search/*
  - manage_todo_list
---

# Project Resources Agent

> AUTO-GENERATED concept — canonical knowledge lives in `agent-core/roles/project-resources.md`.

## When to use this agent

Invoke this agent whenever a task will create code, outputs, dashboards,
or S3 assets and a project container is needed.

## Session start

1. Read `agent-core/roles/project-resources.md`
2. Read `agent-core/rules/core-rules.md`

## Workflow

1. **Propose** a project ID: `YYYY_ISO3_shortname`
2. Check for existing project under `projects/<project_id>/`
3. If new: `python scripts/init_project.py --year YYYY --iso3 ISO3 --short-name slug --display-name "..."`
4. Write `.agent-state/active_project.json`
5. Confirm with user (one round only)
6. At session end: `python scripts/closeout_project.py --project-id ... --session-id ... --status ...`

## Path conventions

| Path | Formula |
|---|---|
| Local root | `projects/<project_id>` |
| S3 root | `s3://rads-projects/short_term/<year>/<project_id>` |
| DBFS root | `/mnt/els/rads-projects/short_term/<year>/<project_id>` |
| Workspace | `/Workspace/rads/projects/<project_id>` |

## Key scripts

- `scripts/init_project.py` — create project scaffold
- `scripts/closeout_project.py` — finalize session
- `scripts/capture_lessons.py` — structured lesson capture
- `scripts/triage_lessons.py` — review inbox lessons

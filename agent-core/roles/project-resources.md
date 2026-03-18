# Project Resources — Role Definition

> Manages project lifecycle: bootstrap, path derivation, active project state,
> scaffolding, and session closeout with lesson capture.

## Responsibilities

### 1. Bootstrap

For any task that will create code, outputs, dashboards, or S3 assets:

1. **Propose** a project ID using the convention `YYYY_ISO3_shortname`
   - `YYYY` = calendar year
   - `ISO3` = ISO 3166-1 alpha-3 country code, or `INTERNAL`
   - `shortname` = lowercase snake_case slug
2. Check for an existing project under `projects/<project_id>/`
3. If new, run: `python scripts/init_project.py --year YYYY --iso3 ISO3 --short-name slug --display-name "..." [--ani-stamp YYYYMMDD]`
4. Confirm the proposed project with the user (one round only)

### 2. Path derivation

All paths derive from `project_id` and `year`:

| Path | Formula |
|---|---|
| Local root | `projects/<project_id>` |
| S3 root | `s3://rads-projects/short_term/<year>/<project_id>` |
| DBFS root | `/mnt/els/rads-projects/short_term/<year>/<project_id>` |
| Workspace root | `/Workspace/rads/projects/<project_id>` |
| Local output | `projects/<project_id>/output` |
| Local tmp | `projects/<project_id>/tmp` |
| S3 cache | `s3://rads-projects/short_term/<year>/<project_id>/cache` |

### 3. Active project state

Write `.agent-state/active_project.json` with:
```json
{
  "project_id": "2026_NLD_journal_trend",
  "session_id": "20260318T1620",
  "started_at": "2026-03-18T16:20:00Z",
  "local_root": "projects/2026_NLD_journal_trend"
}
```

Scripts and hooks read this to know where to write session artifacts.

### 4. Scaffolding

The `init_project.py` script creates:
```
projects/<project_id>/
  project.yaml          # manifest with all paths
  README.md
  notebooks/spark/
  notebooks/exploratory/
  scripts/local/
  context/brief.md
  context/decisions.md
  context/open-questions.md
  context/summary.md
  context/deliverables.yaml
  context/session-notes/
  context/lessons/
  output/               # gitignored
  tmp/                  # gitignored
```

### 5. Session closeout

At the end of a project session, run explicit closeout:

```bash
python scripts/closeout_project.py \
    --project-id 2026_NLD_journal_trend \
    --session-id 20260318T1620 \
    --status completed \
    --summary "Produced journal trend charts for 2015-2025" \
    --deliverables output/trend.png,output/trend.csv
```

This produces:
1. **Session summary** → `context/session-notes/<session_id>.md`
2. **Lessons file** → `context/lessons/<session_id>.yaml` (fill in or use capture_lessons.py)
3. **Deliverables manifest** → `context/deliverables.yaml` (appended)
4. **Intake record** → `agent-improvement/inbox/` (cross-project lessons only)

### 6. Lesson capture

For structured lessons, use:
```bash
echo '[{"scope":"global","memory_type":"semantic","category":"schema","summary":"...","confidence":"high"}]' | \
python scripts/capture_lessons.py \
    --project-id 2026_NLD_journal_trend \
    --session-id 20260318T1620 \
    --from-json
```

### 7. Agent interaction pattern

When the user brings a new analysis task, propose the project like this:

> Proposed project: `2026_NLD_journal_trend`
> Local root: `projects/2026_NLD_journal_trend`
> Shared root: `s3://rads-projects/short_term/2026/2026_NLD_journal_trend`
> I'll use this unless you want a different short name or country code.

If the user does not object, use the proposed defaults and continue.

## Anti-patterns

- Do not create projects under `notebooks/` — use `projects/` only
- Do not skip closeout — even partial sessions should produce a summary
- Do not append raw lessons to core-rules.md or any always-on prompt file
- Do not mix project-specific facts with global lessons

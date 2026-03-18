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
| S3 exports | `s3://rads-projects/short_term/<year>/<project_id>/exports` |
| S3 logs | `s3://rads-projects/short_term/<year>/<project_id>/logs` |
| Databricks notebooks | `/Workspace/rads/projects/<project_id>/notebooks` |

### 3. Active project state

Write `.agent-state/active_project.json` with:
```json
{
  "project_id": "2026_NLD_journal_trend",
  "session_id": "20260318T1620",
  "started_at": "2026-03-18T16:20:00Z",
  "local_root": "projects/2026_NLD_journal_trend",
  "manifest_path": "projects/2026_NLD_journal_trend/project.yaml",
  "s3_root": "s3://rads-projects/short_term/2026/2026_NLD_journal_trend",
  "dbfs_root": "/mnt/els/rads-projects/short_term/2026/2026_NLD_journal_trend",
  "databricks_workspace_root": "/Workspace/rads/projects/2026_NLD_journal_trend"
}
```

This is written automatically by `init_project.py` (default) and
`activate_project.py`. Cleared on closeout.

To resume an existing project:
```bash
python scripts/activate_project.py --project-id 2026_NLD_journal_trend
```

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

At the end of a project session, run the explicit closeout sequence:

**Step 1 — closeout:**
```bash
python scripts/closeout_project.py \
    --project-id 2026_NLD_journal_trend \
    --session-id 20260318T1620 \
    --session-status completed \
    --summary "Produced journal trend charts for 2015-2025" \
    --deliverables output/trend.png,output/trend.csv
```

Use `--session-status` with values: `completed`, `paused`, or `blocked`.
Optionally add `--project-status completed` when the entire project is done.

**Step 2 — structured lesson capture (if applicable):**
```bash
echo '[{"scope":"global","memory_type":"semantic","category":"schema","summary":"...","confidence":"high"}]' | \
python scripts/capture_lessons.py \
    --project-id 2026_NLD_journal_trend \
    --session-id 20260318T1620 \
    --from-json
```

**Step 3 — triage (periodic, outside the session loop):**
```bash
python scripts/triage_lessons.py
```

This produces:
1. **Session summary** → `context/session-notes/<session_id>.md`
2. **Lessons file** → `context/lessons/<session_id>.yaml` (fill in or use capture_lessons.py)
3. **Deliverables manifest** → `context/deliverables.yaml` (appended)
4. **Intake record** → `agent-improvement/inbox/` (cross-project lessons only)

### 6. Agent interaction pattern

When the user brings a new analysis task, propose the project like this:

> Proposed project: `2026_NLD_journal_trend`
> Local root: `projects/2026_NLD_journal_trend`
> Shared root: `s3://rads-projects/short_term/2026/2026_NLD_journal_trend`
> I'll use this unless you want a different short name or country code.

If the user does not object, use the proposed defaults and continue.

---

## Lesson Capture Workflow (Consolidated Path)

All lessons flow through **a single collection point**: `agent-improvement/inbox/`.

### Two-tier lesson system

**Tier 1: Informal project notes** → `projects/<project_id>/context/session-notes/`
- Free-form markdown files capturing session decisions, what worked, bugs encountered.
- Format: markdown (.md)
- Purpose: project memory and debugging context.
- NOT intended for promotion to canonical knowledge.
- Examples: `session-notes/20260318-summary.md`, `session-notes/debugging-notes.md`

**Tier 2: Formal structured lessons** → `agent-improvement/inbox/`
- Lessons ready for triage and potential promotion to canonical knowledge.
- Format: **YAML only** (follow `agent-improvement/schemas/lesson.yaml`)
- Required fields: id, project_id, scope, memory_type, category, summary, confidence, impact, status, suggested_action
- Scope: one of `project` | `client` | `user` | `global`
  - `project` scope: Lessons affecting only this project (stay local; not promoted)
  - `global` scope: Lessons that benefit all future analyses (promote to agent-core/)
- Examples: `LES-2026-03-18-001.yaml`, `LES-2026-03-14-aum-burst.yaml`

### Creating formal lessons for inbox

When a lesson is ready for potential promotion, structure it as YAML:

```bash
# Template
cat > agent-improvement/inbox/LES-YYYY-MM-DD-NNN.yaml << 'EOF'
source_project: 2026_INTERNAL_coaffiliation
source_session: 20260318_session_id
captured_at: 2026-03-18T16:20:00Z

lessons:
  - id: LES-2026-03-18-001
    project_id: 2026_INTERNAL_coaffiliation
    scope: global
    memory_type: semantic
    category: schema
    confidence: high
    impact: high
    status: captured
    summary: >
      One-paragraph summary of the lesson.
    observed_in:
      - projects/2026_INTERNAL_coaffiliation/notebooks/...
    suggested_action:
      target: agent-core/references/...md
      change_type: doc_update
      proposed_eval: optional-eval-name
    notes: Optional elaboration.
EOF
```

### Translation: session-notes → inbox → canonical

1. **During project:** Capture informal session notes in `projects/<id>/context/session-notes/`
2. **At closeout:** Run `python scripts/closeout_project.py --project-id ... --session-id ...`
   - Generates session summary → `session-notes/<session_id>.md`
   - Creates lessons YAML → `context/lessons/` (optional; user-supplied)
   - Routes cross-project lessons → `agent-improvement/inbox/` (formal YAML, scope != project)
3. **Periodic:** Run `python scripts/triage_lessons.py`
   - Groups lessons by category/scope
   - Identifies duplicates
   - Reports promotion candidates
4. **Trainer review:** Agent-trainer reviews inbox, applies promotions, runs validators

### Important: Inbox is for structured lessons only

- **DO** create `.yaml` files in inbox following the lesson schema
- **DO NOT** create `.md` files in inbox (use `projects/<id>/context/session-notes/` for informal notes)
- **DO NOT** edit `core-rules.md` or generated adapter files directly; route through inbox → trainer → canonical

---

## Anti-patterns

- Do not create projects under `notebooks/` — use `projects/` only
- Do not skip closeout — even partial sessions should produce a summary
- Do not append raw lessons to core-rules.md or any always-on prompt file
- Do not mix project-specific facts with global lessons
- **Do not create markdown (.md) files in agent-improvement/inbox/** — use structured YAML only
- **Do not promote lessons without going through agent-improvement/inbox/** — always use the single path

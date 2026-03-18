---
name: agent-trainer
description: >
  Agent maintenance and knowledge promotion workflow. Classifies lessons, updates
  canonical files in agent-core/, regenerates platform adapters, and runs
  validations. Explicitly invoked — never self-triggered during analysis.
disable-model-invocation: true
---

# Agent Trainer Skill

> AUTO-GENERATED. Canonical role lives in `agent-core/roles/agent-trainer.md`.

## At session start

Read these before doing anything else:

1. `agent-core/roles/agent-trainer.md` — full role spec and routing rules
2. `agent-core/runbooks/agent-maintenance.md` — repo architecture and procedures
3. `agent-core/rules/core-rules.md`
4. `agent-core/catalog/knowledge-index.yaml`

## Two modes

### Capture mode
"Teach the agent…", "Capture this as a lesson…", "Remember that…"

1. Classify: scope, memory_type, category, confidence, impact
2. Route to correct target (see routing rules in role file)
3. Write to project lessons or `agent-improvement/inbox/`
4. Do NOT change canonical files unless explicitly asked

### Promote mode
"Promote this…", "Consolidate lessons…", "Review the inbox…"

1. Read candidate lessons (inbox and/or project context)
2. Deduplicate and group
3. Choose targets using routing matrix
4. Draft promotion:
   ```bash
   python scripts/promote_lessons.py --from-inbox <path> --output <path> --report
   ```
5. Apply canonical changes (or let user review draft first)
6. Post-change pipeline:
   ```bash
   python scripts/validate_agent_core.py
   python scripts/build_agent_adapters.py
   python scripts/validate_platform_outputs.py
   ```
7. Run evals if behaviour changed

## Routing quick-reference

| Lesson type | Target |
|---|---|
| Project-only | `projects/<id>/context/` |
| Client preference | `agent-core/profiles/clients/` |
| User preference | `agent-core/profiles/users/` |
| Schema / table fact | `agent-core/catalog/tables/` + `agent-core/references/` |
| Analysis pattern | `agent-core/recipes/` |
| Global operating rule | `agent-core/rules/core-rules.md` |
| Tool usage | `agent-core/tool-contract/` |
| Maintenance procedure | `agent-core/runbooks/agent-maintenance.md` |

## Key scripts

- `scripts/capture_lessons.py` — structured lesson capture
- `scripts/triage_lessons.py` — inbox scan, grouping, dedup
- `scripts/promote_lessons.py` — draft or apply promotions
- `scripts/validate_agent_core.py` — canonical consistency
- `scripts/build_agent_adapters.py` — regenerate adapters
- `scripts/validate_platform_outputs.py` — verify generated files

## Anti-patterns

- Never edit generated files (`.github/`, `.claude/`, `.agents/`, `.codex/`)
- Never dump everything into `core-rules.md` — use closest valid layer
- Never auto-promote without user consent or meeting promotion threshold
- Never skip the post-change validation pipeline

## Self-improvement discipline

- Capture lessons via `scripts/capture_lessons.py`
- Promotion pipeline: inbox → triage → promote → validate → rebuild → eval
- Audit trail: `agent-improvement/promotions/`

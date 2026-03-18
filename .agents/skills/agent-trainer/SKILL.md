---
name: agent-trainer
description: >
  Agent maintenance and knowledge promotion. Classifies lessons, updates
  canonical files in agent-core/, regenerates platform adapters, runs
  validations. Explicitly invoked for teaching and maintenance only.
applyTo: "agent-core/**,agent-improvement/**,scripts/**,platform/**"
---

# Agent Trainer Skill

> AUTO-GENERATED. Canonical role lives in `agent-core/roles/agent-trainer.md`.

## Session start checklist

1. Read `agent-core/roles/agent-trainer.md`
2. Read `agent-core/runbooks/agent-maintenance.md`
3. Read `agent-core/rules/core-rules.md`
4. Read `agent-core/catalog/knowledge-index.yaml`

## Operating modes

### Capture mode
"Teach the agent…", "Capture this…", "Remember that…"

1. Classify: scope, memory_type, category, confidence, impact
2. Route to correct target (routing matrix in role file)
3. Write to project lessons or `agent-improvement/inbox/`
4. Do NOT change canonical files unless explicitly asked

### Promote mode
"Promote this…", "Consolidate lessons…", "Review the inbox…"

1. Read candidate lessons
2. Deduplicate and group
3. Choose targets
4. `python scripts/promote_lessons.py --from-inbox <path> --output <path>`
5. Apply canonical changes
6. Run post-change pipeline:
   - `python scripts/validate_agent_core.py`
   - `python scripts/build_agent_adapters.py`
   - `python scripts/validate_platform_outputs.py`
7. Run evals if behaviour changed

## Routing

| Lesson type | Target |
|---|---|
| Project-only | `projects/<id>/context/` |
| Client pref | `agent-core/profiles/clients/` |
| User pref | `agent-core/profiles/users/` |
| Schema/table | `agent-core/catalog/tables/` + `references/` |
| Analysis pattern | `agent-core/recipes/` |
| Global rule | `agent-core/rules/core-rules.md` |

## Anti-patterns

- Never edit generated files
- Never dump everything into `core-rules.md`
- Never auto-promote without consent
- Never skip validation pipeline

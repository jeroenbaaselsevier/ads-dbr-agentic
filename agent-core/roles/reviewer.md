# Role: Reviewer

## Purpose
The **reviewer** performs a final quality check on notebooks and results before
they are presented to the user. It can operate as a distinct subagent or as an
internal checklist the orchestrator runs through.

## Review checklist

### Notebook correctness
- [ ] `column_functions.nopp()` is applied as first filter on any ANI query
- [ ] All secondary table joins are `LEFT JOIN` (never INNER)
- [ ] `afid` is cast to string before joining to OrgDB
- [ ] ANI `Eid` is converted with `long_eid_to_eidstr()` before joining SciVal
- [ ] Every expensive intermediate step uses `df_cached()`
- [ ] **New work:** notebook is under `projects/<project_id>/notebooks/spark/`
- [ ] **Legacy:** existing `notebooks/<shortname>/` is acceptable for untouched code
- [ ] `ani_stamp` uses a first-of-month date (e.g. `20260301`, not `20260312`)
- [ ] No credentials embedded in the notebook
- [ ] Project path follows `<year>_<CCC>_<shortname>` convention

### Output correctness
- [ ] **New work:** deliverables saved to `projects/<project_id>/output/`, not `tmp/`
- [ ] **Legacy / one-off:** deliverables in `./output/`, not `./tmp/`
- [ ] User is informed of output paths
- [ ] Column names in results match documented schema (no hallucinated columns)
- [ ] Aggregation logic is correct (correct grain, no double-counting from
  exploded arrays)

### Project consistency (new work)
- [ ] `projects/<project_id>/project.yaml` exists and `status` is `active`
- [ ] `.agent-state/active_project.json` matches the project being worked on
- [ ] Temp files are in `projects/<project_id>/tmp/`, not root `./tmp/`
- [ ] Deploy path derived from manifest (not a user workspace path)

### Coverage warnings provided
- [ ] If coverage < 100%, the user is told the expected match rate
- [ ] LEFT JOIN nulls are accounted for in denominator if computing percentages

## Platform mappings
| Platform | Implementation |
|---|---|
| GitHub Copilot | Inline checklist in `analyst.agent.md` (no separate agent yet) |
| Claude Code | `.claude/agents/reviewer.md` |
| Codex | `.codex/agents/reviewer.toml` |

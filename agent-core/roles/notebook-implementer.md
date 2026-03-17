# Role: Notebook Implementer

## Purpose
The **notebook-implementer** writes PySpark notebooks, respects project path
conventions, inserts mandatory boilerplate, and deploys and monitors Databricks
runs.

## Responsibilities
- Accept a query contract from `schema-explorer` (or inline specifications)
- Write a complete, deployable Databricks notebook following the mandatory
  boilerplate pattern
- Apply `nopp()` as first filter when ANI is the primary table
- Wrap expensive steps in `df_cached()`
- Use correct join types and type conversions from the query contract
- Store the notebook in `notebooks/<shortname>/` (see `rules/output-contract.md`)
- Deploy via `./deploy.sh notebooks/<name>.py --run`
- Monitor via `./poll_run.sh <run_id>` until terminal state
- Decode and return the result output (see `runbooks/databricks.md`)

## What it should NOT do
- Produce local charts or Excel files (delegate to `results-packager`)
- Present raw JSON to the user — parse it first
- Overwrite an existing notebook without explicit user approval

## Mandatory notebook template
```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions, snapshot_functions

ani_stamp        = '<YYYYMMDD first-of-month>'
str_path_project = '/mnt/els/rads-projects/short_term/<year>/<year>_<CCC>_<shortname>'
cache_folder     = os.path.join(str_path_project, 'cache')
```

## Platform mappings
| Platform | Implementation |
|---|---|
| GitHub Copilot | `analyst.agent.md` (steps 4–7) |
| Claude Code | `.claude/agents/analyst.md` |
| Codex | `.codex/agents/analyst.toml` |

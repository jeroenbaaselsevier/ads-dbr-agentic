# Role: Results Packager

## Purpose
The **results-packager** handles all local post-processing: reading parquet
from S3, producing charts and tables, and creating the final client deliverable.

## Responsibilities
- Activate `.venv` before any local Python work
- Read parquet from S3 via `pyarrow.parquet` + `S3FileSystem`
- Use DuckDB for GROUP BY, JOIN, and window operations on large parquet
- Use pandas for final formatting, pivots, and chart data prep
- **Project-scoped (preferred for new work):**
  - Write charts to `projects/<project_id>/output/` (PNG/HTML)
  - Write Excel/CSV deliverables to `projects/<project_id>/output/`
  - Write intermediate files to `projects/<project_id>/tmp/`
- **Legacy / one-off fallback:**
  - Charts and deliverables to `./output/`
  - Intermediate files to `./tmp/`
- Report output paths to the user via `pandas.to_excel()` or
  `dataframe_functions.export_df_csv()`
- Flag any missing package to the user, install it, and self-patch
  `requirements.txt` + `runbooks/local-python.md`

## What it should NOT do
- Write Spark notebooks (delegate to `notebook-implementer`)
- Run Databricks jobs
- Treat `./tmp/` files as deliverables

## Platform mappings
| Platform | Implementation |
|---|---|
| GitHub Copilot | `analyst.agent.md` (step 8) |
| Claude Code | `.claude/agents/analyst.md` (local branch) |
| Codex | `.codex/agents/analyst.toml` |

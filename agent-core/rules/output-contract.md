# Output Contract

Rules governing where and how outputs are written.
Extracted from `core-rules.md` rules 6, 15, and 16 for quick reference.

## Project-first model (new work)

All new analysis work lives under `projects/<project_id>/`. The project
manifest (`project.yaml`) is the authoritative source of paths.

### Local folder conventions

| Folder | Purpose | Git-ignored? |
|---|---|---|
| `projects/<project_id>/tmp/` | Throwaway artifacts: downloaded HTML, raw parquet copies, intermediate files, decode scripts. Never share with the user. | Yes |
| `projects/<project_id>/output/` | Deliverables: charts (PNG/HTML), tables (CSV/Excel), any file the user needs to review or share. Always report paths here to the user. | Yes |

### Notebook and code placement

```
projects/<project_id>/notebooks/spark/<shortname>.py        # Databricks notebook
projects/<project_id>/notebooks/exploratory/<name>.py       # ad-hoc exploration
projects/<project_id>/scripts/local/postprocess.py          # local charts / exports
```

### Deployment

`deploy.sh` reads the nearest `project.yaml` to derive the remote Databricks
workspace path automatically. No manual remote path needed.

```bash
./deploy.sh projects/<project_id>/notebooks/spark/<shortname>.py --run
```

## Legacy fallback (root-level one-offs)

Existing root-level notebooks (`notebooks/`) and global output folders are
still supported for backward compatibility. Use them only when:

- Working on an existing legacy notebook that hasn't been migrated
- Running a quick one-off inspection that doesn't justify a full project

| Folder | Purpose | Git-ignored? |
|---|---|---|
| `./tmp/` | Throwaway artifacts | Yes |
| `./output/` | Deliverables | Yes |

Legacy notebook placement:
```
notebooks/<shortname>.py                            # single-file analyses
notebooks/<shortname>/<shortname>_spark.py          # grouped legacy
notebooks/<shortname>/<shortname>_postprocess.py    # grouped legacy
```

## S3 project path convention

```
/mnt/els/rads-projects/short_term/<year>/<year>_<CCC>_<shortname>/
```
where `<CCC>` is ISO 3166-1 alpha-3 (e.g. `USA`, `GBR`, `NLD`, `INTERNAL`).

## Data handover format

Always use **parquet** to pass data from Databricks to local Python.
Never use CSV as an intermediate format between Spark and pandas.
Only use `export_df_csv()` when the final client deliverable is a CSV/Excel file.

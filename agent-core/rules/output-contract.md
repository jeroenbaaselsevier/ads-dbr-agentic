# Output Contract

Rules governing where and how outputs are written.
Extracted from `core-rules.md` rules 15 and 16 for quick reference.

## Local folder conventions

| Folder | Purpose | Git-ignored? |
|---|---|---|
| `./tmp/` | Throwaway artifacts: downloaded HTML, raw parquet copies, intermediate files, decode scripts. Never share with the user. | Yes |
| `./output/` | Deliverables: charts (PNG/HTML), tables (CSV/Excel), any file the user needs to review or share. Always report paths here to the user. | Yes |

Create as needed:
```bash
mkdir -p ./tmp ./output
```

## Notebook and code placement

All code (Spark notebooks + local post-processing) lives in:
```
notebooks/<shortname>/<shortname>_spark.py        # Databricks notebook
notebooks/<shortname>/<shortname>_postprocess.py   # local charts / exports
```
Single-file analyses may sit directly under `notebooks/`.

## S3 project path convention

```
/mnt/els/rads-projects/short_term/<year>/<year>_<CCC>_<shortname>/
```
where `<CCC>` is ISO 3166-1 alpha-3 (e.g. `USA`, `GBR`, `NLD`, `INTERNAL`).

## Data handover format

Always use **parquet** to pass data from Databricks to local Python.  
Never use CSV as an intermediate format between Spark and pandas.  
Only use `export_df_csv()` when the final client deliverable is a CSV/Excel file.

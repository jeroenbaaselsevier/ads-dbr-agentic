# Local Python Runbook

## Databricks → local handover

**Always use parquet** to pass data from a Databricks notebook to local
processing. Never use CSV as an intermediate format between Spark and pandas —
parquet preserves types (long, double, timestamp, nested structs), is ~10×
smaller, and reads directly into pandas via PyArrow without any parsing.

In the Databricks notebook, write to S3:
```python
# In the Spark notebook — write result to S3 as parquet
(
    df_result
    .repartition(1)          # or more partitions for large results
    .write
    .mode('overwrite')
    .parquet(f'{str_path_project}/output/my_result')
)
```

Then read locally (see "Reading S3 data with PyArrow" below).

Only use `dataframe_functions.export_df_csv()` when the **final deliverable**
to the client is a CSV/Excel file — not as an intermediate between Spark and
local code.

## Environment setup

The repo contains a `requirements.txt` with all local processing dependencies.
Create the venv once from the repo root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Always activate before running any local script:
```bash
source .venv/bin/activate
```

Never install into system Python. To add a new package:
```bash
source .venv/bin/activate
pip install <package>
pip freeze | grep <package> >> requirements.txt   # capture the pinned version
```
After installing, **self-patch this runbook**: add a row for the new package
(name and purpose) to the "Packages available in .venv" table below, so every
future session knows the capability exists.

## Packages available in .venv

| Package | Purpose |
|---|---|
| `pandas` | DataFrames, pivots, final formatting, Excel/CSV export |
| `pyarrow` | Read parquet from S3 via `S3FileSystem`; fast columnar I/O |
| `boto3` | AWS SDK — credential handling, S3 client |
| `duckdb` | Fast local SQL over remote parquet: GROUP BY, JOIN, window functions |
| `matplotlib` | Static charts (PNG) |
| `plotly` | Interactive charts (HTML) |
| `openpyxl` | `pandas.to_excel()` and windows-1252 CSV for Excel compatibility |
| `tqdm` | Progress bars for long local loops |
| `pyyaml` | YAML parsing for `validate_agent_core.py` and catalog scripts |

## Reading S3 data with PyArrow

```python
import pyarrow.parquet as pq
import pyarrow.fs as pafs

s3 = pafs.S3FileSystem(region="us-east-1")
table = pq.read_table(
    "rads-projects/short_term/2026/2026_INTERNAL_my_project/cache/result",
    filesystem=s3
)
df = table.to_pandas()
```

Requires an active AWS session — see `runbooks/aws-and-s3-runbook.md`.

## DuckDB for local analytics

Prefer DuckDB over pandas for GROUP BY, JOIN, and window operations.

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("CALL load_aws_credentials();")

df = con.sql("""
    SELECT sort_year, COUNT(*) AS n_papers
    FROM read_parquet('s3://rads-projects/short_term/2026/my_project/cache/result/*.parquet')
    GROUP BY sort_year
    ORDER BY sort_year
""").df()
```

`load_aws_credentials()` picks up credentials from the environment — set them
first via `go-aws-sso` if needed.

## Local folder conventions

| Folder | Purpose |
|---|---|
| `./tmp/` | Throwaway artifacts: downloaded HTML, raw parquet copies, intermediate files, decode scripts. Not shared with the user. |
| `./output/` | Deliverables: charts (PNG/HTML), tables (CSV/Excel), any file the user needs. Always report paths here to the user. |

Both folders are git-ignored. Create them as needed:
```bash
mkdir -p ./tmp ./output
```

## Pandas / polars for final formatting

Use pandas for final presentation, pivot tables, and chart data prep:
```python
import pandas as pd

# Read a small local parquet (downloaded to tmp first)
df = pd.read_parquet('./tmp/results.parquet')

# Pivot
pivot = df.pivot_table(index='sort_year', columns='oa_type', values='n_papers', aggfunc='sum')
```

## Matplotlib charts

```python
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(df['sort_year'], df['n_papers'], marker='o')
ax.set_xlabel('Year')
ax.set_ylabel('Papers')
ax.set_title('Annual output')

import os
os.makedirs('output', exist_ok=True)
fig.savefig('output/annual_output.png', dpi=150, bbox_inches='tight')
plt.close()
print('Saved: output/annual_output.png')
```

## Plotly interactive charts

```python
import plotly.express as px

fig = px.line(df, x='sort_year', y='n_papers', title='Annual output')
fig.write_html('output/annual_output.html')
print('Saved: output/annual_output.html')
```

## Excel export (via pandas)

```python
df.to_excel('output/results.xlsx', index=False, engine='openpyxl')
```

For Windows-compatible CSV (encode issues with special chars):
```python
df.to_csv('output/results.csv', index=False, encoding='windows-1252', quoting=1)
```

## Post-processing workflow

Typical sequence after a Databricks notebook writes parquet to S3:

1. Verify the output path has a `_SUCCESS` marker:
   ```bash
   aws s3 ls s3://rads-projects/short_term/2026/my_project/cache/result/_SUCCESS
   ```
2. Read with DuckDB or pyarrow.
3. Run aggregations / joins locally.
4. Generate charts and export to `output/`.
5. Report file paths to the user.

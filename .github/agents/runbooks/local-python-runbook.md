# Local Python Runbook

## Environment setup

Before running any local Python code, ensure the `.venv` exists:
```bash
[[ -d .venv ]] || python3 -m venv .venv
source .venv/bin/activate
pip install pandas pyarrow duckdb matplotlib plotly openpyxl
```

Always activate the venv before running local scripts. Never install into
system Python.

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

## Pandas / polars for final formatting

Use pandas for final presentation, pivot tables, and chart data prep:
```python
import pandas as pd

# Read a small local parquet
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

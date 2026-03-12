# Copilot Instructions — ads-dbr-agentic

This workspace develops PySpark analytics notebooks that run on a remote
**Databricks** cluster. Code is written locally and deployed via the
Databricks CLI. Use these instructions when answering questions about
analytics problems, writing new notebooks, or debugging existing ones.

---

## Environment

| Item | Value |
|---|---|
| Databricks workspace | `https://elsevier-dev.cloud.databricks.com` |
| Databricks CLI version | **v0.282.0** |
| Default Spark library path | `/Workspace/rads/library/` |
| Local library mirror | `./rads_library/` (run `./sync_library.sh` to refresh) |
| Python path | `PYTHONPATH` includes `./rads_library/` (set in `.databricks.env`) |

### Available clusters

| cluster_id | name | notes |
|---|---|---|
| `0107-154653-j5wd510m` | **rads-private-unity** | Default for development; RUNNING |


To use a different cluster, pass `--cluster-id` to `deploy.sh` or set
`DATABRICKS_CLUSTER_ID` in `.databricks/.databricks.env`.

---

## Deploying notebooks

All notebooks are plain `.py` files using Databricks cell separators:

```python
# Databricks notebook source

# COMMAND ----------
# cell 1 code here

# COMMAND ----------
# cell 2 code here
```

### Upload only
```bash
./deploy.sh notebooks/my_analysis.py
# deploys to /Workspace/Users/J.Baas@elsevier.com/my_analysis
```

### Upload and run on default cluster
```bash
./deploy.sh notebooks/my_analysis.py --run
```

### Upload and run on a specific cluster
```bash
./deploy.sh notebooks/my_analysis.py --run --cluster-id 0303-153342-zrgyfy1c
```

### Upload to a custom remote path
```bash
./deploy.sh notebooks/my_analysis.py /Workspace/Users/J.Baas@elsevier.com/subfolder/my_analysis --run
```

### Monitor a running job
`RUN_ID` is a **positional argument** in v0.282.0 (no `--run-id` flag):
```bash
databricks jobs get-run <run_id> -o json
```

### Fetch cell output from a completed run
`databricks jobs get-run-output <run_id>` only returns `dbutils.notebook.exit()` output.
To get full per-cell stdout, use `export-run`:
```bash
databricks jobs export-run <run_id> --views-to-export ALL -o json
```
The response is HTML with a double-encoded notebook model inside a
`__DATABRICKS_NOTEBOOK_MODEL` JS variable. Decode order:
`urllib.parse.unquote` → `base64.b64decode` → `urllib.parse.unquote` → `json.loads`.

### Submit a one-time notebook run (manually, without deploy.sh)
`databricks runs submit` **does not exist** in v0.282.0. Use `databricks jobs submit`
with a `tasks` array (see `deploy.sh` for the authoritative structure):
```bash
databricks jobs submit --no-wait --json '{
  "run_name": "my-run",
  "tasks": [{
    "task_key": "notebook",
    "existing_cluster_id": "<cluster_id>",
    "notebook_task": { "notebook_path": "<remote_path>" }
  }]
}' -o json
# stdout: JSON with run_id
```

### Upload a plain file (e.g. a library file, not a notebook)
```bash
databricks workspace import /Workspace/rads/library/my_file.py \
    --file rads_library/my_file.py --format RAW --overwrite
# NOTE: use --format SOURCE --language PYTHON for notebooks
# NOTE: use --format RAW for plain .py files (library modules)
# IMPORTANT: --format SOURCE with --language PYTHON creates a NOTEBOOK object.
#            --format RAW creates a FILE object. These types cannot be overwritten
#            with each other — the remote type must match.
```

---

## Main data table: ANI (Scopus)

The primary data source is the Scopus Article-level November Index (ANI):

```python
df_ani = spark.table('scopus.ani_20250301')  # snapshot date in YYYYMMDD
# or inside a notebook: table('scopus.ani_20250301')
```

### Key ANI columns

| Column | Type | Description |
|---|---|---|
| `eid` | string | Scopus unique article ID (`2-s2.0-...`) |
| `sort_year` | int | Publication year |
| `source.srcid` | long | Journal source ID (e.g. 16590 = The Lancet) |
| `doi` | string | DOI |
| `citations` | array\<string\> | List of cited EIDs (may have duplicates) |
| `dbcollections` | array\<string\> | Index collections (`SCOPUS`, `MEDL`, etc.) |
| `citation_title` | struct | Array of titles with language tags |
| `Af` | array\<struct\> | Affiliations with `affiliation_ids` |

### Filter out preprints (always apply this)
```python
import column_functions
df = df_ani.filter(column_functions.nopp())
# nopp() = keep only SCOPUS or MEDL indexed docs (excludes preprints)
```

---

## Library reference (`/Workspace/rads/library/`)

Run `./sync_library.sh` to pull the latest versions into `./rads_library/`.

Always import with:
```python
import sys
sys.path.append('/Workspace/rads/library/')
```

### `column_functions`

| Function | Description |
|---|---|
| `nopp()` | Boolean column filter: keeps SCOPUS + MEDL, excludes preprints |
| `normalize_doi(col)` | Lowercase, strip prefix, whitespace for robust DOI matching |
| `long_eid_to_eidstr(col)` | Convert long EID integer → `"2-s2.0-..."` string |
| `long_eid_to_keystr(col)` | Convert long EID → zero-padded 10-char key |
| `eid_to_long(col)` | Parse `"2-s2.0-..."` → long integer |
| `long_eid_to_url(col)` | Build Scopus URL from long EID |
| `get_ani_title_col(col)` | Extract preferred title (English first, then original) |
| `null_if_empty(col)` | Replace blank strings with null |
| `nullsafeflatten(x)` | Flatten nested array, removing nulls |
| `nullsafeconcat(*cols)` | Concat columns treating null/`[]` as empty string |
| `array_contains_other(col, val)` | True if array has any value other than val |
| `regexp_replace_iter(col, patterns, replacements)` | Apply multiple regexp_replace in sequence |

### `dataframe_functions`

| Function | Signature | Description |
|---|---|---|
| `df_cached` | `(df, str_path, format="parquet", partitions=1, ...)` | Write-once cache: saves df to parquet if not present, then reads back. Uses `repartition` (not `coalesce`) to avoid stage fusion. |
| `export_df_csv` | `(df, name, path_storage, compressed=True, partitions=1, excel_format=False)` | Export DataFrame as CSV with friendly filename. `excel_format=True` uses windows-1252 + quoteAll. |
| `export_df_json` | `(df, name, path_storage, compressed=True, partitions=1)` | Export as JSON with friendly filename. |
| `share_dataframe` | `(df, recipient, dataset_name, ...)` | Share a DataFrame with an external recipient. |
| `share_file_path` | `(s3_path, recipient, dataset_name, ...)` | Share an S3 path with a recipient. |
| `check_path_for_completeness` | `(path, show_sub_path=False)` | Validate a cache path has a `_SUCCESS` marker. |
| `df_all_struct_to_json` | `(df)` | Convert all struct/array columns to JSON strings for CSV export. |

**`df_cached` pattern:**
```python
df_result = dataframe_functions.df_cached(
    df.filter(...).select(...),
    '/mnt/els/rads-projects/.../cache/my_step',
    partitions=10,  # use higher values for large datasets
)
```

### `rwdb_functions`

| Function | Description |
|---|---|
| `check_and_fetch_rwdb_csv(folder, max_age_rwdb_days=30)` | Get latest Retraction Watch CSV (fetches from GitLab if cache is older than `max_age_rwdb_days`). Returns local path. |
| `get_rw_scopus_match_df(path_rw_csv, ani_stamp, folder)` | Match all RWDB records to ANI by DOI + title/year. Returns df with `eid`, `Record ID`. |
| `get_clean_rw_df(path_rw_csv, ani_stamp, folder)` | Like above but filters out journal errors, retract-and-replace, and withdrawn-out-of-date. Returns df with `eid` + `isRetracted=True`. |

### `file_functions`

| Function | Description |
|---|---|
| `file_exists(path)` | Check if a DBFS or `/dbfs/` path exists. |
| `rename_single_partition_file(path)` | Rename a single-partition Spark output to a file (not folder). |

### `orgdb_functions`

Organisation hierarchy (OrgDB). Key functions:
- `get_df_hierarchy_selected(orgdb_date, relationships)` — get parent-child hierarchy
- `get_df_generated_mapping_cached(file, orgdb_date, df_institutions, ...)` — cached affiliation → institution mapping

### `scd_functions`

Scopus Citation Database helpers:
- `configure(snapshot)` — set ANI snapshot for subsequent calls
- `get_asjc_name_array(df_ani)` — add ASJC subject area names
- `csv_formatted_from_selected_ani(...)` — format selected ANI columns for CSV export

### `snapshot_functions`

| Function | Description |
|---|---|
| `find_closest_dates(folders, date, ...)` | Find the nearest available snapshot date |
| `get_snapshot_from_folder(folder)` | Parse snapshot date from a folder name |
| `scival.get_table(name, snapshot=...)` | Load a SciVal table (topic_eid, topic_prominence, etc.) |
| `ads.publication.get_table(name, ...)` | Load an ADS publication table |

### `static_data`

Provides lookup lists as lazy-loaded objects:
- `static_data.asjc` — ASJC subject codes + descriptions
- `static_data.doctype` — document type codes
- `static_data.sdg` — UN Sustainable Development Goal codes

### `worldbank_functions`

- `fetch_worldbank_indicator(indicator, start_year, end_year)` — fetch World Bank data
- `get_indicator_with_metadata(indicator, start_year, end_year)` — fetch with country metadata

---

## Storage paths

| Purpose | Base path |
|---|---|
| Temporary (1 day) | `/mnt/els/rads-projects/temporary_to_be_deleted/1d/` |
| Short-term projects | `/mnt/els/rads-projects/short_term/YYYY/<project>/` |
| Retraction Watch cache | `/mnt/els/rads-projects/temporary_to_be_deleted/1d/<project>/rw_cache/` |

---

## Typical notebook structure

```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions

ani_stamp        = '20250301'
str_path_project = '/mnt/els/rads-projects/temporary_to_be_deleted/1d/my_project'
cache_folder     = os.path.join(str_path_project, 'cache')

# COMMAND ----------
df_ani = spark.table(f'scopus.ani_{ani_stamp}')

df_subset = dataframe_functions.df_cached(
    df_ani.filter(column_functions.nopp()).filter(...).select('eid', 'sort_year'),
    os.path.join(cache_folder, 'my_subset'),
)

# COMMAND ----------
df_subset.show()
```

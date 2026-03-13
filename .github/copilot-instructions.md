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
| ADS metrics pipeline code | `./rads_metrics_code/` (run `./sync_metrics_code.sh` to refresh) |

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

## ADS derived metrics pipeline (`snapshot_functions.ads`)

The ADS derived metrics are produced by a **monthly Databricks job** whose full
source code lives at:

> **GitHub:** `https://github.com/elsevier-research/rads-derived-metrics-code`
> **Local mirror:** `./rads_metrics_code/` — run `./sync_metrics_code.sh` to clone/update

The mirror is gitignored and is a shallow clone of the `main` branch.  It
gives the agent direct read access to every production notebook that generates
the tables visible via `snapshot_functions.ads.publication` and
`snapshot_functions.ads.author`.

### When to read the mirror

- **Answering schema questions** about an ADS table: read the corresponding
  notebook in `./rads_metrics_code/` to see exactly what columns are produced.
- **Debugging a join or a metric value**: the notebooks show the full
  computation including normalisation logic, window functions, and edge cases.
- **Checking freshness of reference docs**: the agent-facing table reference
  docs live in `.github/agents/ads-derived/`. If a query or result looks
  inconsistent with those docs, inspect the current notebook source in the
  mirror to check whether the pipeline was updated. Do **not** poll GitHub on
  every request — only re-read the mirror when there is a concrete reason to
  suspect the docs are stale (e.g. a column the docs describe is absent from
  the actual data, or a metric behaves unexpectedly).

### Key files in the mirror

| File | Purpose |
|---|---|
| `AGENTS.md` | Architecture overview, output paths, major metric categories |
| `dbr_job_definition.yaml` | Full pipeline task graph — shows run order and dependencies |
| `_utils.py` | `get_snapshot_parameters()`, `get_table_ani()`, path helpers |
| `*.py` (root) | One notebook per metric — filename = table name |
| `archived/` | Deprecated notebooks — ignore unless explicitly researching history |

### Output paths

- **Publication-level parquet:**
  `/mnt/els/rads-main/mappings_and_metrics/bibliometrics/publication_level/snapshot_metrics/<YYYYMMDD>/<TableName>/`
- **Author-level parquet:**
  `/mnt/els/rads-main/mappings_and_metrics/bibliometrics/author_level/snapshot_metrics/<YYYYMMDD>/<TableName>/`
- **Hive tables:** `fca_ds.<TableName>_<YYYYMMDD>` (Unity Catalog)

Access via `snapshot_functions.ads`:
```python
import snapshot_functions

# List what tables exist in the latest publication snapshot
snapshot_functions.ads.publication.list()

# Load a table
df = snapshot_functions.ads.publication.get_table('Article_Citation_Metrics')

# Author-level
df = snapshot_functions.ads.author.get_table('Author_Info_and_H_Index')
```

---

## Storage paths

| Purpose | Base path |
|---|---|
| Temporary (1 day) | `/mnt/els/rads-projects/temporary_to_be_deleted/1d/` |
| Short-term projects | `/mnt/els/rads-projects/short_term/YYYY/<project>/` |
| Retraction Watch cache | `/mnt/els/rads-projects/temporary_to_be_deleted/1d/<project>/rw_cache/` |

### Databricks mount → S3 mapping

Paths under `/mnt/els/` map directly to S3 buckets in our AWS account.
The following buckets are accessible via the AWS CLI without any extra credentials:

| Databricks path | S3 URI |
|---|---|
| `/mnt/els/rads-projects/…` | `s3://rads-projects/…` |
| `/mnt/els/rads-main/…` | `s3://rads-main/…` |
| `/mnt/els/rads-mappings/…` | `s3://rads-mappings/…` |
| `/mnt/els/rads-pipelines/…` | `s3://rads-pipelines/…` |
| `/mnt/els/rads-users/…` | `s3://rads-users/…` |
| `/mnt/els/rads-restricted/…` | `s3://rads-restricted/…` |

The mapping is: `/mnt/els/{bucket-name}/path` ↔ `s3://{bucket-name}/path`.

**When to use the AWS CLI:**
- Listing or inspecting parquet/CSV files without starting a Spark job
- Checking whether a path exists or has a `_SUCCESS` marker
- Downloading small result files locally for inspection
- Copying outputs between paths

```bash
# List a path
aws s3 ls s3://rads-projects/temporary_to_be_deleted/1d/my_project/

# Check _SUCCESS marker (equivalent to file_functions.file_exists on Databricks)
aws s3 ls s3://rads-main/mappings_and_metrics/bibliometrics/publication_level/snapshot_metrics/20250301/FWCI_All_cits_and_non_self_cits_perc/_SUCCESS

# Download a small CSV
aws s3 cp s3://rads-projects/short_term/2026/my_project/output.csv ./tmp/
```

### EDC — access to external (sister-team) buckets

Data from other Elsevier teams is accessed via the **EDC (Elsevier Data Catalog)**,
managed through Collibra at `https://elsevier.collibra.com/apps/`. We also call
this system "Collibra". Each external dataset has a Collibra page that lists its
Databricks mount point and other metadata.

There are two separate access grants to request — one for Databricks, one for
direct S3/AWS CLI access. Both are filed as access request forms on Collibra.

#### Databricks access request

Grants the Databricks AWS account permission to mount and read the external bucket.

| Field | Value |
|---|---|
| Term | `Long Term` |
| AWS Account Name | `aws-rt-databricks-prod` |
| AWS Account Number | `533013353365` |
| AWS Role Name | `AcademicLeadersFunders-AnalyticalDataServices-dev` |

Once granted, the dataset is accessible under its Databricks mount point
(listed on the Collibra page, typically under `/mnt/els/edc/…`).

#### Direct S3 / AWS CLI access request

Grants our AWS account permission to assume a cross-account role in the target
account to read the bucket directly (without going through Databricks).

| Field | Value |
|---|---|
| Term | `Long Term` |
| AWS Account Name | `Data Science Production` |
| AWS Account Number | `029211843733` |
| AWS Role Name | `ads_crossaccount_data_consumer_role` |

Access uses **role chaining**: first assume `ads_crossaccount_data_consumer_role`
in our account (029211843733), then from that role assume the target dataset role
(communicated by email after the Collibra request is approved).

```bash
# Prerequisites: active AWS session in WSL
# If this returns InvalidClientTokenId or ExpiredToken, run go-aws-sso
# in the WSL terminal to refresh credentials, then retry.
aws sts get-caller-identity --output json

# Step 1: assume our cross-account consumer role
aws sts assume-role \
  --role-arn arn:aws:iam::029211843733:role/ads_crossaccount_data_consumer_role \
  --role-session-name edc-session \
  --output json

# Step 2: export the returned credentials into the shell, then assume the target dataset role
export AWS_ACCESS_KEY_ID=<AccessKeyId from step 1>
export AWS_SECRET_ACCESS_KEY=<SecretAccessKey from step 1>
export AWS_SESSION_TOKEN=<SessionToken from step 1>

aws sts assume-role \
  --role-arn <target_role_arn> \
  --role-session-name edc-data-session \
  --output json

# Step 3: export the second set of credentials, then access the bucket
export AWS_ACCESS_KEY_ID=<AccessKeyId from step 2>
export AWS_SECRET_ACCESS_KEY=<SecretAccessKey from step 2>
export AWS_SESSION_TOKEN=<SessionToken from step 2>

aws s3 ls s3://sccontent-parsed-ani-core-parquet-prod/ --no-sign-request
```

#### Known external datasets

| Dataset | S3 bucket | Databricks mount | Target role ARN |
|---|---|---|---|
| ANI parsed (Scopus ANI core) | `sccontent-parsed-ani-core-parquet-prod` | `/mnt/els/edc/seccont-anicore-parsed-edc` | `arn:aws:iam::838239208477:role/EDC_814132467461_seccont-anicore-parsed-edc-01` |
| ANI raw | `sccontent-ani-parquet-prod` | `/mnt/els/edc/seccont-anicore-raw-edc` | `arn:aws:iam::838239208477:role/EDC_814132467461_seccont-anicore-raw-edc` |

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

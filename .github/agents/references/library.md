# Library Reference — `/Workspace/rads/library/`

Run `./sync_library.sh` to pull the latest versions into `./rads_library/`.

Always import with:
```python
import sys
sys.path.append('/Workspace/rads/library/')
```

---

## `column_functions`

| Function | Signature | Description |
|---|---|---|
| `nopp()` | `() → Column` | Boolean filter: keeps SCOPUS + MEDL indexed docs, excludes preprints. **Always apply as first filter on ANI.** |
| `normalize_doi(col)` | `(Column) → Column` | Lowercase, strip `10.xxx/` prefix edge cases, trim whitespace — for robust DOI matching. |
| `long_eid_to_eidstr(col)` | `(Column) → Column` | Convert long EID integer → `"2-s2.0-..."` string (for SciVal joins). |
| `long_eid_to_keystr(col)` | `(Column) → Column` | Convert long EID → zero-padded 10-char key string. |
| `eid_to_long(col)` | `(Column) → Column` | Parse `"2-s2.0-..."` string → long integer. |
| `long_eid_to_url(col)` | `(Column) → Column` | Build Scopus URL from long EID. |
| `get_ani_title_col(col)` | `(Column) → Column` | Extract preferred title: English first, falls back to original language. |
| `null_if_empty(col)` | `(Column) → Column` | Replace blank strings (`""`) with `null`. |
| `nullsafeflatten(x)` | `(Column) → Column` | Flatten nested array while removing nulls. |
| `nullsafeconcat(*cols)` | `(*Column) → Column` | Concatenate columns treating `null` and `[]` as empty string. |
| `array_contains_other(col, val)` | `(Column, Any) → Column` | True if array contains any value other than `val`. |
| `regexp_replace_iter(col, patterns, replacements)` | `(Column, list, list) → Column` | Apply multiple `regexp_replace` calls in sequence. |

---

## `dataframe_functions`

| Function | Signature | Description |
|---|---|---|
| `df_cached` | `(df, str_path, format="parquet", partitions=1, ...)` | Write-once cache: saves df to parquet if not present, then reads back. Uses `repartition` (not `coalesce`) to avoid stage fusion. |
| `export_df_csv` | `(df, name, path_storage, compressed=True, partitions=1, excel_format=False)` | Export DataFrame as CSV with friendly filename. `excel_format=True` uses windows-1252 + quoteAll for Excel compatibility. |
| `export_df_json` | `(df, name, path_storage, compressed=True, partitions=1)` | Export as newline-delimited JSON with friendly filename. |
| `share_dataframe` | `(df, recipient, dataset_name, ...)` | Share a DataFrame with an external recipient. |
| `share_file_path` | `(s3_path, recipient, dataset_name, ...)` | Share an S3 path with a recipient. |
| `check_path_for_completeness` | `(path, show_sub_path=False)` | Validate that a cache path has a `_SUCCESS` marker. |
| `df_all_struct_to_json` | `(df)` | Convert all struct and array columns to JSON strings — useful before CSV export. |

### `df_cached` usage pattern
```python
df_result = dataframe_functions.df_cached(
    df.filter(...).select(...),
    '/mnt/els/rads-projects/short_term/2026/2026_XXX_project/cache/my_step',
    partitions=10,  # higher for large datasets (>10M rows), lower for small
)
```

---

## `rwdb_functions`

Retraction Watch Database helpers.

| Function | Description |
|---|---|
| `check_and_fetch_rwdb_csv(folder, max_age_rwdb_days=30)` | Get latest Retraction Watch CSV — fetches from GitLab if cache is older than `max_age_rwdb_days`. Returns local path to the CSV. |
| `get_rw_scopus_match_df(path_rw_csv, ani_stamp, folder)` | Match all RWDB records to ANI by DOI first, then title+year. Returns df with `eid` (long) and `Record ID`. |
| `get_clean_rw_df(path_rw_csv, ani_stamp, folder)` | Like `get_rw_scopus_match_df` but additionally filters out: journal errors, expressions of concern, retract-and-replace, withdrawn-out-of-date. Returns df with `eid` + `isRetracted=True`. |

---

## `file_functions`

| Function | Description |
|---|---|
| `file_exists(path)` | Check if a DBFS or `/dbfs/` path exists. Returns bool. |
| `rename_single_partition_file(path)` | Rename a single-partition Spark output (folder) to a single file. |

---

## `orgdb_functions`

Organisation hierarchy (OrgDB). See `references/orgdb.md` for full schema.

| Function | Signature | Description |
|---|---|---|
| `get_last_orgdb_date()` | `() → str` | Returns the most current OrgDB snapshot date (YYYYMMDD). |
| `get_df_hierarchy_selected(orgdb_date, relationships)` | `(str, list) → DataFrame` | Get parent-child hierarchy for specified relationship types. |
| `get_df_generated_mapping_cached(file, orgdb_date, df_institutions, ...)` | `(str, str, DataFrame, ...) → DataFrame` | Cached affiliation ID → institution mapping. |

---

## `scd_functions`

Scopus Citation Database helpers.

| Function | Signature | Description |
|---|---|---|
| `configure(snapshot)` | `(str)` | Set the ANI snapshot for subsequent calls. |
| `get_asjc_name_array(df_ani)` | `(DataFrame) → DataFrame` | Add ASJC subject area name column to ANI slice. |
| `csv_formatted_from_selected_ani(...)` | `(...)` | Format selected ANI columns for CSV export. |

---

## `snapshot_functions`

| Function/Accessor | Description |
|---|---|
| `find_closest_dates(folders, date, ...)` | Find the nearest available snapshot date from a list of folder names. |
| `get_snapshot_from_folder(folder)` | Parse a snapshot date string from a folder path name. |
| `scival.get_table(name, snapshot=...)` | Load a SciVal table by name. See `references/scival.md` for table names. |
| `scival.list()` | List available SciVal table names. |
| `ads.publication.get_table(name, ...)` | Load a publication-level ADS metrics table. |
| `ads.publication.list()` | List available ADS publication table names in the latest snapshot. |
| `ads.author.get_table(name, ...)` | Load an author-level ADS metrics table. |
| `ads.author.list()` | List available ADS author table names. |
| `source.get_table(name, ...)` | Load a source profile table. |
| `patents.get_table(name, ...)` | Load a patents table (`metadata` or `npl_citations_scopus`). |
| `sdg.get_table(with_labels=False, ...)` | Load the SDG classification table. |
| `topic_burst.topic.get_table(year=None)` | Load topic-level burst scores. `year` defaults to latest. Primary key: `TopicID`. |
| `topic_burst.topic.list_snapshots()` | List available analysis years for topic burst. |
| `topic_burst.topic_cluster.get_table(year=None)` | Load topic-cluster-level burst scores. Primary key: `Topic_Cluster`. |
| `topic_burst.topic_cluster.list_snapshots()` | List available analysis years for topic-cluster burst. |

---

## `static_data`

Lookup lists as lazy-loaded objects (no Spark table needed):

| Object | Description |
|---|---|
| `static_data.asjc` | ASJC subject codes + descriptions (pandas DataFrame). |
| `static_data.doctype` | Document type codes (pandas DataFrame). |
| `static_data.sdg` | UN Sustainable Development Goal codes + labels. |

---

## `worldbank_functions`

| Function | Signature | Description |
|---|---|---|
| `fetch_worldbank_indicator(indicator, start_year, end_year)` | `(str, int, int) → DataFrame` | Fetch a World Bank indicator series (e.g. `"SP.POP.TOTL"`). |
| `get_indicator_with_metadata(indicator, start_year, end_year)` | `(str, int, int) → DataFrame` | Same as above but joined with country metadata (region, income group). |

---

## ADS metrics pipeline mirror

The production pipeline source code is mirrored at `./rads_metrics_code/`
(run `./sync_metrics_code.sh` to update). Read notebooks there to verify exact
column definitions, window logic, or edge-case handling.

Key files:
- `AGENTS.md` — architecture overview and table index
- `dbr_job_definition.yaml` — task graph and run order
- `_utils.py` — `get_snapshot_parameters()`, path helpers
- `<TableName>.py` — one notebook per metric

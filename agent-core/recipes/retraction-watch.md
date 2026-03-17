---
name: retraction-watch
triggers:
  - retraction
  - retracted papers
  - retraction watch
  - retraction notice
  - citation shadow
  - post-retraction citations
required_tables:
  - ANI
required_functions:
  - column_functions.nopp
  - dataframe_functions.df_cached
common_outputs:
  - notebook
  - csv
pitfalls:
  - Retraction Watch matching requires DOI or title normalisation
  - Post-retraction citations require ANI citation network traversal
review_checks:
  - verify nopp() on ANI
  - verify correct join strategy for Retraction Watch database
---

# Recipe: Retraction Watch — identify retracted papers in a dataset

## When to use
Any time the analysis involves retracted papers: counting retractions, finding
citing papers, studying citation shadows, or flagging retraction status in results.

## Prerequisites
- `rwdb_functions` module in `rads_library/`
- ANI snapshot stamp

## Notebook template

```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions, rwdb_functions

ani_stamp        = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_my_project'
cache_folder     = os.path.join(str_path_project, 'cache')
rw_cache         = os.path.join(str_path_project, 'rw_cache')

# COMMAND ----------
# Fetch Retraction Watch CSV (cached locally, refreshed if >30 days old)
path_rw_csv = rwdb_functions.check_and_fetch_rwdb_csv(rw_cache)

# Match RWDB to ANI by DOI + title/year; filter junk entries
df_retracted = rwdb_functions.get_clean_rw_df(path_rw_csv, ani_stamp, rw_cache)
# Columns: eid (long), isRetracted (True)
print(f"Retracted papers matched to ANI: {df_retracted.count()}")

# COMMAND ----------
# Load ANI and flag retracted papers
df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())

df_with_flag = df_ani.join(df_retracted, on='Eid', how='left')
df_with_flag = df_with_flag.withColumn(
    'is_retracted', F.coalesce(F.col('isRetracted'), F.lit(False))
)

# COMMAND ----------
# Example: count by year and retraction status
df_counts = dataframe_functions.df_cached(
    df_with_flag.groupBy('sort_year', 'is_retracted').count(),
    os.path.join(cache_folder, 'retraction_counts_by_year'),
    partitions=1,
)
df_counts.orderBy('sort_year', 'is_retracted').show(40)
```

## Output columns
- `Eid` — Scopus EID (long)
- `isRetracted` — True/null from RWDB match
- `is_retracted` — boolean (coalesced to False for non-retracted)

## Common pitfalls
- `citations` in ANI is **outgoing** (what this paper cites), not incoming count.
  To find papers that **cite** a retracted paper, explode `citations` and join
  to the retracted EID list.
- `get_clean_rw_df` excludes journal errors and expressions of concern.
  Use `get_rw_scopus_match_df` instead if you need unfiltered RWDB matches.
- The RWDB cache folder must be persistent (not `temporary_to_be_deleted`) if
  you might re-run the notebook over multiple days.

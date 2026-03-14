# Recipe: Journal Output Trend

## When to use
Counting publications in one or more journals over time, with optional
enrichment (OA type, document type, subject area, citation metrics).

## Prerequisites
- ANI snapshot stamp
- Source profiles (for CiteScore, publisher info)

## Discover a journal's srcid first

If you don't know the `srcid`, add a discovery cell:
```python
spark.table(f'scopus.ani_{ani_stamp}').filter(
    F.lower(F.col('source.sourcetitle')).contains('lancet')
).select('source.srcid', 'source.sourcetitle').distinct().show(truncate=False)
```

## Notebook template

```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions, snapshot_functions

ani_stamp        = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_XXX_journal_trend'
cache_folder     = os.path.join(str_path_project, 'cache')
SRCID            = 16590   # The Lancet — replace with actual srcid

# COMMAND ----------
df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())

df_journal = dataframe_functions.df_cached(
    df_ani.filter(F.col('source.srcid') == SRCID)
          .select(
              'Eid', 'sort_year', 'citation_type',
              F.col('openaccess.flag').alias('oa_flag'),
              F.col('openaccess.oatype').alias('oa_type'),
          ),
    os.path.join(cache_folder, 'journal_papers'),
    partitions=1,
)
print(f"Total papers: {df_journal.count()}")

# COMMAND ----------
# Yearly output breakdown
df_yearly = df_journal.groupBy('sort_year').agg(
    F.count('Eid').alias('n_papers'),
    F.sum(F.when(F.col('oa_flag') == True, 1).otherwise(0)).alias('n_oa'),
    F.round(
        F.sum(F.when(F.col('oa_flag') == True, 1).otherwise(0)) / F.count('Eid') * 100, 1
    ).alias('pct_oa'),
).orderBy('sort_year')
df_yearly.show(40)

# COMMAND ----------
# Optional: enrich with source profile (CiteScore, publisher)
df_source = snapshot_functions.source.get_table('source_profiles')
# Join key: source profile `id` (long) ↔ ANI `source.srcid` (long)
df_meta = df_source.filter(F.col('id') == SRCID).select(
    'id', 'source_title', 'publisher', 'citescore'
)
df_meta.show()

# COMMAND ----------
# Export
dataframe_functions.export_df_csv(
    df_yearly,
    name='journal_yearly_output',
    path_storage=str_path_project,
    excel_format=True,
)
```

## Output columns
- `sort_year` — publication year
- `n_papers` — paper count
- `n_oa` — open access paper count
- `pct_oa` — open access percentage

## Common pitfalls
- `citation_type` values include `ar` (article), `re` (review), `le` (letter),
  `no` (note). Filter to `ar` and `re` for standard article counts.
- OA type (`oa_type`) encodes gold/hybrid/green/bronze — check ANI schema for
  exact string values used in the snapshot you're working with.
- For multi-journal comparisons, keep `source.srcid` in the output and join
  source names from the profiles table.

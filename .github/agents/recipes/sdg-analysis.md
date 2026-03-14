# Recipe: SDG Classification Analysis

## When to use
Identifying papers classified to UN Sustainable Development Goals and
analysing their distribution, citation impact, or trends.

## Prerequisites
- ANI snapshot stamp
- SDG table (`snapshot_functions.sdg`)
- Read `.github/agents/references/sdg.md` for schema details

## Key SDG facts
- ~31M rows, covers 17 SDGs
- `eid` (long) joins directly to ANI `Eid` — no conversion needed
- Only ~22–25% of ANI papers are classified → always LEFT JOIN
- `confidence` is always ≥ 0.95 (pre-filtered in the table)
- A paper can be classified to multiple SDGs

## Notebook template

```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions, snapshot_functions

ani_stamp        = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_XXX_sdg_analysis'
cache_folder     = os.path.join(str_path_project, 'cache')

# COMMAND ----------
# Load SDG table with text labels
df_sdg = snapshot_functions.sdg.get_table(with_labels=True)
# Columns: eid (long), sdg (int 1-17), confidence (float), sdg_label (string)

# COMMAND ----------
df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())
df_target = dataframe_functions.df_cached(
    df_ani.filter(F.col('sort_year').between(2015, 2024))
          .filter(F.col('citation_type').isin('ar', 're'))
          .select('Eid', 'sort_year'),
    os.path.join(cache_folder, 'target'),
    partitions=10,
)

# COMMAND ----------
# Join SDG (each paper can appear multiple times — once per SDG)
df_with_sdg = dataframe_functions.df_cached(
    df_target.join(df_sdg.select('eid', 'sdg', 'sdg_label'), on='Eid', how='left'),
    os.path.join(cache_folder, 'target_sdg'),
    partitions=10,
)

# COMMAND ----------
# Papers with at least one SDG classification
df_classified = df_with_sdg.filter(F.col('sdg').isNotNull())
print(f"Classified papers: {df_classified.select('Eid').distinct().count()}")
print(f"Total papers: {df_target.count()}")

# COMMAND ----------
# SDG distribution by year
df_sdg_dist = dataframe_functions.df_cached(
    df_classified.groupBy('sort_year', 'sdg', 'sdg_label').agg(
        F.countDistinct('Eid').alias('n_papers')
    ).orderBy('sort_year', 'sdg'),
    os.path.join(cache_folder, 'sdg_dist_by_year'),
    partitions=1,
)
df_sdg_dist.show(51)

# COMMAND ----------
dataframe_functions.export_df_csv(
    df_sdg_dist,
    name='sdg_distribution',
    path_storage=str_path_project,
    excel_format=True,
)
```

## Output columns
- `sdg` — integer 1–17 (SDG number)
- `sdg_label` — string label (e.g. `'SDG 7: Affordable and Clean Energy'`)
- `confidence` — classification confidence (≥ 0.95 in filtered table)

## Common pitfalls
- Papers with multiple SDG assignments are counted once per SDG — use
  `countDistinct('Eid')` not `count()` when counting unique papers.
- SDG snapshots are weekly; use `snapshot_functions.find_closest_dates()` if
  you need a specific date rather than the latest.
- Low coverage (~22–25%) means SDG results are not representative of a field's
  full output — always report the classified fraction alongside counts.

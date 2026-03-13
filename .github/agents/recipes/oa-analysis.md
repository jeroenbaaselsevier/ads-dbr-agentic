# Recipe: Open Access Analysis

## When to use
Analysing open access status across a paper set: OA type distribution (gold,
hybrid, green, bronze, closed), trends over time, or OA by subject area.

## Prerequisites
- ANI snapshot stamp (`openaccess` struct is in the main ANI table)

## OA fields in ANI

| Field | Description |
|---|---|
| `openaccess.flag` | Boolean — True if any OA type applies |
| `openaccess.oatype` | String — `'gold'`, `'hybrid'`, `'green'`, `'bronze'`, `null` (closed) |
| `openaccess.oacollaboration` | Whether part of an OA agreement |

## Notebook template

```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions

ani_stamp        = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_XX_oa_analysis'
cache_folder     = os.path.join(str_path_project, 'cache')

# COMMAND ----------
df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())

# Select OA fields from your target set
df_target = dataframe_functions.df_cached(
    df_ani.filter(F.col('sort_year').between(2015, 2024))
          .filter(F.col('citation_type').isin('ar', 're'))   # articles + reviews only
          .select(
              'Eid', 'sort_year',
              F.col('openaccess.flag').alias('is_oa'),
              F.col('openaccess.oatype').alias('oa_type'),
          ),
    os.path.join(cache_folder, 'target_oa'),
    partitions=10,
)

# COMMAND ----------
# OA type distribution by year
df_oa_dist = df_target.groupBy('sort_year', 'oa_type').count()
df_oa_dist = df_oa_dist.withColumn(
    'oa_label',
    F.when(F.col('oa_type') == 'gold', 'Gold')
     .when(F.col('oa_type') == 'hybrid', 'Hybrid')
     .when(F.col('oa_type') == 'green', 'Green')
     .when(F.col('oa_type') == 'bronze', 'Bronze')
     .otherwise('Closed')
)

df_pivot = dataframe_functions.df_cached(
    df_oa_dist.groupBy('sort_year').pivot('oa_label', ['Gold', 'Hybrid', 'Green', 'Bronze', 'Closed'])
              .agg(F.sum('count')).fillna(0),
    os.path.join(cache_folder, 'oa_by_year'),
    partitions=1,
)
df_pivot.orderBy('sort_year').show()

# COMMAND ----------
# Total OA rate by year
df_target.groupBy('sort_year').agg(
    F.count('Eid').alias('n_total'),
    F.sum(F.when(F.col('is_oa') == True, 1).otherwise(0)).alias('n_oa'),
    F.round(F.sum(F.when(F.col('is_oa') == True, 1).otherwise(0)) / F.count('Eid') * 100, 1)
     .alias('pct_oa'),
).orderBy('sort_year').show()

# COMMAND ----------
dataframe_functions.export_df_csv(
    df_pivot.orderBy('sort_year'),
    name='oa_type_by_year',
    path_storage=str_path_project,
    excel_format=True,
)
```

## Common pitfalls
- `oa_type` is `null` for closed-access papers, not `'closed'`. Use
  `F.coalesce(F.col('oa_type'), F.lit('closed'))` when pivoting.
- OA flag and oa_type definitions can change between ANI snapshots as policy
  evolves — note the snapshot date in outputs.
- Filter to document types before calculating rates (`ar`/`re`), otherwise
  editorials and corrections inflate closed-access counts.

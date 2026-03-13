# Recipe: Patents Citation Analysis

## When to use
Identifying which papers in a set are cited by patents (knowledge transfer /
technology translation analysis), or counting patent citations per paper.

## Prerequisites
- ANI snapshot stamp
- Patents tables (`snapshot_functions.patents`)
- Read `.github/agents/references/patents.md` for schema details

## Key patents facts
- `npl_citations_scopus` joins via `eid` (long) = ANI `Eid` (direct match)
- Only ~15% of NPL citations resolve to a Scopus EID — expect low coverage
- `npl_citations_scopus` lags `patents.metadata` by ~5 months
- `patents.metadata` has 174M rows — cache aggressively
- Use `patents.join_npl_citations()` helper to get citations with optional EID

## Notebook template

```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions, snapshot_functions

ani_stamp        = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_XX_patent_citations'
cache_folder     = os.path.join(str_path_project, 'cache')

# COMMAND ----------
df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())
df_target = dataframe_functions.df_cached(
    df_ani.filter(F.col('sort_year').between(2010, 2020))
          .filter(F.col('citation_type').isin('ar', 're'))
          .select('Eid', 'sort_year'),
    os.path.join(cache_folder, 'target'),
    partitions=10,
)

# COMMAND ----------
# Load NPL citations that resolved to Scopus EIDs
df_npl = snapshot_functions.patents.get_table('npl_citations_scopus')
# Columns: patent_id, eid (long = Scopus EID), npl_text, pub_date, ...

# Cache patents table — it is large
df_npl_cached = dataframe_functions.df_cached(
    df_npl.select('patent_id', 'eid').filter(F.col('eid').isNotNull()),
    os.path.join(cache_folder, 'npl_with_eid'),
    partitions=20,
)

# COMMAND ----------
# Count patent citations received by each paper
df_patent_cit_counts = dataframe_functions.df_cached(
    df_npl_cached.groupBy('eid').agg(
        F.countDistinct('patent_id').alias('n_patent_citations')
    ).withColumnRenamed('eid', 'Eid'),
    os.path.join(cache_folder, 'patent_cit_counts'),
    partitions=5,
)

# COMMAND ----------
# Join to target papers
df_result = dataframe_functions.df_cached(
    df_target.join(df_patent_cit_counts, on='Eid', how='left')
             .fillna(0, subset=['n_patent_citations'])
             .withColumn('is_cited_by_patent', F.col('n_patent_citations') > 0),
    os.path.join(cache_folder, 'target_with_patent_cits'),
    partitions=5,
)

# COMMAND ----------
# Summary by year
df_result.groupBy('sort_year').agg(
    F.count('Eid').alias('n_papers'),
    F.sum(F.when(F.col('is_cited_by_patent'), 1).otherwise(0)).alias('n_cited_by_patent'),
    F.round(
        F.sum(F.when(F.col('is_cited_by_patent'), 1).otherwise(0)) /
        F.count('Eid') * 100, 1
    ).alias('pct_cited_by_patent'),
    F.round(F.avg('n_patent_citations'), 2).alias('avg_patent_citations'),
).orderBy('sort_year').show()

# COMMAND ----------
dataframe_functions.export_df_csv(
    df_result.select('Eid', 'sort_year', 'n_patent_citations', 'is_cited_by_patent'),
    name='patent_citation_analysis',
    path_storage=str_path_project,
    excel_format=True,
)
```

## Output columns
- `n_patent_citations` — number of distinct patents citing this paper
- `is_cited_by_patent` — boolean
- `patent_id` — patent identifier (in npl_citations_scopus table)

## Common pitfalls
- `npl_citations_scopus` lags by ~5 months. Recent publications (within 5 months
  of the latest snapshot) will be undercounted.
- Coverage is ~15% — most papers are NOT cited by patents. This is normal.
- Patent citations accumulate slowly (years, not months). Short publication
  windows (< 5 years) will show very low patent citation rates.
- Cache `npl_citations_scopus` before joining — it is very large (39M rows).
  Without caching, repeated joins will re-scan the full table.

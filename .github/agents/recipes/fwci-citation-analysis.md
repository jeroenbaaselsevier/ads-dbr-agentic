# Recipe: FWCI and Citation Impact Analysis

## When to use
Benchmarking a paper set's citation impact using field-weighted citation impact
(FWCI) and citation percentiles from the ADS derived metrics tables.

## Prerequisites
- ADS metrics tables available (`snapshot_functions.ads`)
- ANI snapshot stamp
- Read `.github/agents/references/ads-derived/README.md` for full table index

## Notebook template

```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions, snapshot_functions

ani_stamp        = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_XXX_fwci_analysis'
cache_folder     = os.path.join(str_path_project, 'cache')

# COMMAND ----------
# Load your paper set from ANI
df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())
df_target = dataframe_functions.df_cached(
    df_ani.filter(F.col('source.srcid') == 16590)   # e.g. The Lancet
          .select('Eid', 'sort_year', 'source.srcid'),
    os.path.join(cache_folder, 'target_papers'),
    partitions=1,
)

# COMMAND ----------
# Load FWCI — all citations, no window restriction
df_fwci = snapshot_functions.ads.publication.get_table('FWCI_All_cits_and_non_self_cits')
# Columns include: EID, FWCI, FWCI_no_self_cits - see ads-derived reference for full schema

# COMMAND ----------
# Join FWCI to target papers
df_result = dataframe_functions.df_cached(
    df_target.join(df_fwci.select('EID', 'FWCI', 'FWCI_no_self_cits'), on='Eid', how='left'),
    os.path.join(cache_folder, 'target_with_fwci'),
    partitions=1,
)

# COMMAND ----------
# Summary statistics
df_result.groupBy('sort_year').agg(
    F.count('Eid').alias('n_papers'),
    F.round(F.avg('FWCI'), 2).alias('mean_fwci'),
    F.round(F.percentile_approx('FWCI', 0.5), 2).alias('median_fwci'),
    F.round(F.sum(F.when(F.col('FWCI') > 1, 1).otherwise(0)) / F.count('Eid') * 100, 1)
     .alias('pct_above_world_avg'),
).orderBy('sort_year').show()

# COMMAND ----------
# Optional: load citation percentile table for finer benchmarking
df_pct = snapshot_functions.ads.publication.get_table('Citation_Percentile_ASJC27')
# Columns: EID, ASJC27_field, percentile_rank (0-100)
```

## Output columns
- `FWCI` — field-weighted citation impact (1.0 = world average)
- `FWCI_no_self_cits` — FWCI excluding self-citations
- Citation percentiles available in `Citation_Percentile_ASJC27` and `Citation_Percentile_ASJC334`

## Common pitfalls
- ADS tables include preprints; ANI filtered with `nopp()` does not. When joining,
  you may lose ~1-2% of ADS rows that correspond to preprints not in the ANI
  `nopp()` subset. LEFT JOIN and report unmatched count.
- FWCI is `null` for papers with fewer than 3 citations or in the first 4 years
  after publication (insufficient citation window).
- Always check the ADS snapshot date matches your ANI stamp — use
  `snapshot_functions.ads.publication.list()` to verify available snapshots.

# Recipe: Collaboration Analysis

## When to use
Measuring international or institutional collaboration levels for a paper set:
what fraction of papers have co-authors from multiple countries or institutions.

## Prerequisites
- ANI snapshot stamp
- OrgDB (for institution-level collaboration using `orgdb_functions`)
- ADS collaboration tables (for pre-computed SciVal-based collaboration flags)
- Read `.github/agents/references/orgdb.md` for OrgDB schema/functions

## Approach A — ADS pre-computed collaboration flags (fast)

```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions, snapshot_functions

ani_stamp        = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_XX_collab'
cache_folder     = os.path.join(str_path_project, 'cache')

# COMMAND ----------
df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())
df_target = dataframe_functions.df_cached(
    df_ani.filter(F.col('sort_year').between(2019, 2024))
          .select('Eid', 'sort_year'),
    os.path.join(cache_folder, 'target'),
    partitions=5,
)

# COMMAND ----------
# Load ADS collaboration table (SciVal-based: single/national/international)
df_collab = snapshot_functions.ads.publication.get_table('Collaboration_SciVal')
# Columns: EID, collab_level ('Single institution', 'National', 'International')

df_result = dataframe_functions.df_cached(
    df_target.join(df_collab.select('EID', 'collab_level'), on='Eid', how='left'),
    os.path.join(cache_folder, 'target_collab'),
    partitions=1,
)

df_result.groupBy('sort_year', 'collab_level').count().orderBy('sort_year', 'collab_level').show()
```

## Approach B — OrgDB-based institution collaboration (custom, slower)

```python
# COMMAND ----------
import orgdb_functions

orgdb_date = orgdb_functions.get_last_orgdb_date()

# Explode affiliations and cast afid to string for OrgDB join
df_afids = df_target.join(
    df_ani.select('Eid', F.explode('Af').alias('af')).select(
        'Eid',
        F.explode('af.affiliation_ids').cast('string').alias('org_id')
    ),
    on='Eid',
    how='inner',
)

# Get institution hierarchy
df_hierarchy = orgdb_functions.get_df_hierarchy_selected(
    orgdb_date,
    relationships=['institution_to_country']
)

# Join afids to countries via OrgDB
df_countries = df_afids.join(
    df_hierarchy.select('org_id', 'country_name'),
    on='org_id',
    how='left',
)

# Count distinct countries per paper
df_intl = dataframe_functions.df_cached(
    df_countries.groupBy('Eid').agg(
        F.countDistinct('country_name').alias('n_countries')
    ).withColumn(
        'is_international',
        F.col('n_countries') > 1
    ),
    os.path.join(cache_folder, 'intl_collab'),
    partitions=5,
)
```

## Common pitfalls
- `afid` in ANI is a `long` inside an array; cast to `string` before joining OrgDB.
- Some `org_id` values in ANI have no OrgDB match — always LEFT JOIN and report
  unmatched fraction.
- OrgDB is updated daily — use `orgdb_functions.get_last_orgdb_date()` rather
  than hardcoding a date.
- Papers with a single affiliation (solo authors) count as "single institution"
  even if the author is at an international organisation.

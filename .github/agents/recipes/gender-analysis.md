# Recipe: Gender Analysis

## When to use
Analysing author gender distributions across a paper set using the ADS
Genderize/NamSor gender assignment tables.

## Prerequisites
- ADS author tables (`snapshot_functions.ads.author`)
- ANI snapshot stamp
- Access to `rads-restricted` bucket (for raw NamSor output, if needed)
- Read `.github/agents/references/ads-derived/author/` for gender table schemas

## Notebook template

```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions, snapshot_functions

ani_stamp        = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_XX_gender_analysis'
cache_folder     = os.path.join(str_path_project, 'cache')

# COMMAND ----------
# Load your paper set
df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())

df_target = dataframe_functions.df_cached(
    df_ani.filter(F.col('sort_year').between(2020, 2024))
          .select('Eid', 'sort_year', F.explode('Au').alias('author'))
          .select('Eid', 'sort_year', F.col('author.auid').alias('auid')),
    os.path.join(cache_folder, 'target_author_eids'),
    partitions=10,
)

# COMMAND ----------
# Load gender breakdown table from ADS
# Returns one row per EID with counts of male/female/unknown first authors
df_gender = snapshot_functions.ads.publication.get_table('gender_breakdown_by_source')
# Or for author-level gender:
# df_gender_author = snapshot_functions.ads.author.get_table('Genderize_Authors')

# COMMAND ----------
# Join gender to paper set
df_result = dataframe_functions.df_cached(
    df_target.join(df_gender.select('EID', 'first_author_gender',
                                     'n_male_authors', 'n_female_authors', 'n_unknown_authors'),
                   on='Eid', how='left'),
    os.path.join(cache_folder, 'target_with_gender'),
    partitions=5,
)

# COMMAND ----------
# Summary: proportion of female first authors by year
df_result.groupBy('sort_year').agg(
    F.count('Eid').alias('n_papers'),
    F.round(
        F.sum(F.when(F.col('first_author_gender') == 'female', 1).otherwise(0)) /
        F.sum(F.when(F.col('first_author_gender').isin('male', 'female'), 1).otherwise(0)) * 100,
        1
    ).alias('pct_female_first_author'),
).orderBy('sort_year').show()
```

## Output columns
- `first_author_gender` — `'male'`, `'female'`, `'unknown'`, or null
- `n_male_authors`, `n_female_authors`, `n_unknown_authors` — counts across all authors

## Common pitfalls
- Gender data is in `rads-restricted` — access requires appropriate permissions.
- `unknown` gender (cannot be assigned) is typically 15–25% of authors; exclude
  from percentages unless reporting unknown rate explicitly.
- Do not combine NamSor and Genderize results without checking the ADS README —
  the pipeline uses one method per snapshot.
- Author deduplication: the same `auid` may appear multiple times in a paper (e.g.
  as first and corresponding author). Deduplicate before counting unique authors.

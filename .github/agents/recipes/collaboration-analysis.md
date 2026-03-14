# Recipe: Collaboration Analysis

## When to use
Measuring collaboration levels for a paper set: what fraction of papers are
single-author, institutional, national, or international. Also sector-mix
analysis (academic/corporate/government/medical) and per-country breakdown.

## Tables available

| Table | Institution source | Primary key | Status |
|---|---|---|---|
| `Article_Collaboration_orgdb` | OrgDB | `eid` (long, lowercase) | **Preferred** |
| `Article_Collaboration` | SciVal institution metadata | `EID` (long, uppercase) | Legacy |

Both tables work the same way conceptually: they roll affiliation IDs up to an
institutional level, then compute `CollaborationLevel`, country counts, and
sector flags. The difference is the mapping source:

- **`Article_Collaboration_orgdb`** (preferred) — uses the OrgDB institution
  hierarchy. OrgDB covers significantly more institutions than SciVal's metadata,
  so fewer affiliations end up `INDETERMINATE`.
- **`Article_Collaboration`** (legacy) — uses SciVal institution metadata. Kept
  for backwards compatibility and SciVal-specific institution IDs.

**Both tables have `CollaborationLevel`, `DocCountryCount`, sector flags, and
country already computed** — do not join OrgDB hierarchy or SciVal institution
metadata on top of them; that work is already done inside the pipeline.

Schemas: see `.github/agents/references/ads-derived/publication/Article_Collaboration_orgdb-reference.md`
and `Article_Collaboration-reference.md`.

## Notebook template — collaboration level by year

```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions, snapshot_functions

ani_stamp        = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_XXX_collab'
cache_folder     = os.path.join(str_path_project, 'cache')

# COMMAND ----------
df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())
df_target = dataframe_functions.df_cached(
    df_ani.filter(F.col('sort_year').between(2019, 2024))
          .filter(F.col('citation_type').isin('ar', 're'))
          .select('Eid', 'sort_year'),
    os.path.join(cache_folder, 'target'),
    partitions=5,
)

# COMMAND ----------
# Load the preferred collaboration table — CollaborationLevel, DocCountryCount,
# and sector flags are pre-computed. No further hierarchy join needed.
df_collab = snapshot_functions.ads.publication.get_table('Article_Collaboration_orgdb')
# Columns: eid, CollaborationLevel, DocCountryCount, docAfCount,
#          docUniqueAuidCount, Acad, Corp, Govt, Med, Other, org (array)

# COMMAND ----------
df_result = dataframe_functions.df_cached(
    df_target.join(
        df_collab.select('eid', 'CollaborationLevel', 'DocCountryCount',
                         'docAfCount', 'Acad', 'Corp', 'Govt', 'Med', 'Other')
                 .withColumnRenamed('eid', 'Eid'),
        on='Eid',
        how='left',
    ),
    os.path.join(cache_folder, 'target_collab'),
    partitions=2,
)

# COMMAND ----------
# Collaboration level distribution by year
collab_levels = ['SINGLE_AUTHOR', 'INSTITUTIONAL', 'NATIONAL', 'INTERNATIONAL', 'INDETERMINATE']

df_result.groupBy('sort_year').pivot('CollaborationLevel', collab_levels).count() \
    .fillna(0).orderBy('sort_year').show()

# COMMAND ----------
# International collaboration rate by year
df_result.groupBy('sort_year').agg(
    F.count('Eid').alias('n_papers'),
    F.sum(F.when(F.col('CollaborationLevel') == 'INTERNATIONAL', 1).otherwise(0))
     .alias('n_international'),
    F.round(
        F.sum(F.when(F.col('CollaborationLevel') == 'INTERNATIONAL', 1).otherwise(0)) /
        F.count('Eid') * 100, 1
    ).alias('pct_international'),
).orderBy('sort_year').show()
```

## Notebook template — per-institution/country breakdown (explode org)

```python
# COMMAND ----------
# Explode the org array to get one row per institution per paper.
# org already contains country, name, sector — no OrgDB join needed.
df_collab_full = snapshot_functions.ads.publication.get_table('Article_Collaboration_orgdb')

df_inst_rows = dataframe_functions.df_cached(
    df_target.join(
        df_collab_full.select(
            F.col('eid').alias('Eid'),
            F.explode('org').alias('inst'),
        ),
        on='Eid',
        how='left',
    ).select(
        'Eid', 'sort_year',
        F.col('inst.org_id').alias('inst_id'),
        F.col('inst.name').alias('inst_name'),
        F.col('inst.country').alias('country'),
        F.col('inst.sector').alias('sector'),
    ),
    os.path.join(cache_folder, 'target_inst_rows'),
    partitions=10,
)

# Top 20 countries by distinct paper count
df_inst_rows.groupBy('country').agg(
    F.countDistinct('Eid').alias('n_papers')
).orderBy(F.col('n_papers').desc()).show(20)
```

## Key output columns

| Column | Values |
|---|---|
| `CollaborationLevel` | `SINGLE_AUTHOR`, `INSTITUTIONAL`, `NATIONAL`, `INTERNATIONAL`, `INDETERMINATE` |
| `DocCountryCount` | int — number of distinct countries on the paper |
| `docAfCount` | int — number of distinct institutions on the paper |
| `Acad`, `Corp`, `Govt`, `Med`, `Other` | boolean sector flags (non-exclusive) |
| `org[].country` | Country (string, from OrgDB) |
| `org[].sector` | `Academic`, `Corporate`, `Government`, `Medical`, `Other` |

## Common pitfalls
- `INDETERMINATE` means afids were present but could not be resolved to any
  institution. `Article_Collaboration_orgdb` has fewer of these than the legacy
  table because OrgDB covers more institutions.
- Sector flags are non-exclusive — a paper with academic and corporate authors
  has both `Acad=true` and `Corp=true`.
- Primary key in `Article_Collaboration_orgdb` is `eid` (lowercase). Rename to
  `Eid` before joining to ANI, or use `.withColumnRenamed('eid', 'Eid')`.
- In the legacy `Article_Collaboration` table the key is `EID` (uppercase) and
  the institution array column is `m_af` (not `org`).


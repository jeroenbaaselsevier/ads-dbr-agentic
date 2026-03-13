# Recipe: SciVal Topic Analysis

## When to use
Identifying which SciVal research topics a set of papers belongs to,
analysing topic prominence scores, or mapping a portfolio's topic distribution.

## Prerequisites
- ANI snapshot stamp
- SciVal tables (`snapshot_functions.scival`)
- Read `.github/agents/references/scival.md` for full table schemas and topic hierarchy

## Key SciVal facts
- EID join: `topic_eid.EidString` = `"2-s2.0-..."` string form of ANI `Eid`
- Must convert: `column_functions.long_eid_to_eidstr(ANI.Eid)` before joining
- Not all ANI EIDs appear in `topic_eid` → LEFT JOIN
- Topics roll up to topic clusters (broader areas)

## Notebook template

```python
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions, snapshot_functions

ani_stamp        = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_XX_topic_analysis'
cache_folder     = os.path.join(str_path_project, 'cache')

# COMMAND ----------
df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())

# Convert EID to string form for SciVal join
df_target = dataframe_functions.df_cached(
    df_ani.filter(F.col('sort_year').between(2019, 2024))
          .filter(F.col('citation_type').isin('ar', 're'))
          .select(
              'Eid', 'sort_year',
              column_functions.long_eid_to_eidstr(F.col('Eid')).alias('EidString'),
          ),
    os.path.join(cache_folder, 'target_with_eid_str'),
    partitions=10,
)

# COMMAND ----------
# Load topic membership table
df_topic_eid = snapshot_functions.scival.get_table('topic_eid')
# Columns: EidString, topic_id (int)

# Load topic metadata (prominence, cluster, name)
df_topic_prom = snapshot_functions.scival.get_table('topic_prominence')
# Columns: topic_id, topic_name, topic_cluster_id, prominence_percentile, ...

# COMMAND ----------
# Join papers to topics
df_with_topic = dataframe_functions.df_cached(
    df_target.join(df_topic_eid.select('EidString', 'topic_id'), on='EidString', how='left'),
    os.path.join(cache_folder, 'target_topics'),
    partitions=10,
)

# Enrich with topic metadata
df_result = dataframe_functions.df_cached(
    df_with_topic.join(
        df_topic_prom.select('topic_id', 'topic_name', 'topic_cluster_id', 'prominence_percentile'),
        on='topic_id',
        how='left',
    ),
    os.path.join(cache_folder, 'target_topics_meta'),
    partitions=5,
)

# COMMAND ----------
# Top 20 topics by paper count
df_result.filter(F.col('topic_id').isNotNull()) \
    .groupBy('topic_id', 'topic_name', 'prominence_percentile').agg(
        F.countDistinct('Eid').alias('n_papers')
    ).orderBy(F.col('n_papers').desc()).show(20, truncate=False)

# COMMAND ----------
# Topic distribution and average prominence
df_topic_summary = dataframe_functions.df_cached(
    df_result.filter(F.col('topic_id').isNotNull())
             .groupBy('topic_id', 'topic_name').agg(
                 F.countDistinct('Eid').alias('n_papers'),
                 F.round(F.avg('prominence_percentile'), 1).alias('avg_prominence_pct'),
             ).orderBy(F.col('n_papers').desc()),
    os.path.join(cache_folder, 'topic_summary'),
    partitions=1,
)
dataframe_functions.export_df_csv(
    df_topic_summary,
    name='topic_distribution',
    path_storage=str_path_project,
    excel_format=True,
)
```

## Output columns
- `topic_id` — integer topic ID
- `topic_name` — string topic label
- `prominence_percentile` — 0–100, where 100 = highest prominence
- `topic_cluster_id` — parent cluster ID

## Common pitfalls
- `EidString` must be in `"2-s2.0-XXXXXXXXXX"` format — use
  `long_eid_to_eidstr()`, not a manual string concatenation.
- Not all topics have prominence scores — null values indicate unscored topics.
- A paper can appear in multiple topics — use `countDistinct('Eid')` not `count()`.
- Topic assignments change between SciVal snapshot years — specify a snapshot
  to ensure consistency across analyses.

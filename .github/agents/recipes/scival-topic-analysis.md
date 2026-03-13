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

---

## Burst Scores

### What they are
Burst scores measure whether a topic's **prominence percentile is accelerating**
beyond its recent weighted trend.  They are calculated once a year (June) by the
RADS operations team and live in S3 as pre-computed parquet files.

The algorithm (decay = 0.8, 6-year window):
1. For each topic, collect the annual prominence time-series over `year-6 … year-1`.
2. Compute a **decayed weighted average** (recent years count more, weight = 0.8^age).
3. `burstScore = (current_year_prominence - weighted_avg) / weighted_std`
4. The same is done for raw **output (publication count)** — giving two independent scores.

A `burstScore > 2` indicates the topic is rising significantly beyond its trend.

### Available data
| Sub-table | Granularity | Key column | S3 path |
|---|---|---|---|
| `topic_burst.topic` | SciVal topic | `TopicID` | `/mnt/els/rads-mappings/burst_analysis/topics/<year>/` |
| `topic_burst.topic_cluster` | Topic cluster | `Topic_Cluster` | `/mnt/els/rads-mappings/burst_analysis/clusters/<year>/` |

### Output schema (both tables)
| Column | Type | Description |
|---|---|---|
| `TopicID` / `Topic_Cluster` | long | Primary key |
| `Output_by_year` | map<long,float> | Publication count per year |
| `Prominence_by_year` | map<long,float> | Raw prominence score per year |
| `PromPerc_by_year` | map<long,float> | Prominence percentile per year |
| `Prominence_Rank_by_Year` | map<long,float> | Rank among all topics per year |
| `Prominence_ordered` | array<float> | Prominence time series (chronological) |
| `Output_ordered` | array<float> | Output count time series (chronological) |
| `Burst_Stats_Prominence` | struct | `{obs, average, variance, burstScore, std}` on prominence |
| `Burst_Stats_Output` | struct | `{obs, average, variance, burstScore, std}` on output count |

### Usage

```python
import snapshot_functions as sf

# List available analysis years
sf.topic_burst.topic.list_snapshots()          # e.g. [2023, 2024]

# Load topic-level burst scores (latest year)
df_burst_topic = sf.topic_burst.topic.get_table()

# Load for a specific year
df_burst_topic = sf.topic_burst.topic.get_table(2024)

# Load topic-cluster-level burst scores
df_burst_cluster = sf.topic_burst.topic_cluster.get_table(2024)
```

### Joining burst scores to an analysis

```python
# Get topics for a portfolio, then attach burst scores
df_with_burst = (
    df_result                       # from the template above — has topic_id col
    .filter(F.col('topic_id').isNotNull())
    .join(
        df_burst_topic.select(
            F.col('TopicID').alias('topic_id'),
            F.col('Burst_Stats_Prominence.burstScore').alias('burst_prominence'),
            F.col('Burst_Stats_Output.burstScore').alias('burst_output'),
        ),
        on='topic_id',
        how='left',
    )
)

# Top 20 bursting topics (by prominence burst score)
df_with_burst.orderBy(F.col('burst_prominence').desc()).show(20)
```

### Gotchas
- Data is **only refreshed once a year in June** — confirm the analysis year
  matches the snapshot you want (`list_snapshots()` shows what's available).
- The topic-to-topic-cluster mapping changes rarely (only when SciVal adds new
  topics).  The notebooks always pick the newest relevant cluster mapping.
- `burstScore` can be `null` if a topic has no prominence variance across the
  window (constant or missing data).
- There is **no single snapshot combining topics + clusters** — join them
  via the `topic_to_topic_cluster` SciVal table if you need both levels.

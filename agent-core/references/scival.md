# SciVal Tables Reference — `snapshot_functions.scival`

This document describes the SciVal tech-data tables accessible via
`snapshot_functions.scival`. It is referenced by the **analyst** agent.

SciVal data provides research-topic taxonomy, prominence scores, article-level
usage (views), and institution groupings that complement the Scopus ANI table.

---

## API overview

```python
import snapshot_functions

# List all table names
snapshot_functions.scival.list_tables()
# → ['institution', 'institution_metadata', 'topic_eid', 'topic_keywords',
#    'topic_prominence', 'topic_topiccluster', 'topiccluster_keywords',
#    'topiccluster_prominence', 'views']

# List available snapshots for a table
snapshot_functions.scival.list('topic_eid')       # → ['2025-03-18', ..., '2026-03-09']

# Load latest available snapshot (closest to today)
df = snapshot_functions.scival.get_table('topic_eid')

# Load a specific date (falls back to closest available)
df = snapshot_functions.scival.get_table('topic_eid', snapshot='20260301')
```

The `get_table()` call automatically selects the closest snapshot to the
requested date and prints which snapshot it chose.

---

## Table inventory

| Table | Rows | Latest snapshot | Update frequency |
|---|---|---|---|
| `topic_eid` | 121,343,200 | 2026-03-09 | ~weekly |
| `topic_keywords` | 94,151 | 2024-05-21 | infrequent |
| `topic_prominence` | 94,128 | 2025-06-17 | ~every 6 months |
| `topic_topiccluster` | 94,151 | 2024-05-21 | infrequent |
| `topiccluster_keywords` | 1,529 | 2024-05-21 | infrequent |
| `topiccluster_prominence` | 1,529 | 2025-06-17 | ~every 6 months |
| `views` | 498,303,033 | 2026-03-12 | daily |
| `institution` | 57,904 | 2026-03-09 | ~weekly |
| `institution_metadata` | 24,814 | 2026-03-05 | ~weekly |

---

## Topic taxonomy

SciVal groups publications into **~94,000 Topics** clustered into **~1,500 Topic Clusters**.
The hierarchy is: EID → Topic → Topic Cluster.

Topics are micro-level research fronts (e.g. "Contact Angle / Hydrophobicity /
Superhydrophobic"). Topic Clusters are broader meta-fields grouping related topics.

---

## Table details

### `topic_eid` — EID to Topic mapping

**121.3M rows.** The largest and most frequently updated SciVal table.
One row per document–topic assignment.

| Column | Type | Description |
|---|---|---|
| `TopicId` | long | SciVal Topic ID |
| `EidString` | string | EID in string form: `"2-s2.0-NNNNNNNNNN"` |

> **Key join note:** `EidString` uses the **string EID format** (`"2-s2.0-..."`),
> not the numeric `Eid` long from ANI. Convert before joining:
>
> ```python
> import column_functions, snapshot_functions
> from pyspark.sql import functions as F
>
> df_te = snapshot_functions.scival.get_table('topic_eid')
>
> # Option A: convert ANI Eid → string for join
> df_ani_with_topic = (
>     df_ani
>     .filter(column_functions.nopp())
>     .withColumn('EidString', column_functions.long_eid_to_eidstr(F.col('Eid')))
>     .join(df_te, 'EidString', 'left')  # LEFT JOIN — not all papers have a topic
> )
>
> # Option B: convert topic EidString → long for join
> df_te_long = df_te.withColumn('Eid', column_functions.eid_to_long(F.col('EidString')))
> df_ani_with_topic = df_ani.join(df_te_long.drop('EidString'), 'Eid', 'left')
> ```

Not all ANI EIDs are assigned to a topic — very recent papers, some book
chapters, and certain document types may be unassigned. Always LEFT JOIN.

---

### `topic_prominence` — Topic-level prominence metrics

**94,128 rows** (one per topic). Snapshot cadence: roughly every 6 months.
Latest: 2025-06-17.

| Column | Type | Description |
|---|---|---|
| `TopicId` | long | SciVal Topic ID |
| `Prominence` | double | Prominence score (log-scale composite of citation momentum + views) |
| `Rank` | int | Rank by Prominence (1 = most prominent) |
| `ProminenceP` | double | Percentile (0–100); 100 = top topic |
| `citationcount` | double | Total citations received by topic papers (recent window) |
| `views` | double | Total abstract+full-text views |
| `avgcitescore` | double | Average CiteScore of journals publishing in this topic |

**Prominence score stats (2025-06-17):** min = -1.93, mean = -1.16, max = 3.75.
Top-ranked topic (rank 1) has prominence 3.75. Topics below ~0 are declining fields.

```python
df_tp = snapshot_functions.scival.get_table('topic_prominence')

# Top 20 most prominent topics
df_tp.orderBy(F.asc('Rank')).show(20)

# Topics above 99th percentile
df_tp.filter(F.col('ProminenceP') >= 99.0).count()
```

---

### `topic_keywords` — Top keywords per topic

**94,151 rows** (one per topic, grouped). Returns keywords already aggregated
into a `Keywords` array sorted by rank. Snapshot cadence: infrequent;
latest is 2024-05-21.

| Column | Type | Description |
|---|---|---|
| `TopicId` | long | SciVal Topic ID |
| `Keywords` | array\<struct\> | `{KeywordRank: long, Keyword: string}` — sorted ascending by rank |

```python
df_kw = snapshot_functions.scival.get_table('topic_keywords')

# Get top-3 keywords per topic as a string label
df_topic_label = df_kw.select(
    'TopicId',
    F.array_join(
        F.slice(F.col('Keywords.Keyword'), 1, 3),
        ' / '
    ).alias('topic_label')
)
```

To get the raw (ungrouped) keyword rows instead:
```python
df_kw_raw = snapshot_functions.scival.get_table('topic_keywords', raw=True)
# Columns: TopicId, KeywordRank, Keyword
```

---

### `topic_topiccluster` — Topic to Topic Cluster mapping

**94,151 rows.** One row per topic. Maps each `TopicId` to its `TopicClusterId`.

| Column | Type | Description |
|---|---|---|
| `TopicId` | long | SciVal Topic ID |
| `TopicClusterId` | long | SciVal Topic Cluster ID |

```python
df_ttc = snapshot_functions.scival.get_table('topic_topiccluster')
```

---

### `topiccluster_prominence` — Topic Cluster prominence metrics

**1,529 rows** (one per topic cluster). Same schema as `topic_prominence`.

| Column | Type | Description |
|---|---|---|
| `TopicClusterId` | long | SciVal Topic Cluster ID |
| `Prominence` | double | Cluster prominence score |
| `Rank` | int | Rank (1 = most prominent) |
| `ProminenceP` | double | Prominence percentile (0–100) |
| `citationcount` | double | Total citations for cluster papers |
| `views` | double | Total views |
| `avgcitescore` | double | Average CiteScore of journals in cluster |

---

### `topiccluster_keywords` — Top keywords per topic cluster

**1,529 rows** (one per cluster, grouped). Same shape as `topic_keywords`.

| Column | Type | Description |
|---|---|---|
| `TopicClusterId` | long | SciVal Topic Cluster ID |
| `Keywords` | array\<struct\> | `{KeywordRank: long, Keyword: string}` sorted by rank |

---

### `views` — Article-level usage (abstract views + outward links)

**498M rows.** Updated daily. One row per (EID, year) pair.

| Column | Type | Description |
|---|---|---|
| `eid` | long | Numeric EID (same type as ANI `Eid`) — **no string conversion needed** |
| `usageyear` | long | Calendar year of usage |
| `AbstractViews` | double | Number of abstract page views |
| `OutwardLinks` | double | Number of outward link clicks (full-text referrals) |

> Unlike `topic_eid`, the `views` table uses the **numeric `eid`** (long) — joins
> directly to `Eid` in ANI without conversion.

```python
df_views = snapshot_functions.scival.get_table('views')

# Total views per paper across all years
df_total_views = (
    df_views
    .groupBy('eid')
    .agg(
        F.sum('AbstractViews').alias('total_abstract_views'),
        F.sum('OutwardLinks').alias('total_outward_links'),
    )
)

# Join to ANI (left join — not all papers have usage data)
df_enriched = df_ani.join(df_total_views, F.col('Eid') == F.col('eid'), 'left')
```

---

### `institution` — SciVal institution → afid mapping

**57,904 rows** (one row per institution–afid pair; one institution can map to
multiple afids). 24,814 distinct `institution_id`s, 51,559 distinct `afid`s.

| Column | Type | Description |
|---|---|---|
| `institution_id` | long | SciVal institution ID |
| `institution_name` | string | SciVal institution display name (may contain encoding artifacts — see note) |
| `afid` | long | Scopus Affiliation ID (matches `Af[*].afid` in ANI, `org_id` in OrgDB) |
| `afid_name` | string | Name of the specific afid |

> **Encoding note:** `institution_name` in this table sometimes has Unicode
> encoding artifacts (e.g. `"UniversitÃ© de YaoundÃ© I"` instead of
> `"Université de Yaoundé I"`). Use `institution_metadata.institutionName` for
> clean display names.

This table is useful for mapping publications from any `afid` to its SciVal
`institution_id`, which is the grouping unit used in SciVal's UI.

```python
df_inst = snapshot_functions.scival.get_table('institution')

# Map ANI afids to SciVal institution IDs
df_ani_afids = (
    df_ani
    .select(F.explode('Af').alias('af'))
    .select(F.col('af.afid').alias('afid'))
    .distinct()
)

df_with_inst = df_ani_afids.join(df_inst.select('afid', 'institution_id'), 'afid', 'left')
```

---

### `institution_metadata` — SciVal institution details

**24,814 rows** (one per `institution_id`). Provides clean names, country,
geolocation, type, and region.

| Column | Type | Description |
|---|---|---|
| `institutionId` | long | SciVal institution ID (joins to `institution.institution_id`) |
| `institutionName` | string | Clean institution name |
| `acronym` | string | Abbreviation (may be empty) |
| `instType` | string | Institution type (e.g. `"Corporate"`, `"University"`) |
| `countryCodeIso2` | string | ISO 2-letter country code (lowercase, e.g. `"us"`) |
| `countryCodeIso3` | string | ISO 3-letter country code (lowercase, e.g. `"usa"`) |
| `countryName` | string | Full country name |
| `region` | string | World region code: `"EUR"`, `"NAM"`, `"APAC"`, etc. |
| `latitude` | double | Latitude |
| `longitude` | double | Longitude |
| `nameVariant1` | string | Alternative name (may be empty) |
| `nameVariant2` | string | Alternative name (may be empty) |

---

## Common multi-table patterns

### Get topic label + prominence for a set of papers

```python
ani_stamp = '20260301'
df_ani_core = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())

df_te  = snapshot_functions.scival.get_table('topic_eid')
df_tp  = snapshot_functions.scival.get_table('topic_prominence')
df_tkw = snapshot_functions.scival.get_table('topic_keywords')

# Build topic label from top-3 keywords
df_topic_label = df_tkw.select(
    'TopicId',
    F.array_join(F.slice(F.col('Keywords.Keyword'), 1, 3), ' / ').alias('topic_label')
)

df_paper_topics = (
    df_ani_core
    .withColumn('EidString', column_functions.long_eid_to_eidstr(F.col('Eid')))
    .join(df_te, 'EidString', 'left')
    .join(df_tp.select('TopicId', 'Rank', 'ProminenceP'), 'TopicId', 'left')
    .join(df_topic_label, 'TopicId', 'left')
)
```

### Roll up EID → Topic → Topic Cluster

```python
df_ttc = snapshot_functions.scival.get_table('topic_topiccluster')
df_tcp = snapshot_functions.scival.get_table('topiccluster_prominence')
df_tckw = snapshot_functions.scival.get_table('topiccluster_keywords')

df_cluster_label = df_tckw.select(
    'TopicClusterId',
    F.array_join(F.slice(F.col('Keywords.Keyword'), 1, 3), ' / ').alias('cluster_label')
)

df_with_cluster = (
    df_paper_topics  # from previous example
    .join(df_ttc, 'TopicId', 'left')
    .join(df_tcp.select('TopicClusterId', F.col('Rank').alias('ClusterRank'), F.col('ProminenceP').alias('ClusterProminenceP')), 'TopicClusterId', 'left')
    .join(df_cluster_label, 'TopicClusterId', 'left')
)
```

---

## Join key summary

| SciVal table | SciVal column | ANI column | Cast needed? |
|---|---|---|---|
| `topic_eid` | `EidString` (string `"2-s2.0-..."`) | `Eid` (long) | Yes — use `column_functions.long_eid_to_eidstr(F.col('Eid'))` |
| `views` | `eid` (long) | `Eid` (long) | No |
| `institution` | `afid` (long) | `Af[*].afid` (long) | No |
| `institution` | `institution_id` | `institution_metadata.institutionId` | No |

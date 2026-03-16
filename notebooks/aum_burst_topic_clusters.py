# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions, snapshot_functions

ani_stamp        = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_KW_aum_burst'
cache_folder     = os.path.join(str_path_project, 'cache')

# AUM = American University of the Middle East (Kuwait)
# OrgDB org_id 60105846 — single afid, no sub-units
AUM_AFID = '60105846'

# COMMAND ----------
# MAGIC %md ## Step 1 — Get all AUM-authored papers (2020–2024)

# COMMAND ----------
df_ani_raw = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())

df_aum_papers = dataframe_functions.df_cached(
    df_ani_raw
    .filter(F.col('sort_year').between(2020, 2024))
    .filter(F.col('citation_type').isin('ar', 're', 'cp'))
    .select('Eid', 'sort_year', F.explode('Af').alias('af'))
    .filter(F.col('af.afid').cast('string') == AUM_AFID)
    .select('Eid', 'sort_year')
    .distinct()
    .withColumn('EidString', column_functions.long_eid_to_eidstr(F.col('Eid'))),
    os.path.join(cache_folder, 'aum_papers_2020_2024'),
    partitions=5,
)

aum_total = df_aum_papers.count()
print(f"Total AUM papers 2020-2024: {aum_total}")
df_aum_papers.groupBy('sort_year').count().orderBy('sort_year').show()

# COMMAND ----------
# MAGIC %md ## Step 2 — Load SciVal topic → topic cluster mapping

# COMMAND ----------
df_topic_eid = snapshot_functions.scival.get_table('topic_eid')

df_topic_cluster_map = (
    snapshot_functions.scival.get_table('topic_topiccluster')
    .select('TopicId', F.col('TopicClusterId').alias('Topic_Cluster'))
)

df_cluster_keywords = snapshot_functions.scival.get_table('topiccluster_keywords')
df_cluster_prominence = snapshot_functions.scival.get_table('topiccluster_prominence')

# COMMAND ----------
# MAGIC %md ## Step 3 — Assign each AUM paper to a topic cluster

# COMMAND ----------
df_aum_with_cluster = dataframe_functions.df_cached(
    df_aum_papers
    .join(df_topic_eid.select('EidString', 'TopicId'), on='EidString', how='left')
    .join(df_topic_cluster_map, on='TopicId', how='left')
    .select('Eid', 'sort_year', 'Topic_Cluster'),
    os.path.join(cache_folder, 'aum_papers_with_cluster'),
    partitions=5,
)

n_matched = df_aum_with_cluster.filter(F.col('Topic_Cluster').isNotNull()).count()
print(f"Papers with topic cluster assignment: {n_matched} / {aum_total}")

# COMMAND ----------
# MAGIC %md ## Step 4 — Load global burst scores (topic cluster level)

# COMMAND ----------
df_burst = snapshot_functions.topic_burst.topic_cluster.get_table()
df_burst.printSchema()

# COMMAND ----------
# MAGIC %md ## Step 5 — AUM paper count per topic cluster

# COMMAND ----------
df_aum_cluster_counts = dataframe_functions.df_cached(
    df_aum_with_cluster
    .filter(F.col('Topic_Cluster').isNotNull())
    .groupBy('Topic_Cluster')
    .agg(F.countDistinct('Eid').alias('aum_papers')),
    os.path.join(cache_folder, 'aum_cluster_counts'),
    partitions=1,
)
print(f"Distinct topic clusters with AUM papers: {df_aum_cluster_counts.count()}")

# COMMAND ----------
# MAGIC %md ## Step 6 — Global Scopus paper count per topic cluster (2020–2024)
# MAGIC This is the denominator for computing AUM's share.

# COMMAND ----------
df_global_cluster_counts = dataframe_functions.df_cached(
    df_ani_raw
    .filter(F.col('sort_year').between(2020, 2024))
    .filter(F.col('citation_type').isin('ar', 're', 'cp'))
    .select('Eid', column_functions.long_eid_to_eidstr(F.col('Eid')).alias('EidString'))
    .join(df_topic_eid.select('EidString', 'TopicId'), on='EidString', how='inner')
    .join(df_topic_cluster_map, on='TopicId', how='inner')
    .groupBy('Topic_Cluster')
    .agg(F.countDistinct('Eid').alias('global_papers')),
    os.path.join(cache_folder, 'global_cluster_counts_2020_2024'),
    partitions=10,
)

# COMMAND ----------
# MAGIC %md ## Step 7 — Assemble final result table

# COMMAND ----------
# Top-3 keywords as cluster label
# Keywords is ARRAY<STRUCT<KeywordRank: BIGINT, Keyword: STRING>> — extract string field first
df_cluster_label = (
    df_cluster_keywords
    .select(
        F.col('TopicClusterId').alias('Topic_Cluster'),
        F.array_join(
            F.slice(F.transform(F.col('Keywords'), lambda kw: kw['Keyword']), 1, 3),
            ', '
        ).alias('cluster_label'),
    )
)

df_result = dataframe_functions.df_cached(
    df_aum_cluster_counts
    .join(df_global_cluster_counts, on='Topic_Cluster', how='left')
    .join(
        df_cluster_prominence.select(
            F.col('TopicClusterId').alias('Topic_Cluster'),
            'Prominence', 'Rank', 'ProminenceP',
        ),
        on='Topic_Cluster', how='left',
    )
    .join(
        df_burst.select(
            'Topic_Cluster',
            F.col('Burst_Stats_Prominence.burstScore').alias('burst_prominence'),
            F.col('Burst_Stats_Output.burstScore').alias('burst_output'),
            'PromPerc_by_year',
        ),
        on='Topic_Cluster', how='left',
    )
    .join(df_cluster_label, on='Topic_Cluster', how='left')
    .withColumn(
        'aum_share_pct',
        F.round(F.col('aum_papers') / F.col('global_papers') * 100, 4),
    )
    .orderBy(F.col('aum_papers').desc()),
    os.path.join(cache_folder, 'aum_burst_result'),
    partitions=1,
)

# COMMAND ----------
# Preview: top 30 by AUM paper count
display(
    df_result.select(
        'Topic_Cluster', 'cluster_label', 'aum_papers', 'global_papers',
        'aum_share_pct', 'ProminenceP', 'Rank', 'burst_prominence', 'burst_output',
    ).orderBy(F.col('aum_papers').desc()).limit(30)
)

# COMMAND ----------
# Preview: top 30 by global prominence
display(
    df_result.select(
        'Topic_Cluster', 'cluster_label', 'aum_papers', 'global_papers',
        'aum_share_pct', 'ProminenceP', 'Rank', 'burst_prominence', 'burst_output',
    ).orderBy(F.col('ProminenceP').desc()).limit(30)
)

# COMMAND ----------
# MAGIC %md ## Step 8 — Export CSVs

# COMMAND ----------
# Drop PromPerc_by_year (MAP type — not CSV-compatible; keep in parquet cache only)
df_export = df_result.drop('PromPerc_by_year')

# Full table — every cluster where AUM has ≥1 paper
dataframe_functions.export_df_csv(
    df_export,
    name='aum_burst_all_clusters',
    path_storage=str_path_project,
    excel_format=True,
)

# Top 100 by prominence (for "Top 20 prominent" chart)
dataframe_functions.export_df_csv(
    df_export.orderBy(F.col('ProminenceP').desc()).limit(100),
    name='aum_burst_top100_prominent',
    path_storage=str_path_project,
    excel_format=True,
)

# Top 100 by burst score (for "Top 20 trending" chart)
dataframe_functions.export_df_csv(
    df_export
    .filter(F.col('burst_prominence').isNotNull())
    .orderBy(F.col('burst_prominence').desc())
    .limit(100),
    name='aum_burst_top100_trending',
    path_storage=str_path_project,
    excel_format=True,
)

print("=== DONE (cluster level) ===")
print(f"Exports written to: {str_path_project}")
print(f"Total AUM papers 2020-2024: {aum_total}")
df_result.agg(F.sum('aum_papers').alias('papers_assigned_to_clusters')).show()

# Also export top 100 by AUM share (for "highest share" chart — slide 2 equivalent)
dataframe_functions.export_df_csv(
    df_export.filter(F.col('ProminenceP') >= 50)
             .orderBy(F.col('aum_share_pct').desc()).limit(100),
    name='aum_burst_top100_highshare',
    path_storage=str_path_project,
    excel_format=True,
)
print("Exported top100_highshare")

# COMMAND ----------
# MAGIC %md ## Step 9 — Topic-level analysis (finer-grained, ~96k topics)

# COMMAND ----------
df_topic_keywords   = snapshot_functions.scival.get_table('topic_keywords')
df_topic_prominence = snapshot_functions.scival.get_table('topic_prominence')
df_burst_topic      = snapshot_functions.topic_burst.topic.get_table()
print("topic_burst.topic schema:")
df_burst_topic.printSchema()

# COMMAND ----------
# AUM papers joined to topic (TopicId level)
df_aum_with_topic = dataframe_functions.df_cached(
    df_aum_papers
    .join(df_topic_eid.select('EidString', 'TopicId'), on='EidString', how='left')
    .select('Eid', 'sort_year', 'TopicId'),
    os.path.join(cache_folder, 'aum_papers_with_topic'),
    partitions=5,
)
n_topic = df_aum_with_topic.filter(F.col('TopicId').isNotNull()).count()
print(f"Papers with topic assignment: {n_topic} / {aum_total}")

# COMMAND ----------
# AUM paper count per topic
df_aum_topic_counts = dataframe_functions.df_cached(
    df_aum_with_topic
    .filter(F.col('TopicId').isNotNull())
    .groupBy('TopicId')
    .agg(F.countDistinct('Eid').alias('aum_papers')),
    os.path.join(cache_folder, 'aum_topic_counts'),
    partitions=1,
)
print(f"Distinct topics with AUM papers: {df_aum_topic_counts.count()}")

# COMMAND ----------
# Global Scopus paper count per topic (2020–2024)
df_global_topic_counts = dataframe_functions.df_cached(
    df_ani_raw
    .filter(F.col('sort_year').between(2020, 2024))
    .filter(F.col('citation_type').isin('ar', 're', 'cp'))
    .select('Eid', column_functions.long_eid_to_eidstr(F.col('Eid')).alias('EidString'))
    .join(df_topic_eid.select('EidString', 'TopicId'), on='EidString', how='inner')
    .groupBy('TopicId')
    .agg(F.countDistinct('Eid').alias('global_papers')),
    os.path.join(cache_folder, 'global_topic_counts_2020_2024'),
    partitions=10,
)

# COMMAND ----------
# Topic label: top 3 keywords (also ARRAY<STRUCT<KeywordRank, Keyword>>)
df_topic_label = (
    df_topic_keywords
    .select(
        'TopicId',
        F.array_join(
            F.slice(F.transform(F.col('Keywords'), lambda kw: kw['Keyword']), 1, 3),
            ', '
        ).alias('topic_label'),
    )
)

df_result_topic = dataframe_functions.df_cached(
    df_aum_topic_counts
    .join(df_global_topic_counts, on='TopicId', how='left')
    .join(
        df_topic_prominence.select('TopicId', 'Prominence', 'Rank', 'ProminenceP'),
        on='TopicId', how='left',
    )
    .join(
        df_burst_topic.select(
            F.col('TopicID').alias('TopicId'),
            F.col('Burst_Stats_Prominence.burstScore').alias('burst_prominence'),
            F.col('Burst_Stats_Output.burstScore').alias('burst_output'),
        ),
        on='TopicId', how='left',
    )
    .join(df_topic_label, on='TopicId', how='left')
    .withColumn('aum_share_pct', F.round(F.col('aum_papers') / F.col('global_papers') * 100, 4))
    .orderBy(F.col('aum_papers').desc()),
    os.path.join(cache_folder, 'aum_burst_result_topic'),
    partitions=1,
)

# COMMAND ----------
display(
    df_result_topic.select(
        'TopicId', 'topic_label', 'aum_papers', 'global_papers',
        'aum_share_pct', 'ProminenceP', 'Rank', 'burst_prominence', 'burst_output',
    ).orderBy(F.col('aum_papers').desc()).limit(30)
)

# COMMAND ----------
# MAGIC %md ## Step 10 — Export topic-level CSVs

# COMMAND ----------
dataframe_functions.export_df_csv(
    df_result_topic,
    name='aum_burst_all_topics',
    path_storage=str_path_project,
    excel_format=True,
)
dataframe_functions.export_df_csv(
    df_result_topic.orderBy(F.col('ProminenceP').desc()).limit(100),
    name='aum_burst_top100_topics_prominent',
    path_storage=str_path_project,
    excel_format=True,
)
dataframe_functions.export_df_csv(
    df_result_topic.filter(F.col('ProminenceP') >= 50)
                   .orderBy(F.col('aum_share_pct').desc()).limit(100),
    name='aum_burst_top100_topics_highshare',
    path_storage=str_path_project,
    excel_format=True,
)
dataframe_functions.export_df_csv(
    df_result_topic.filter(F.col('burst_prominence').isNotNull())
                   .orderBy(F.col('burst_prominence').desc()).limit(100),
    name='aum_burst_top100_topics_trending',
    path_storage=str_path_project,
    excel_format=True,
)
print("=== DONE (topic level) ===")
df_result_topic.agg(F.sum('aum_papers').alias('papers_in_topics')).show()

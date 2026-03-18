# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import os
import sys

sys.path.append('/Workspace/rads/library/')
import dataframe_functions

# COMMAND ----------
ani_stamp = '20250801'

stanford_base_path = 'dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801'
path_career_top_cited = os.path.join(
    stanford_base_path,
    'Table-S1_20250801_1960_1996_2024_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2024.parquet',
)

str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_journal_author_ranking'
cache_folder = os.path.join(str_path_project, 'cache')
output_folder = os.path.join(str_path_project, 'output')

TOP_Y = 15
WINDOWS = [
    {'window_start': y - 2, 'window_end': y, 'window_id': f'w3_{y-2}_{y}'}
    for y in range(2017, 2025)
]

# COMMAND ----------
# First publication year from Stanford career table.
df_author_first_year = (
    spark.read.format('parquet').load(path_career_top_cited)
    .select(
        F.col('author_id').cast('long').alias('auid'),
        F.col('firstPubYear').cast('int').alias('first_pub_year'),
    )
    .dropDuplicates(['auid'])
)

print(f'Authors with first_pub_year: {df_author_first_year.count():,}')

# COMMAND ----------
# Load top-author cache outputs produced by trend notebook per 3-year window.
df_age_rows = None

for w in WINDOWS:
    ys = w['window_start']
    ye = w['window_end']
    wid = w['window_id']
    path_top = os.path.join(cache_folder, f'{wid}_top_authors_y{TOP_Y}')

    df_top = (
        spark.read.format('parquet').load(path_top)
        .select(
            F.col('srcid').cast('long').alias('srcid'),
            'sourcetitle',
            F.col('auid').cast('long').alias('auid'),
            F.col('n_pubs').cast('int').alias('n_pubs'),
            F.col('rownum').cast('int').alias('rownum'),
            F.col('author_rank_ns').cast('long').alias('author_rank_ns'),
        )
        .withColumn('window_id', F.lit(wid))
        .withColumn('window_start', F.lit(ys))
        .withColumn('window_end', F.lit(ye))
    )

    df_w = (
        df_top.join(df_author_first_year, ['auid'], 'left')
        .withColumn(
            'career_age_at_window_end',
            F.when(F.col('first_pub_year').isNull(), F.lit(None)).otherwise(F.col('window_end') - F.col('first_pub_year')),
        )
        .withColumn(
            'career_age_valid',
            F.when((F.col('career_age_at_window_end') >= 0) & (F.col('career_age_at_window_end') <= 120), F.col('career_age_at_window_end')).otherwise(F.lit(None)),
        )
    )

    if df_age_rows is None:
        df_age_rows = df_w
    else:
        df_age_rows = df_age_rows.unionByName(df_w)

# COMMAND ----------
# Journal-window level age summary for top-15.
df_journal_window_age = (
    df_age_rows.groupBy('window_id', 'window_start', 'window_end', 'srcid', 'sourcetitle')
    .agg(
        F.avg('career_age_valid').alias('mean_top15_career_age'),
        F.expr('percentile_approx(career_age_valid, 0.5)').alias('median_top15_career_age'),
        F.count('career_age_valid').alias('n_top15_with_age'),
        F.count('*').alias('n_top15_selected'),
    )
)

# Window-level age trend summary.
df_window_age_summary = (
    df_journal_window_age.groupBy('window_id', 'window_start', 'window_end')
    .agg(
        F.avg('mean_top15_career_age').alias('avg_of_journal_mean_age'),
        F.expr('percentile_approx(mean_top15_career_age, 0.5)').alias('median_of_journal_mean_age'),
        F.avg(F.when(F.col('n_top15_selected') > 0, F.col('n_top15_with_age') / F.col('n_top15_selected')).otherwise(F.lit(None))).alias('avg_age_coverage'),
        F.count('*').alias('n_journals'),
    )
    .orderBy('window_end')
)

# COMMAND ----------
# Join with primary indicator trend for side-by-side interpretation.
df_primary_scores = (
    spark.read.csv(
        os.path.join(output_folder, 'journal_author_rankings_2026', 'journal_window_scores_primary_w3_2017_2024.csv.gz'),
        header=True,
    )
    .select(
        F.col('window_id'),
        F.col('window_start').cast('int').alias('window_start'),
        F.col('window_end').cast('int').alias('window_end'),
        F.col('srcid').cast('long').alias('srcid'),
        F.col('topY_median_rank_ns').cast('double').alias('topY_median_rank_ns'),
    )
)

df_window_score_summary = (
    df_primary_scores.groupBy('window_id', 'window_start', 'window_end')
    .agg(
        F.expr('percentile_approx(topY_median_rank_ns, 0.5)').alias('median_journal_score_primary'),
        F.avg('topY_median_rank_ns').alias('mean_journal_score_primary'),
        F.count('*').alias('n_journals_score'),
    )
)

df_age_score_joined = (
    df_window_age_summary.join(df_window_score_summary, ['window_id', 'window_start', 'window_end'], 'inner')
    .orderBy('window_end')
)

# COMMAND ----------
export_base = os.path.join(output_folder, 'journal_author_rankings_2026')

dataframe_functions.export_df_csv(
    df_journal_window_age,
    'journal_window_top15_career_age_w3_2017_2024',
    export_base,
    compressed=True,
    partitions=1,
)

dataframe_functions.export_df_csv(
    df_window_age_summary,
    'window_top15_career_age_summary_w3_2017_2024',
    export_base,
    compressed=True,
    partitions=1,
)

dataframe_functions.export_df_csv(
    df_age_score_joined,
    'window_top15_career_age_vs_score_summary_w3_2017_2024',
    export_base,
    compressed=True,
    partitions=1,
)

# COMMAND ----------
df_age_score_joined.display()

# COMMAND ----------
note_lines = [
    '# Career-Age Trend Diagnostic for Top-15 Authors',
    '',
    'Purpose:',
    '- Evaluate whether changes in top-15 author career age by window help explain score drift over time.',
    '',
    'Definition:',
    '- career_age_at_window_end = window_end_year - firstPubYear (from Stanford career table).',
    '- Computed for top-15 prolific authors selected per journal-window.',
    '',
    'Outputs:',
    '- journal_window_top15_career_age_w3_2017_2024.csv.gz',
    '- window_top15_career_age_summary_w3_2017_2024.csv.gz',
    '- window_top15_career_age_vs_score_summary_w3_2017_2024.csv.gz',
]

dbutils.fs.put(
    os.path.join(export_base, 'career_age_trend_note.md'),
    '\n'.join(note_lines),
    True,
)

print('S3 output folder:')
print('s3://' + export_base.replace('dbfs:/', '').replace('/mnt/els/', ''))

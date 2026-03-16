# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import os
import sys

sys.path.append('/Workspace/rads/library/')
import snapshot_functions
import dataframe_functions

# COMMAND ----------
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
# Stanford first publication year reference.
df_stanford_first = (
    spark.read.format('parquet').load(path_career_top_cited)
    .select(
        F.col('author_id').cast('long').alias('auid'),
        F.col('firstPubYear').cast('int').alias('stanford_first_pub_year'),
    )
    .dropDuplicates(['auid'])
)

# COMMAND ----------
# Collect top-15 authors per window from trend cache outputs.
df_top_all = None
for w in WINDOWS:
    wid = w['window_id']
    ys = w['window_start']
    ye = w['window_end']
    path_top = os.path.join(cache_folder, f'{wid}_top_authors_y{TOP_Y}')

    df_w = (
        spark.read.format('parquet').load(path_top)
        .select(
            F.col('srcid').cast('long').alias('srcid'),
            F.col('sourcetitle'),
            F.col('auid').cast('long').alias('auid'),
            F.col('rownum').cast('int').alias('rownum'),
            F.col('n_pubs').cast('int').alias('n_pubs'),
            F.col('author_rank_ns').cast('long').alias('author_rank_ns'),
        )
        .withColumn('window_id', F.lit(wid))
        .withColumn('window_start', F.lit(ys))
        .withColumn('window_end', F.lit(ye))
    )

    if df_top_all is None:
        df_top_all = df_w
    else:
        df_top_all = df_top_all.unionByName(df_w)

# COMMAND ----------
# Use the requested ADS author table directly.
selected_table = 'Author_Info_and_H_Index'
df_ads_author = snapshot_functions.ads.author.get_table(selected_table)
cols_ads = set(df_ads_author.columns)

if 'auid' not in cols_ads:
    raise ValueError("Expected column 'auid' not found in Author_Info_and_H_Index")

if 'First_year_in_scopus' in cols_ads:
    selected_year_col = 'First_year_in_scopus'
elif 'first_year_in_scopus' in cols_ads:
    selected_year_col = 'first_year_in_scopus'
else:
    raise ValueError("Expected first-year column not found in Author_Info_and_H_Index")

selected_id_col = 'auid'
print(f'Selected ADS author table: {selected_table}')
print(f'Selected ADS ID column: {selected_id_col}')
print(f'Selected ADS first-year column: {selected_year_col}')

# COMMAND ----------
# Build ADS first-year lookup.
df_ads_first = (
    df_ads_author
    .select(
        F.col(selected_id_col).cast('long').alias('auid'),
        F.col(selected_year_col).cast('int').alias('ads_first_pub_year'),
    )
    .dropDuplicates(['auid'])
)

# COMMAND ----------
# Join and compute null-rate diagnostics by window.
df_joined = (
    df_top_all
    .join(df_stanford_first, ['auid'], 'left')
    .join(df_ads_first, ['auid'], 'left')
    .withColumn(
        'stanford_first_valid',
        F.when((F.col('stanford_first_pub_year') >= 1800) & (F.col('stanford_first_pub_year') <= 2026), F.col('stanford_first_pub_year')).otherwise(F.lit(None)),
    )
    .withColumn(
        'ads_first_valid',
        F.when((F.col('ads_first_pub_year') >= 1800) & (F.col('ads_first_pub_year') <= 2026), F.col('ads_first_pub_year')).otherwise(F.lit(None)),
    )
)

df_null_rates = (
    df_joined.groupBy('window_id', 'window_start', 'window_end')
    .agg(
        F.count('*').alias('n_top15_rows'),
        F.countDistinct('auid').alias('n_distinct_auid'),
        F.sum(F.col('stanford_first_valid').isNotNull().cast('int')).alias('stanford_nonnull_rows'),
        F.sum(F.col('ads_first_valid').isNotNull().cast('int')).alias('ads_nonnull_rows'),
        F.sum((F.col('stanford_first_valid').isNull() & F.col('ads_first_valid').isNotNull()).cast('int')).alias('stanford_null_ads_nonnull_rows'),
        F.sum((F.col('stanford_first_valid').isNotNull() & F.col('ads_first_valid').isNull()).cast('int')).alias('stanford_nonnull_ads_null_rows'),
        F.sum((F.col('stanford_first_valid').isNotNull() & F.col('ads_first_valid').isNotNull()).cast('int')).alias('both_nonnull_rows'),
        F.expr('percentile_approx(CASE WHEN stanford_first_valid IS NOT NULL AND ads_first_valid IS NOT NULL THEN ABS(stanford_first_valid - ads_first_valid) END, 0.5)').alias('median_abs_year_diff_both_nonnull'),
    )
    .withColumn('stanford_null_rate', 1 - F.col('stanford_nonnull_rows') / F.col('n_top15_rows'))
    .withColumn('ads_null_rate', 1 - F.col('ads_nonnull_rows') / F.col('n_top15_rows'))
    .orderBy('window_end')
)

# COMMAND ----------
# Also compute per-window age trend from ADS first year for direct comparison.
df_age_trend_ads = (
    df_joined
    .withColumn('ads_career_age', F.when(F.col('ads_first_valid').isNotNull(), F.col('window_end') - F.col('ads_first_valid')).otherwise(F.lit(None)))
    .withColumn('stanford_career_age', F.when(F.col('stanford_first_valid').isNotNull(), F.col('window_end') - F.col('stanford_first_valid')).otherwise(F.lit(None)))
    .groupBy('window_id', 'window_start', 'window_end')
    .agg(
        F.avg('ads_career_age').alias('avg_ads_career_age_top15_rows'),
        F.expr('percentile_approx(ads_career_age, 0.5)').alias('median_ads_career_age_top15_rows'),
        F.avg('stanford_career_age').alias('avg_stanford_career_age_top15_rows'),
        F.expr('percentile_approx(stanford_career_age, 0.5)').alias('median_stanford_career_age_top15_rows'),
    )
    .orderBy('window_end')
)

# COMMAND ----------
export_base = os.path.join(output_folder, 'journal_author_rankings_2026')

dataframe_functions.export_df_csv(
    df_null_rates,
    'window_top15_firstyear_null_rates_ads_vs_stanford_w3_2017_2024',
    export_base,
    compressed=True,
    partitions=1,
)

dataframe_functions.export_df_csv(
    df_age_trend_ads,
    'window_top15_career_age_ads_vs_stanford_w3_2017_2024',
    export_base,
    compressed=True,
    partitions=1,
)

# COMMAND ----------
# Short human-readable note.
note_lines = [
    '# ADS vs Stanford first-year null-rate diagnostic',
    '',
    f'Selected ADS author table: {selected_table}',
    f'Selected ADS ID column: {selected_id_col}',
    f'Selected ADS first-year column: {selected_year_col}',
    '',
    'Outputs:',
    '- window_top15_firstyear_null_rates_ads_vs_stanford_w3_2017_2024.csv.gz',
    '- window_top15_career_age_ads_vs_stanford_w3_2017_2024.csv.gz',
]

dbutils.fs.put(
    os.path.join(export_base, 'ads_firstyear_nullcheck_note.md'),
    '\n'.join(note_lines),
    True,
)

# COMMAND ----------
df_null_rates.display()
df_age_trend_ads.display()

print('S3 output folder:')
print('s3://' + export_base.replace('dbfs:/', '').replace('/mnt/els/', ''))

# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
import os
import sys

sys.path.append('/Workspace/rads/library/')
import column_functions
import dataframe_functions
import snapshot_functions

# COMMAND ----------
# Parameters
ani_stamp = '20250801'
source_snapshot = '20260214'

stanford_base_path = 'dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801'
path_career_top_cited = os.path.join(
    stanford_base_path,
    'Table-S1_20250801_1960_1996_2024_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2024.parquet',
)

str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_journal_author_ranking'
cache_folder = os.path.join(str_path_project, 'cache')
output_folder = os.path.join(str_path_project, 'output')

FULL_DOC_TYPES = ['ar', 're', 'cp']
TOP_Y = 15

# Sliding 3-year windows ending 2017..2024.
WINDOWS = [
    {'window_start': y - 2, 'window_end': y, 'window_id': f'w3_{y-2}_{y}'}
    for y in range(2017, 2025)
]

# COMMAND ----------
# Author ranks from Stanford career table.
df_author_rank = dataframe_functions.df_cached(
    spark.read.format('parquet').load(path_career_top_cited)
    .select(
        F.col('author_id').cast('long').alias('auid'),
        F.col('ns_ord').cast('long').alias('author_rank_ns'),
        F.col('ws_ord').cast('long').alias('author_rank_ws'),
        F.col('ns_c').cast('double').alias('composite_ns'),
        F.col('ws_c').cast('double').alias('composite_ws'),
    )
    .dropDuplicates(['auid']),
    os.path.join(cache_folder, 'author_rank_from_stanford_career'),
    partitions=40,
)

print(f'Authors with ranking loaded: {df_author_rank.count():,}')

# COMMAND ----------
# Base ANI extract reused across windows.
df_ani_base = dataframe_functions.df_cached(
    spark.table(f'scopus.ani_{ani_stamp}')
    .filter(column_functions.nopp())
    .filter(F.col('source.type') == 'j')
    .filter(F.col('sort_year').between(2015, 2024))
    .select(
        F.col('Eid').cast('long').alias('eid'),
        F.col('sort_year').cast('int').alias('year'),
        F.col('citation_type'),
        F.col('source.srcid').cast('long').alias('srcid'),
        F.col('source.sourcetitle').alias('sourcetitle'),
        F.col('Au').alias('authors'),
    ),
    os.path.join(cache_folder, 'ani_base_journals_2015_2024_nopp'),
    partitions=80,
)

# COMMAND ----------
# Authorship rows reused in all windows.
df_authorships = dataframe_functions.df_cached(
    df_ani_base.withColumn('author', F.explode('authors'))
    .select(
        'eid',
        'year',
        'citation_type',
        'srcid',
        'sourcetitle',
        F.col('author.auid').cast('long').alias('auid'),
    )
    .filter(F.col('auid').isNotNull()),
    os.path.join(cache_folder, 'authorships_journals_2015_2024_nopp'),
    partitions=100,
)

print(f'Authorship rows loaded: {df_authorships.count():,}')

# COMMAND ----------
# Source metrics by year: SNIP, SJR, CiteScore (csCiteScore).
df_source = snapshot_functions.source.get_table(snapshot=source_snapshot).select(
    F.col('id').cast('long').alias('srcid'),
    'sourcetitle',
    'metrics',
    'calculations',
)

# SNIP / SJR from metrics array.
df_source_metrics_long = (
    df_source.withColumn('m', F.explode_outer('metrics'))
    .select(
        'srcid',
        F.col('m.year').cast('int').alias('metric_year'),
        F.col('m.name').alias('metric_name'),
        F.col('m.value').cast('double').alias('metric_value'),
    )
    .filter(F.col('metric_name').isin(['SNIP', 'SJR']))
)

df_source_snip_sjr = (
    df_source_metrics_long.groupBy('srcid', 'metric_year')
    .pivot('metric_name', ['SNIP', 'SJR'])
    .agg(F.first('metric_value'))
    .withColumnRenamed('SNIP', 'snip')
    .withColumnRenamed('SJR', 'sjr')
)

# CiteScore from calculations array.
df_source_citescore = (
    df_source.withColumn('calc', F.explode_outer('calculations'))
    .select(
        'srcid',
        F.col('calc.year').cast('int').alias('metric_year'),
        F.col('calc.csMetric.csCiteScore').cast('double').alias('citescore'),
    )
)

df_source_year_metrics = dataframe_functions.df_cached(
    df_source_snip_sjr.join(df_source_citescore, ['srcid', 'metric_year'], 'full_outer')
    .filter(F.col('metric_year').between(2015, 2024)),
    os.path.join(cache_folder, 'source_year_metrics_2015_2024'),
    partitions=20,
)

print(f'Source/year metric rows: {df_source_year_metrics.count():,}')

# COMMAND ----------
def build_window_outputs(window_cfg):
    ys = window_cfg['window_start']
    ye = window_cfg['window_end']
    window_id = window_cfg['window_id']

    df_scope = dataframe_functions.df_cached(
        df_authorships.filter(F.col('year').between(ys, ye))
        .filter(F.col('citation_type').isin(FULL_DOC_TYPES)),
        os.path.join(cache_folder, f'{window_id}_scope_ar_re_cp'),
        partitions=80,
    )

    df_counts = dataframe_functions.df_cached(
        df_scope.groupBy('srcid', 'sourcetitle', 'auid').agg(
            F.countDistinct('eid').alias('n_pubs')
        ),
        os.path.join(cache_folder, f'{window_id}_journal_author_counts'),
        partitions=80,
    )

    w_journal_author = Window.partitionBy('srcid').orderBy(
        F.col('n_pubs').desc(),
        F.col('author_rank_ns').asc_nulls_last(),
        F.col('auid').asc(),
    )

    df_top_authors = dataframe_functions.df_cached(
        df_counts.join(df_author_rank.select('auid', 'author_rank_ns', 'author_rank_ws'), ['auid'], 'left')
        .withColumn('author_has_rank', F.col('author_rank_ns').isNotNull())
        .withColumn('rownum', F.row_number().over(w_journal_author))
        .filter(F.col('rownum') <= F.lit(TOP_Y)),
        os.path.join(cache_folder, f'{window_id}_top_authors_y{TOP_Y}'),
        partitions=80,
    )

    df_journal_primary = (
        df_top_authors.groupBy('srcid', 'sourcetitle')
        .agg(
            F.expr('percentile_approx(author_rank_ns, 0.5)').alias('topY_median_rank_ns'),
            F.avg('author_rank_ns').alias('topY_mean_rank_ns'),
            F.count('*').alias('topY_selected'),
            F.sum(F.col('author_has_rank').cast('int')).alias('topY_with_rank_ns'),
        )
        .withColumn('topY_rank_coverage', F.when(F.col('topY_selected') > 0, F.col('topY_with_rank_ns') / F.col('topY_selected')).otherwise(F.lit(None)))
    )

    df_journal_totals = (
        df_scope.groupBy('srcid', 'sourcetitle')
        .agg(
            F.countDistinct('eid').alias('total_n_pubs_period'),
            F.countDistinct('auid').alias('total_n_authors_period'),
        )
    )

    df_primary_scored = dataframe_functions.df_cached(
        df_journal_primary.join(df_journal_totals, ['srcid', 'sourcetitle'], 'left')
        .join(
            df_source_year_metrics.filter(F.col('metric_year') == F.lit(ye)).drop('metric_year'),
            ['srcid'],
            'left',
        )
        .withColumn('window_id', F.lit(window_id))
        .withColumn('window_start', F.lit(ys))
        .withColumn('window_end', F.lit(ye))
        .withColumn('topY', F.lit(TOP_Y))
        .withColumn('doc_type_scope', F.lit('ar_re_cp')),
        os.path.join(cache_folder, f'{window_id}_journal_primary_scored'),
        partitions=40,
    )

    # Alternative metric: per-paper best author rank median.
    df_paper_best = dataframe_functions.df_cached(
        df_scope.join(df_author_rank.select('auid', 'author_rank_ns'), ['auid'], 'left')
        .groupBy('srcid', 'sourcetitle', 'eid')
        .agg(F.min('author_rank_ns').alias('paper_best_author_rank_ns')),
        os.path.join(cache_folder, f'{window_id}_paper_best'),
        partitions=80,
    )

    df_alt_scored = dataframe_functions.df_cached(
        df_paper_best.groupBy('srcid', 'sourcetitle')
        .agg(
            F.expr('percentile_approx(paper_best_author_rank_ns, 0.5)').alias('paper_best_median_rank_ns'),
            F.avg('paper_best_author_rank_ns').alias('paper_best_mean_rank_ns'),
            F.count('*').alias('n_papers_evaluated'),
            F.sum(F.col('paper_best_author_rank_ns').isNotNull().cast('int')).alias('n_papers_with_rank'),
        )
        .join(df_journal_totals, ['srcid', 'sourcetitle'], 'left')
        .join(
            df_source_year_metrics.filter(F.col('metric_year') == F.lit(ye)).drop('metric_year'),
            ['srcid'],
            'left',
        )
        .withColumn('window_id', F.lit(window_id))
        .withColumn('window_start', F.lit(ys))
        .withColumn('window_end', F.lit(ye))
        .withColumn('topY', F.lit(TOP_Y))
        .withColumn('doc_type_scope', F.lit('ar_re_cp')),
        os.path.join(cache_folder, f'{window_id}_journal_alt_scored'),
        partitions=40,
    )

    return df_primary_scored, df_alt_scored

# COMMAND ----------
primary_dfs = []
alt_dfs = []

for w in WINDOWS:
    print(f"Window {w['window_start']}-{w['window_end']} ({w['window_id']})")
    df_primary, df_alt = build_window_outputs(w)
    primary_dfs.append(df_primary)
    alt_dfs.append(df_alt)

# COMMAND ----------
# Union all windows.
df_primary_all = primary_dfs[0]
for df in primary_dfs[1:]:
    df_primary_all = df_primary_all.unionByName(df)

df_alt_all = alt_dfs[0]
for df in alt_dfs[1:]:
    df_alt_all = df_alt_all.unionByName(df)

# COMMAND ----------
# Rank journals within each window.
w_primary_rank = Window.partitionBy('window_id').orderBy(F.asc('topY_median_rank_ns'), F.desc('total_n_pubs_period'))
df_primary_all = df_primary_all.withColumn('window_rank_primary', F.row_number().over(w_primary_rank))

w_alt_rank = Window.partitionBy('window_id').orderBy(F.asc('paper_best_median_rank_ns'), F.desc('total_n_pubs_period'))
df_alt_all = df_alt_all.withColumn('window_rank_alt', F.row_number().over(w_alt_rank))

# COMMAND ----------
# Export main trend tables.
export_base = os.path.join(output_folder, 'journal_author_rankings_2026')

dataframe_functions.export_df_csv(
    df_primary_all,
    'journal_window_scores_primary_w3_2017_2024',
    export_base,
    compressed=True,
    partitions=1,
)

dataframe_functions.export_df_csv(
    df_alt_all,
    'journal_window_scores_alt_w3_2017_2024',
    export_base,
    compressed=True,
    partitions=1,
)

# COMMAND ----------
# Window-level correlation summary against source metrics at window end year.
def window_corrs(df_in, score_col):
    return (
        df_in.groupBy('window_id', 'window_start', 'window_end')
        .agg(
            F.corr(score_col, 'snip').alias('corr_score_snip'),
            F.corr(score_col, 'sjr').alias('corr_score_sjr'),
            F.corr(score_col, 'citescore').alias('corr_score_citescore'),
            F.count('*').alias('n_journals'),
        )
        .orderBy('window_end')
    )

df_corr_primary = window_corrs(df_primary_all, 'topY_median_rank_ns').withColumn('metric_variant', F.lit('primary_topY_median_rank_ns'))
df_corr_alt = window_corrs(df_alt_all, 'paper_best_median_rank_ns').withColumn('metric_variant', F.lit('alt_paper_best_median_rank_ns'))

df_corr_all = df_corr_primary.unionByName(df_corr_alt)

dataframe_functions.export_df_csv(
    df_corr_all,
    'window_correlations_vs_source_metrics_w3_2017_2024',
    export_base,
    compressed=True,
    partitions=1,
)

# COMMAND ----------
# Quick display of trend correlation table.
df_corr_all.display()

# COMMAND ----------
# Write methodological note for this trend extension.
note_lines = [
    '# Sliding-Window Trend Extension (3-year windows)',
    '',
    'This extension computes journal indicators in rolling 3-year windows and aligns source metrics at each window end-year.',
    '',
    'Design:',
    '- Windows: 2015-2017 ... 2022-2024.',
    '- Document scope: ar/re/cp only.',
    '- Primary journal indicator: median rank of top 15 prolific authors in each journal-window.',
    '- Alternative indicator: median of per-paper best-author ranks in each journal-window.',
    '- Source metrics joined at window_end year: SNIP, SJR, CiteScore.',
    '',
    'Output files:',
    '- journal_window_scores_primary_w3_2017_2024.csv.gz',
    '- journal_window_scores_alt_w3_2017_2024.csv.gz',
    '- window_correlations_vs_source_metrics_w3_2017_2024.csv.gz',
    '',
    'Interpretation note:',
    '- Lower rank-based indicator values mean lower rank numbers (closer to rank 1) under this specific construct.',
    '- This is not an intrinsic journal quality claim.',
]

dbutils.fs.put(
    os.path.join(export_base, 'trend_extension_method_note.md'),
    '\n'.join(note_lines),
    True,
)

print('S3 output folder:')
print('s3://' + export_base.replace('dbfs:/', '').replace('/mnt/els/', ''))

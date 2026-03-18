# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
import os
import sys

sys.path.append('/Workspace/rads/library/')
import column_functions
import dataframe_functions

# COMMAND ----------
# Parameters
ani_stamp = '20250801'

# Stanford top-cited career table (without self-citations) produced by v10 run.
stanford_base_path = 'dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801'
path_career_top_cited = os.path.join(
    stanford_base_path,
    'Table-S1_20250801_1960_1996_2024_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2024.parquet',
)

str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_journal_author_ranking'
cache_folder = os.path.join(str_path_project, 'cache')
output_folder = os.path.join(str_path_project, 'output')

FULL_DOC_TYPES = ['ar', 're', 'cp']
JBC_SRCID = 17592

SCENARIOS = [
    {
        'scenario_id': 's1_2015_2024_full_docs_top15',
        'year_start': 2015,
        'year_end': 2024,
        'doc_types': FULL_DOC_TYPES,
        'top_y': 15,
        'label': 'Primary: 2015-2024, full documents only, top 15 authors',
    },
    {
        'scenario_id': 's2_2015_2024_all_items_top15',
        'year_start': 2015,
        'year_end': 2024,
        'doc_types': None,
        'top_y': 15,
        'label': 'Sensitivity A: 2015-2024, all item types, top 15 authors',
    },
    {
        'scenario_id': 's3_2023_2024_full_docs_top15',
        'year_start': 2023,
        'year_end': 2024,
        'doc_types': FULL_DOC_TYPES,
        'top_y': 15,
        'label': 'Sensitivity B: 2023-2024, full documents only, top 15 authors',
    },
    {
        'scenario_id': 's4_2015_2024_full_docs_top25',
        'year_start': 2015,
        'year_end': 2024,
        'doc_types': FULL_DOC_TYPES,
        'top_y': 25,
        'label': 'Sensitivity C: 2015-2024, full documents only, top 25 authors',
    },
]

# COMMAND ----------
# Load author ranks from the Stanford career table.
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
# Base ANI extract.
# Hard rule: nopp() must be the first ANI filter.
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

print(f'Journal papers in ANI base set: {df_ani_base.count():,}')

# COMMAND ----------
# Exploded authorship table reused across scenarios.
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

print(f'Authorship rows in 2015-2024 journals: {df_authorships.count():,}')

# COMMAND ----------
def build_scenario_outputs(config):
    scenario_id = config['scenario_id']
    year_start = config['year_start']
    year_end = config['year_end']
    doc_types = config['doc_types']
    top_y = config['top_y']

    df_scope = df_authorships.filter(F.col('year').between(year_start, year_end))
    if doc_types is not None:
        df_scope = df_scope.filter(F.col('citation_type').isin(doc_types))

    df_scope = dataframe_functions.df_cached(
        df_scope,
        os.path.join(cache_folder, f'{scenario_id}_scope'),
        partitions=80,
    )

    df_counts = dataframe_functions.df_cached(
        df_scope.groupBy('srcid', 'sourcetitle', 'auid').agg(
            F.countDistinct('eid').alias('n_pubs')
        ),
        os.path.join(cache_folder, f'{scenario_id}_journal_author_counts'),
        partitions=80,
    )

    w_journal_author = Window.partitionBy('srcid').orderBy(
        F.col('n_pubs').desc(),
        F.col('author_rank_ns').asc_nulls_last(),
        F.col('auid').asc(),
    )

    df_top_authors = dataframe_functions.df_cached(
        df_counts.join(df_author_rank, ['auid'], 'left')
        .withColumn('author_has_rank', F.col('author_rank_ns').isNotNull())
        .withColumn('rownum', F.row_number().over(w_journal_author))
        .filter(F.col('rownum') <= F.lit(top_y)),
        os.path.join(cache_folder, f'{scenario_id}_top_authors'),
        partitions=80,
    )

    df_journal_scores = (
        df_top_authors.groupBy('srcid', 'sourcetitle')
        .agg(
            F.expr('percentile_approx(author_rank_ns, array(0.25, 0.5, 0.75))').alias('q'),
            F.avg('author_rank_ns').alias('topY_mean_rank_ns'),
            F.min('author_rank_ns').alias('topY_best_rank_ns'),
            F.max('author_rank_ns').alias('topY_worst_rank_ns'),
            F.count('*').alias('topY_selected'),
            F.sum(F.col('author_has_rank').cast('int')).alias('topY_with_rank_ns'),
            F.sum(F.when(F.col('author_rank_ns') <= 100000, F.lit(1)).otherwise(F.lit(0))).alias('topY_in_ns_100k'),
        )
        .select(
            'srcid',
            'sourcetitle',
            F.col('q')[0].alias('topY_q1_rank_ns'),
            F.col('q')[1].alias('topY_median_rank_ns'),
            F.col('q')[2].alias('topY_q3_rank_ns'),
            'topY_mean_rank_ns',
            'topY_best_rank_ns',
            'topY_worst_rank_ns',
            'topY_selected',
            'topY_with_rank_ns',
            'topY_in_ns_100k',
        )
    )

    df_journal_totals = (
        df_scope.groupBy('srcid', 'sourcetitle')
        .agg(
            F.countDistinct('eid').alias('total_n_pubs_period'),
            F.countDistinct('auid').alias('total_n_authors_period'),
        )
    )

    df_rankings = dataframe_functions.df_cached(
        df_journal_scores.join(df_journal_totals, ['srcid', 'sourcetitle'], 'left')
        .withColumn('scenario_id', F.lit(scenario_id))
        .withColumn('scenario_label', F.lit(config['label']))
        .withColumn('period_start', F.lit(year_start))
        .withColumn('period_end', F.lit(year_end))
        .withColumn('doc_type_scope', F.lit('all_items' if doc_types is None else 'ar_re_cp'))
        .withColumn('topY', F.lit(top_y))
        .withColumn(
            'topY_rank_coverage',
            F.when(F.col('topY_selected') > 0, F.col('topY_with_rank_ns') / F.col('topY_selected')).otherwise(F.lit(None)),
        )
        .orderBy(F.asc('topY_median_rank_ns')),
        os.path.join(cache_folder, f'{scenario_id}_journal_rankings'),
        partitions=40,
    )

    # Alternative journal score: per-paper best author rank, then median across papers.
    df_paper_best_rank = dataframe_functions.df_cached(
        df_scope.join(df_author_rank.select('auid', 'author_rank_ns'), ['auid'], 'left')
        .groupBy('srcid', 'sourcetitle', 'eid')
        .agg(
            F.min('author_rank_ns').alias('paper_best_author_rank_ns'),
            F.countDistinct('auid').alias('paper_author_count'),
        ),
        os.path.join(cache_folder, f'{scenario_id}_paper_best_rank'),
        partitions=80,
    )

    df_alt_rankings = dataframe_functions.df_cached(
        df_paper_best_rank.groupBy('srcid', 'sourcetitle')
        .agg(
            F.expr('percentile_approx(paper_best_author_rank_ns, array(0.25, 0.5, 0.75))').alias('q'),
            F.avg('paper_best_author_rank_ns').alias('paper_best_mean_rank_ns'),
            F.min('paper_best_author_rank_ns').alias('paper_best_best_rank_ns'),
            F.max('paper_best_author_rank_ns').alias('paper_best_worst_rank_ns'),
            F.count('*').alias('n_papers_evaluated'),
            F.sum(F.col('paper_best_author_rank_ns').isNotNull().cast('int')).alias('n_papers_with_rank'),
        )
        .select(
            'srcid',
            'sourcetitle',
            F.col('q')[0].alias('paper_best_q1_rank_ns'),
            F.col('q')[1].alias('paper_best_median_rank_ns'),
            F.col('q')[2].alias('paper_best_q3_rank_ns'),
            'paper_best_mean_rank_ns',
            'paper_best_best_rank_ns',
            'paper_best_worst_rank_ns',
            'n_papers_evaluated',
            'n_papers_with_rank',
        )
        .join(df_journal_totals, ['srcid', 'sourcetitle'], 'left')
        .withColumn('scenario_id', F.lit(scenario_id))
        .withColumn('scenario_label', F.lit(config['label']))
        .withColumn('period_start', F.lit(year_start))
        .withColumn('period_end', F.lit(year_end))
        .withColumn('doc_type_scope', F.lit('all_items' if doc_types is None else 'ar_re_cp'))
        .withColumn('topY', F.lit(top_y))
        .orderBy(F.asc('paper_best_median_rank_ns')),
        os.path.join(cache_folder, f'{scenario_id}_journal_alt_rankings'),
        partitions=40,
    )

    return df_scope, df_top_authors, df_rankings, df_alt_rankings

# COMMAND ----------
ranking_dfs = []
alt_ranking_dfs = []
top_author_dfs = []

for cfg in SCENARIOS:
    print(f"Running scenario: {cfg['scenario_id']} | {cfg['label']}")
    _, df_top_auth, df_rank, df_alt = build_scenario_outputs(cfg)

    ranking_dfs.append(df_rank)
    alt_ranking_dfs.append(df_alt)
    top_author_dfs.append(
        df_top_auth
        .withColumn('scenario_id', F.lit(cfg['scenario_id']))
        .withColumn('scenario_label', F.lit(cfg['label']))
        .withColumn('period_start', F.lit(cfg['year_start']))
        .withColumn('period_end', F.lit(cfg['year_end']))
        .withColumn('doc_type_scope', F.lit('all_items' if cfg['doc_types'] is None else 'ar_re_cp'))
        .withColumn('topY', F.lit(cfg['top_y']))
    )

# COMMAND ----------
# Combine scenario outputs.
df_rankings_all = ranking_dfs[0]
for df in ranking_dfs[1:]:
    df_rankings_all = df_rankings_all.unionByName(df)

df_alt_rankings_all = alt_ranking_dfs[0]
for df in alt_ranking_dfs[1:]:
    df_alt_rankings_all = df_alt_rankings_all.unionByName(df)

df_top_authors_all = top_author_dfs[0]
for df in top_author_dfs[1:]:
    df_top_authors_all = df_top_authors_all.unionByName(df)

# COMMAND ----------
# Ranked views per scenario.
w_scenario_rank = Window.partitionBy('scenario_id').orderBy(F.asc('topY_median_rank_ns'), F.desc('total_n_pubs_period'))
df_rankings_all = df_rankings_all.withColumn('scenario_journal_rank', F.row_number().over(w_scenario_rank))

w_scenario_rank_alt = Window.partitionBy('scenario_id').orderBy(F.asc('paper_best_median_rank_ns'), F.desc('total_n_pubs_period'))
df_alt_rankings_all = df_alt_rankings_all.withColumn('scenario_journal_rank_alt', F.row_number().over(w_scenario_rank_alt))

# COMMAND ----------
# JBC example requested in email.
df_jbc_top_authors = (
    df_top_authors_all
    .filter(F.col('srcid') == F.lit(JBC_SRCID))
    .orderBy('scenario_id', F.asc('rownum'))
    .select(
        'scenario_id',
        'scenario_label',
        'srcid',
        'sourcetitle',
        'rownum',
        'auid',
        'n_pubs',
        'author_rank_ns',
        'author_rank_ws',
    )
)

# COMMAND ----------
# Exports for downstream reading.
export_base = os.path.join(output_folder, 'journal_author_rankings_2026')

dataframe_functions.export_df_csv(
    df_rankings_all,
    'journal_rankings_top_authors_all_scenarios',
    export_base,
    compressed=True,
    partitions=1,
)

dataframe_functions.export_df_csv(
    df_alt_rankings_all,
    'journal_rankings_alt_paper_best_author_all_scenarios',
    export_base,
    compressed=True,
    partitions=1,
)

dataframe_functions.export_df_csv(
    df_top_authors_all.select(
        'scenario_id',
        'scenario_label',
        'period_start',
        'period_end',
        'doc_type_scope',
        'topY',
        'srcid',
        'sourcetitle',
        'rownum',
        'auid',
        'n_pubs',
        'author_rank_ns',
        'author_rank_ws',
        'author_has_rank',
    ),
    'journal_top_authors_all_scenarios',
    export_base,
    compressed=True,
    partitions=1,
)

dataframe_functions.export_df_csv(
    df_jbc_top_authors,
    'jbc_top_authors_all_scenarios',
    export_base,
    compressed=True,
    partitions=1,
)

# COMMAND ----------
# Quick results preview.
(
    df_rankings_all
    .filter(F.col('topY_with_rank_ns') == F.col('topY'))
    .orderBy('scenario_id', 'scenario_journal_rank')
    .select(
        'scenario_id',
        'scenario_journal_rank',
        'srcid',
        'sourcetitle',
        'topY_median_rank_ns',
        'topY_with_rank_ns',
        'total_n_pubs_period',
        'total_n_authors_period',
    )
    .limit(200)
    .display()
)

# COMMAND ----------
# Scenario-level summary statistics.
df_summary = (
    df_rankings_all.groupBy('scenario_id', 'scenario_label')
    .agg(
        F.count('*').alias('n_journals_scored'),
        F.sum(F.when(F.col('topY_with_rank_ns') == F.col('topY'), F.lit(1)).otherwise(F.lit(0))).alias('n_journals_full_rank_coverage'),
        F.percentile_approx('topY_median_rank_ns', 0.5).alias('median_of_journal_medians'),
        F.avg('topY_rank_coverage').alias('avg_topY_rank_coverage'),
        F.percentile_approx('total_n_pubs_period', 0.5).alias('median_journal_output'),
    )
)

df_summary.display()

# COMMAND ----------
# Write a short recommendations note to project output for easy retrieval.
scenario_note_lines = [
    '# Journal Ranking Based on Top-Cited Authors',
    '',
    'This analysis was executed with Stanford top-cited author ranks (career, no self-citations) and ANI snapshot 20250801.',
    '',
    'Implemented evaluation sets:',
    '- Primary: X=10 years (2015-2024), full document types (ar/re/cp), Y=15 prolific authors per journal.',
    '- Sensitivity A: X=10 years, all item types, Y=15.',
    '- Sensitivity B: X=2 years (2023-2024), full document types, Y=15.',
    '- Sensitivity C: X=10 years, full document types, Y=25.',
    '- Alternative method: per-paper best author rank median per journal.',
    '',
    'Tie handling at the Y-th author slot:',
    '- Authors are sorted by publication count in journal (descending).',
    '- Ties are broken by better (lower) career no-self-citation rank, then by AUID.',
    '- This exactly fills Y slots while favoring better-ranked authors in ties.',
    '',
    'Output files (CSV gzip) are in:',
    f'- {export_base}',
    '',
    'Recommended default for reporting:',
    '- Use Primary scenario (2015-2024, ar/re/cp, Y=15) as the headline metric.',
    '- Report Sensitivity A and B in supplement to show robustness against item type and window length.',
    '- Consider Y=25 only for large journals; Y=15 is less diluted and aligns with the initial concept.',
    '- Keep the alternative per-paper metric as exploratory, because it shifts from sustained author-journal affinity to one-paper superstar effects.',
]

note_text = '\n'.join(scenario_note_lines)
note_path = os.path.join(export_base, 'recommendations_and_method_note.md')
dbutils.fs.put(note_path, note_text, True)
print(f'Wrote: {note_path}')

# COMMAND ----------
# S3 pointers for easy copy/paste.
print('S3 output folder:')
print('s3://' + export_base.replace('dbfs:/', '').replace('/mnt/els/', ''))

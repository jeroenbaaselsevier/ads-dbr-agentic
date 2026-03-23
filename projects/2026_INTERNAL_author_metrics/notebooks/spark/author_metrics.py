# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
import os
import sys

sys.path.append('/Workspace/rads/library/')
import column_functions
import snapshot_functions

# COMMAND ----------
ANI_STAMP = '20260301'
SOURCE_SNAPSHOT = '20260301'
OUTPUT_BASE = '/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_author_metrics/output/author_metrics_20260320'
OUTPUT_PATH = os.path.join(OUTPUT_BASE, 'author_eid_level_parquet')

AUTHOR_ROWS = [
    {'auid': 58306344700, 'nr': 1, 'last_name': 'Barnes', 'first_name': 'Kathryn'},
    {'auid': 57200231841, 'nr': 2, 'last_name': 'Bast', 'first_name': 'Nico'},
    {'auid': 57210360881, 'nr': 3, 'last_name': 'Chakraborty', 'first_name': 'Taniya'},
    {'auid': 57189708250, 'nr': 4, 'last_name': 'Depner', 'first_name': 'Anamaria'},
    {'auid': 56311121500, 'nr': 5, 'last_name': 'Eckart', 'first_name': 'Sebastian'},
    {'auid': 57212106700, 'nr': 6, 'last_name': 'Eibes', 'first_name': 'Pia'},
    {'auid': 57193451034, 'nr': 7, 'last_name': 'Hussein', 'first_name': 'Rana'},
    {'auid': 57204933506, 'nr': 8, 'last_name': 'König', 'first_name': 'Tobias'},
    {'auid': 57211913001, 'nr': 9, 'last_name': 'Kortendiek', 'first_name': 'Nele'},
    {'auid': 56708137700, 'nr': 10, 'last_name': 'McCormack', 'first_name': 'Jeremy'},
    {'auid': 57225238849, 'nr': 11, 'last_name': 'Middelhoff', 'first_name': 'Frederike'},
    {'auid': 57212374512, 'nr': 12, 'last_name': 'Petry', 'first_name': 'Johannes'},
    {'auid': 57194129320, 'nr': 13, 'last_name': 'Scheich', 'first_name': 'Sebastian'},
    {'auid': 60241831500, 'nr': 14, 'last_name': 'Steele', 'first_name': 'Sina'},
    {'auid': 57210441659, 'nr': 14, 'last_name': 'Selzer', 'first_name': 'Sina'},
    {'auid': 57200530111, 'nr': 15, 'last_name': 'Stephan', 'first_name': 'Till'},
    {'auid': 59895841600, 'nr': 16, 'last_name': 'Taylor', 'first_name': 'Nathan'},
    {'auid': 23029487300, 'nr': 17, 'last_name': 'Uckelmann', 'first_name': 'Hannah'},
]

author_schema = T.StructType([
    T.StructField('auid', T.LongType(), False),
    T.StructField('nr', T.IntegerType(), False),
    T.StructField('last_name', T.StringType(), False),
    T.StructField('first_name', T.StringType(), False),
])

df_authors = spark.createDataFrame(AUTHOR_ROWS, schema=author_schema)
author_ids = [row['auid'] for row in AUTHOR_ROWS]

print(f'ANI stamp: {ANI_STAMP}')
print(f'Output path: {OUTPUT_PATH}')
print(f'Requested authors: {len(author_ids)} IDs across {df_authors.select("nr").distinct().count()} Nr values')

# COMMAND ----------
df_author_eids = (
    spark.table(f'scopus.ani_{ANI_STAMP}')
    .filter(column_functions.nopp())
    .select(
        F.col('Eid').cast('long').alias('eid'),
        F.col('sort_year').cast('int').alias('sort_year'),
        F.col('source.srcid').cast('long').alias('srcid'),
        F.col('source.sourcetitle').alias('source_title'),
        F.explode('Au').alias('author_struct'),
    )
    .filter(F.col('author_struct.auid').cast('long').isin(author_ids))
    .select(
        F.col('author_struct.auid').cast('long').alias('auid'),
        'eid',
        'sort_year',
        'srcid',
        'source_title',
    )
    .dropDuplicates(['auid', 'eid'])
    .join(df_authors, ['auid'], 'inner')
)

df_cached = df_author_eids.cache()
print(f'Author-eid rows after ANI filter: {df_cached.count()}')

# COMMAND ----------
df_fwci = (
    snapshot_functions.ads.publication.get_table('FWCI_All_cits_and_non_self_cits_perc')
    .select(
        F.col('EID').cast('long').alias('eid'),
        F.col('Citations_NoWindow').cast('double').alias('citations_nowindow'),
        F.col('FWCI_4y').cast('double').alias('fwci_4y'),
        F.col('perc_FWCI_4y').cast('double').alias('fwci_4y_percentile'),
    )
    .withColumn(
        'is_top10_fwci_4y',
        F.when(F.col('fwci_4y_percentile') <= F.lit(0.10), F.lit(1)).otherwise(F.lit(0)),
    )
)

df_collab = (
    snapshot_functions.ads.publication.get_table('Article_Collaboration_orgdb')
    .select(
        F.col('eid').cast('long').alias('eid'),
        F.col('CollaborationLevel').alias('collaboration_level'),
    )
)

# COMMAND ----------
df_source = snapshot_functions.source.get_table(snapshot=SOURCE_SNAPSHOT)

df_source_citescore_by_year = (
    df_source
    .select(F.col('id').cast('long').alias('srcid'), F.explode_outer('calculations').alias('calc'))
    .filter(F.col('calc.status') == F.lit('Complete'))
    .filter(F.col('calc.csMetric').isNotNull())
    .select(
        'srcid',
        F.col('calc.year').cast('int').alias('calc_year'),
        F.col('calc.csMetric.csCiteScore').cast('double').alias('citescore_selected_year'),
        F.explode_outer('calc.csMetric.csSubjectCategory').alias('subject_category'),
    )
    .groupBy('srcid', 'calc_year', 'citescore_selected_year')
    .agg(
        F.max(F.col('subject_category.csPercentile').cast('double')).alias('best_citescore_percentile'),
    )
)

df_paper_journal_candidates = (
    df_cached
    .select('auid', 'eid', 'sort_year', 'srcid')
    .join(df_source_citescore_by_year, ['srcid'], 'left')
    .filter(F.col('calc_year').isNull() | (F.col('calc_year') >= F.col('sort_year')))
    .withColumn(
        'year_gap',
        F.when(F.col('calc_year').isNull(), F.lit(9999)).otherwise(F.col('calc_year') - F.col('sort_year')),
    )
    .withColumn('has_calc', F.when(F.col('calc_year').isNull(), F.lit(0)).otherwise(F.lit(1)))
)

paper_rank_window = Window.partitionBy('auid', 'eid').orderBy(
    F.col('has_calc').desc(),
    F.col('year_gap').asc(),
    F.col('calc_year').asc(),
)

df_paper_journal = (
    df_paper_journal_candidates
    .withColumn('rn', F.row_number().over(paper_rank_window))
    .filter(F.col('rn') == F.lit(1))
    .select(
        'auid',
        'eid',
        F.col('best_citescore_percentile'),
    )
    .withColumn(
        'is_top10_journal',
        F.when(F.col('best_citescore_percentile') >= F.lit(90.0), F.lit(1)).otherwise(F.lit(0)),
    )
)

df_source_citescore_2024 = (
    df_source
    .select(F.col('id').cast('long').alias('srcid'), F.explode_outer('calculations').alias('calc'))
    .filter(F.col('calc.year') == F.lit(2024))
    .filter(F.col('calc.csMetric').isNotNull())
    .groupBy('srcid')
    .agg(
        F.max(F.col('calc.csMetric.csCiteScore').cast('double')).alias('citescore_2024'),
    )
)

df_source_snip = (
    df_source
    .select(F.col('id').cast('long').alias('srcid'), F.explode_outer('metrics').alias('metric'))
    .filter(F.col('metric.name') == F.lit('SNIP'))
    .filter(F.col('metric.year') == F.lit(2024))
    .select(
        'srcid',
        F.col('metric.value').cast('double').alias('snip_2024'),
    )
)

# COMMAND ----------
df_result = (
    df_cached
    .join(df_fwci, ['eid'], 'left')
    .join(df_collab, ['eid'], 'left')
    .join(df_paper_journal, ['auid', 'eid'], 'left')
    .join(df_source_citescore_2024, ['srcid'], 'left')
    .join(df_source_snip, ['srcid'], 'left')
    .select(
        'nr',
        'auid',
        'last_name',
        'first_name',
        'eid',
        'sort_year',
        'srcid',
        'source_title',
        'citations_nowindow',
        'fwci_4y',
        'fwci_4y_percentile',
        'is_top10_fwci_4y',
        'best_citescore_percentile',
        'is_top10_journal',
        'citescore_2024',
        'snip_2024',
        'collaboration_level',
    )
)

df_result.orderBy('nr', 'auid', 'sort_year', 'eid').write.mode('overwrite').parquet(OUTPUT_PATH)

summary = (
    df_result
    .groupBy('nr')
    .agg(
        F.countDistinct('auid').alias('n_auids'),
        F.countDistinct('eid').alias('n_eids'),
        F.min('sort_year').alias('first_year_publication'),
        F.sum(F.coalesce(F.col('citations_nowindow'), F.lit(0.0))).alias('citations_nowindow_sum'),
    )
    .orderBy('nr')
)

summary.show(50, truncate=False)

try:
    dbutils.notebook.exit(OUTPUT_PATH)
except NameError:
    print(OUTPUT_PATH)
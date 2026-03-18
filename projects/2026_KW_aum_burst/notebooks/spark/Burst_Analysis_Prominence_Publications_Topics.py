# Databricks notebook source
# MAGIC %md
# MAGIC # Burst Score — Topics
# MAGIC
# MAGIC Calculates annual burst scores for SciVal **topics** based on a 6-year
# MAGIC prominence time series.  Run once a year in **June**, using the first ANI
# MAGIC and SciVal topic-mapping snapshots of that month.
# MAGIC
# MAGIC Output: `/mnt/els/rads-mappings/burst_analysis/topics/<analyze_year>/`

# COMMAND ----------

# MAGIC %md
# MAGIC # Settings

# COMMAND ----------

# Year being analysed (i.e. the most recent year in the 6-year window)
analyze_year = 2024
bottom_year  = 2019

# ANI snapshot stamp — also used to pick the usage and topic-mapping snapshots
scopus_snapshot_date = '20250601'

# Output path
path_output = f'/mnt/els/rads-mappings/burst_analysis/topics/{analyze_year}/'

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import sys
sys.path.append('/Workspace/rads/library/')

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import LongType, FloatType, MapType, StructType, StructField
import numpy as np

import snapshot_functions as sf
import column_functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Sanity check: source profiles have Complete CiteScore for analyze_year
# MAGIC
# MAGIC We always load the **latest** source profiles snapshot — Complete CiteScore values
# MAGIC are static once published, so there is no need to pin a date.

# COMMAND ----------

df_src = sf.source.get_table()

complete_count = (
    df_src
    .select(F.explode('calculations').alias('calc'))
    .filter(
        (F.col('calc.year') == analyze_year) &
        (F.col('calc.status') == 'Complete')
    )
    .count()
)
assert complete_count > 0, (
    f"No Complete CiteScore calculations found for {analyze_year} in source profiles. "
    "Check whether the snapshot is up to date."
)
print(f"Source check OK: {complete_count:,} sources have Complete calculations for {analyze_year}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load source CiteScore

# COMMAND ----------

df_src_citescore = (
    df_src
    .select('id', F.explode('calculations').alias('calc'))
    .filter(
        F.col('calc.year').between(bottom_year, analyze_year) &
        (F.col('calc.status') == 'Complete') &
        F.col('calc.csMetric.csCiteScore').isNotNull()
    )
    .select(
        F.col('id').alias('srcid'),
        F.col('calc.year').alias('data_year'),
        F.col('calc.csMetric.csCiteScore').cast('double').alias('citescore'),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load ANI

# COMMAND ----------

df_ani = (
    spark.table(f'scopus.ani_{scopus_snapshot_date}')
    .filter(column_functions.nopp())
    .select(
        'Eid', 'datesort', 'citations',
        F.col('source.srcid').alias('srcid'),
        F.col('source.date_year').alias('date_year'),
        F.col('source.date_month').alias('date_month'),
        F.col('source.date_day').alias('date_day'),
    )
    .withColumn('datesort_fmt', F.date_format(F.to_date('datesort', 'yyyyMMdd'), 'yyyy-MM-dd'))
    .withColumn('date_month',
        F.when((F.col('date_month').isNull()) | (F.col('date_month') == 0), F.lit('01'))
         .otherwise(F.col('date_month')))
    .withColumn('date_day',
        F.when((F.col('date_day').isNull()) | (F.col('date_day') == 0), F.lit('01'))
         .otherwise(F.col('date_day')))
    .withColumn('sort_year',
        F.substring(
            F.when((F.col('date_year').isNull()) | (F.col('date_year') == 0), F.col('datesort_fmt'))
             .otherwise(F.concat_ws('-', 'date_year', 'date_month', 'date_day')),
            1, 4
        )
    )
    .select('Eid', 'sort_year', 'citations', 'srcid')
    .filter('sort_year >= 1996')
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load SciVal topic mapping

# COMMAND ----------

# TopicId (long), EidString ("2-s2.0-XXXXX") → extract numeric Eid for ANI join
df_topic = (
    sf.scival.get_table('topic_eid', snapshot=scopus_snapshot_date)
    .withColumn('Eid', F.regexp_extract('EidString', r'2-s2\.0-(\d+)', 1).cast(LongType()))
    .select(F.col('TopicId').alias('TopicID'), 'Eid')
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load usage data

# COMMAND ----------

# Use the same snapshot date as ANI; Usage_Data lives under the same snapshot folder
df_usage = sf.ads.publication.get_table('Usage_Data', snapshot=scopus_snapshot_date)

# COMMAND ----------

# MAGIC %md
# MAGIC # Compute per-topic metrics

# COMMAND ----------

# Decay weights: most recent history year has weight decay^1, oldest has decay^(n-1)
decay = 0.8
decay_weights = [decay**a for a in range(analyze_year - bottom_year, 0, -1)]

# Year scaffold — ensures every topic gets a row even for years with no data
df_years = spark.createDataFrame([[y] for y in range(bottom_year, analyze_year + 1)], ['data_year'])

df_ani_window = df_ani.where(F.col('sort_year').between(bottom_year, analyze_year))

df_pubs = (
    df_ani_window
    .select('Eid', F.col('sort_year').alias('data_year'))
    .join(df_topic, 'Eid', 'left')
    .select('TopicID', 'data_year', 'Eid')
    .distinct()
    .groupBy('TopicID', 'data_year')
    .agg(F.count('Eid').alias('output'))
)

df_views = (
    df_usage
    .withColumnRenamed('usage_year', 'data_year')
    .join(df_ani_window.select('Eid', 'sort_year'), 'Eid', 'left')
    .join(df_topic, 'Eid', 'left')
    .select('TopicID', 'data_year', 'Eid', 'abstract_views', 'outward_links', 'sort_year')
    .filter('data_year between sort_year and sort_year + 1')
    .groupBy('TopicID', 'data_year')
    .agg((F.sum('abstract_views') + F.sum('outward_links')).alias('views'))
)

df_citescore = (
    df_ani_window
    .select('Eid', 'srcid', F.col('sort_year').alias('data_year'))
    .join(df_topic, 'Eid')
    .join(df_src_citescore, ['srcid', 'data_year'], 'left')
    .select('TopicID', 'data_year', 'citescore', 'Eid')
    .distinct()
    .groupBy('TopicID', 'data_year')
    .agg(F.avg('citescore').alias('citescore'))
    .fillna(0, ['citescore'])
)

df_citations = (
    df_ani_window
    .select(
        F.col('Eid').alias('citing'),
        F.col('sort_year').alias('data_year'),
        F.explode_outer('citations').alias('Eid'),
    )
    .join(df_ani.select('Eid', 'sort_year'), 'Eid', 'left')
    .join(df_topic, 'Eid', 'left')
    .distinct()
    .filter('data_year between sort_year and sort_year + 1')
    .groupBy('TopicID', 'data_year')
    .agg(F.count('citing').alias('citations'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Compute prominence

# COMMAND ----------

df_topic_info = (
    df_years
    .crossJoin(df_topic.select('TopicID').distinct())
    .join(df_pubs,      ['TopicID', 'data_year'], 'left')
    .join(df_views,     ['TopicID', 'data_year'], 'left')
    .join(df_citations, ['TopicID', 'data_year'], 'left')
    .join(df_citescore, ['TopicID', 'data_year'], 'left')
    .fillna(0, ['output', 'views', 'citations', 'citescore'])
    .withColumn('Cj',  F.log1p('citations'))
    .withColumn('Vj',  F.log1p('views'))
    .withColumn('CSj', F.log1p('citescore'))
)

df_info_mean = (
    df_topic_info
    .groupBy('data_year')
    .agg(
        F.avg('Cj').alias('avgCj'),   F.stddev('Cj').alias('stdCj'),
        F.avg('Vj').alias('avgVj'),   F.stddev('Vj').alias('stdVj'),
        F.avg('CSj').alias('avgCSj'), F.stddev('CSj').alias('stdCSj'),
    )
)

df_topic_prom = (
    df_topic_info
    .join(df_info_mean, 'data_year', 'left')
    .withColumn('Prominence',
        0.495 * (F.col('Cj')  - F.col('avgCj'))  / F.col('stdCj')  +
        0.391 * (F.col('Vj')  - F.col('avgVj'))  / F.col('stdVj')  +
        0.114 * (F.col('CSj') - F.col('avgCSj')) / F.col('stdCSj')
    )
    .withColumn('Rank', F.rank().over(
        Window.partitionBy('data_year')
              .orderBy(F.desc('Prominence'), F.desc('output'), F.asc('TopicID'))
    ))
    .withColumn('ProminencePerc', F.expr(
        '((max(Rank) over (partition by data_year)) - Rank + 1) '
        '/ (max(Rank) over (partition by data_year)) * 100'
    ))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Compute burst score

# COMMAND ----------

schema_burst = StructType([
    StructField('obs',        FloatType(), False),
    StructField('average',    FloatType(), False),
    StructField('variance',   FloatType(), False),
    StructField('burstScore', FloatType(), False),
    StructField('std',        FloatType(), False),
])


def Burst(s):
    """
    Compute burst score for a time series s.

    s[-1] is the observation year; s[:-1] is the history window.
    Score = (obs - weighted_avg) / weighted_std.
    If the history is perfectly constant but obs differs, extend with obs at
    a low weight to produce a finite (large) score rather than infinity.
    """
    try:
        s     = np.array(s, dtype=float)
        obs   = s[-1]
        vlist = s[:-1]
        avg      = np.average(vlist, weights=decay_weights)
        variance = float(np.average((vlist - avg) ** 2, weights=decay_weights))
        std      = np.sqrt(variance)
        if (variance == 0) and (obs == avg):
            score = 0.0
        elif std == 0:
            # Constant history, different observation — extend with obs at low weight
            ext   = np.append(vlist, obs)
            ext_w = decay_weights + [decay ** (len(decay_weights) + 1)]
            avg      = np.average(ext, weights=ext_w)
            variance = float(np.average((ext - avg) ** 2, weights=ext_w))
            std      = np.sqrt(variance)
            score    = (obs - avg) / std if std != 0 else 0.0
        else:
            score = (obs - avg) / std
        return type('obj', (object,), {
            'obs': float(obs), 'average': float(avg),
            'variance': variance, 'burstScore': float(score), 'std': float(std),
        })
    except Exception:
        return None


BurstScoreAnalysis = F.udf(Burst, schema_burst)

# COMMAND ----------


def mapmaker(x, y, z):
    return {j[y]: (float(j[z]) if j[z] is not None else None) for j in x}


mapper = F.udf(mapmaker, MapType(LongType(), FloatType()))

df_burst = (
    df_topic_prom
    .groupBy('TopicID')
    .agg(
        F.collect_list(F.struct(
            'data_year', 'output', 'Prominence', 'ProminencePerc',
            F.col('Rank').alias('ProminenceRank'),
        )).alias('data')
    )
    .withColumn('data',                    F.array_sort('data'))
    .withColumn('Output_by_year',          mapper('data', F.lit('data_year'), F.lit('output')))
    .withColumn('Prominence_by_year',      mapper('data', F.lit('data_year'), F.lit('Prominence')))
    .withColumn('Prominence_Rank_by_Year', mapper('data', F.lit('data_year'), F.lit('ProminenceRank')))
    .withColumn('PromPerc_by_year',        mapper('data', F.lit('data_year'), F.lit('ProminencePerc')))
    .withColumn('Prominence_ordered',      F.col('data.Prominence'))
    .withColumn('Output_ordered',          F.col('data.output'))
    .withColumn('Burst_Stats_Prominence',  BurstScoreAnalysis('Prominence_ordered'))
    .withColumn('Burst_Stats_Output',      BurstScoreAnalysis('Output_ordered'))
    .drop('data')
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

def _path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'FileNotFoundException' in str(e) or 'java.io.IOException' in str(e):
            return False
        raise

assert not _path_exists(path_output), (
    f"Output path already exists: {path_output}\n"
    "Delete it manually or choose a different analyze_year."
)

(
    df_burst
    .repartition(200)
    .write
    .format('parquet')
    .save(path_output)
)
print(f'Saved to: {path_output}')
display(spark.read.parquet(path_output).filter('TopicID = 3070'))

# COMMAND ----------

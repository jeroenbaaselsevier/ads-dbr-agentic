# Databricks notebook source
from pyspark.sql import functions as F
import os
import sys
sys.path.append('/Workspace/rads/library')
import orgdb_functions
import column_functions
import dataframe_functions
import snapshot_functions

# COMMAND ----------

# Journal of Biological Chemistry
# srcid = 17592

# COMMAND ----------

YEAR_START = 2015 
YEAR_END = 2024 
TOP_Y = 15 
FULL_DOC_TYPES = ['ar','re','cp']  # article, review, conference paper

# COMMAND ----------

path_project= f'dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/20250801/'

# COMMAND ----------

top_list_expression='(ws_ord <= 100000 OR ns_ord <= 100000) OR (rank_sm_subfield_1_ws/count_sm_subfield_1 <=.02) OR (rank_sm_subfield_1_ns/count_sm_subfield_1 <=.02)'
str_path_career=os.path.join(path_project,'Table-S1_20250801_1960_1996_2024_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2024.parquet')
str_path_singleyr=os.path.join(path_project,'Table-S1_20250801_1960_2024_2024_1788_ppFalse_minarcpre_5_minnpY1Y3_2_maxcityear_2024.parquet')

# COMMAND ----------

Last_ani = table(f'fca_ds.{max([x for x in sqlContext.tableNames("fca_ds") if x.startswith("job_parameters_for")])}').filter('parameters = "ani"').select('value').collect()[0][0]
Last_apr = table(f'fca_ds.{max([x for x in sqlContext.tableNames("fca_ds") if x.startswith("job_parameters_for")])}').filter('parameters = "apr"').select('value').collect()[0][0]

path = table(f'fca_ds.{max([x for x in sqlContext.tableNames("fca_ds") if x.startswith("job_parameters_for")])}').filter('parameters = "path"').select('value').collect()[0][0]


print("Last_ani: " + Last_ani)
print("Last_apr: " + Last_apr)

# run with
# Last_ani = ani_20260101
snapshot = Last_ani[-8:]

# run for snapshot = 20260101

# COMMAND ----------

# DBTITLE 1,Author names
df_apr = table("scopus." + Last_apr )

df_apr_short = (
    df_apr
        .select(
            "auid",
            F.coalesce(
                F.concat(
                    F.col("surname_pn"),
                    F.lit(", "),
                    F.when(
                        F.col("given_name_pn").isNotNull(),
                        F.col("given_name_pn")
                    ).otherwise(F.col("initials_pn"))
                ),
                F.col("indexed_name_pn")
            ).alias("apr_authfull")
        )
)


# COMMAND ----------

# DBTITLE 1,Carrer and Single Year - Top Lists
df_career  = (
    spark.read.parquet(str_path_career)
    .withColumn(
        'top_listed',F.expr(top_list_expression)
    )
    .withColumn('np_d_p',F.col('np_d')/F.col('np'))
)
df_singleyr=(
    spark.read.parquet(str_path_singleyr)
    .withColumn(
        'top_listed',F.expr(top_list_expression)
    )
    .withColumn('np_d_p',F.col('np_d')/F.col('np'))
)

# COMMAND ----------

df_ani_no_preprints = (
    table(f'scopus.ani_{snapshot}')
    .filter(F.arrays_overlap('dbcollections',F.array(F.lit('SCOPUS'),F.lit('MEDL'))))
    )

# COMMAND ----------

#df_career.limit(100).display()

# COMMAND ----------

#df_career.filter(F.col('ns_ord') < 100000).display()

# COMMAND ----------

df_author_rank = ( 
    df_career
    .select(
        'author_id', 
        #F.col('ns_ord').alias('author_rank')  # no self citation order (TODO confirm, misght be with self citations)
        F.least(F.col('ws_ord'),F.col('ns_ord')).alias('author_rank'),  # Get the best rank for now (between with or withouut self citations)  
        'top_listed')
    .dropDuplicates(['author_id']) 
    .withColumnRenamed('author_id', 'auid')
)


# COMMAND ----------

df_author_rank.filter(F.col("author_rank").isNull()).limit(100).display()

# COMMAND ----------

#df_ani_no_preprints.limit(10).display()
#df_ani_no_preprints.select(F.col('source.type')).limit(100).display()


# COMMAND ----------

# DBTITLE 1,Eid - Journal  - Authors

df_auth_journals = (df_ani_no_preprints #srcid = 17592
              #.filter(F.col('source.srcid') == '17592')  # focus on JBC for now
              .filter(F.col('source.type') == 'j')  # journals only ?
              .filter(F.col('citation_type').isin(FULL_DOC_TYPES))
              .filter((F.col('sort_year') >= YEAR_START) & (F.col('sort_year') <= YEAR_END))
              .select(
                  "eid",
                  "sort_year",
                  "source.srcid",
                  "source.sourcetitle",
                  "Au"
              )
              .withColumn("au", F.explode(F.col("Au")))
              .withColumn('auid', F.col('au.auid'))
              .withColumn('au_name', F.col('au.indexed_name'))
              .withColumnRenamed("sort_year", "year")
              .drop("au")
)

df_auth_journals.cache()
df_auth_journals.count()

# COMMAND ----------

df_auth_journals.limit(100).display()

# COMMAND ----------

# Count distinct publications per (journal, author)
df_counts = (
    df_auth_journals
    .groupBy('srcid','sourcetitle','auid')
    .agg(F.countDistinct('eid').alias('n_pubs'))
)

# COMMAND ----------

df_counts.limit(100).display()

# COMMAND ----------

# Just the total journal output
df_journal_total = (
    df_auth_journals
    .groupBy('srcid')
    .agg(F.countDistinct('eid').alias('total_n_pubs'))
)

#df_journal_total.limit(100).display()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

a =df_counts
b = df_author_rank

# COMMAND ----------

w_j = (
    Window
        .partitionBy("srcid")
        .orderBy(
            F.col("n_pubs").desc(),
            F.col("author_rank").asc(),  # TODO beware author rank with ws or ns_)order can have now the same number see : https://elsevier-dev.cloud.databricks.com/editor/notebooks/3215054196824842?o=8907390598234411#command/3215054196824855
            F.col("auid").asc()
        )
)

# order by number of pubs, then author rank, then author id

# df_topY = (
#     df_counts
#         .join(df_author_rank, ["auid"], "left")
#         .withColumn("rownum", F.row_number().over(w_j))
#         .filter(F.col("rownum") <= TOP_Y)
# )

# Journal with top 15 authors by number of publications - if equal by number of rank 
df_topY = (
    df_counts
        .join(df_author_rank, ["auid"], "left")
        .withColumn(
            "author_has_rank",
            F.coalesce(F.col("author_rank") < F.lit(5000000000000), F.lit(False))  # a lil bit more than the actual authors set
        )
        .withColumn("rownum", F.row_number().over(w_j))
        .filter(F.col("rownum") <= TOP_Y)
)

# COMMAND ----------

(df_topY
 .filter(F.col("author_has_rank") == False)
 .limit(100)
 ).display()

# COMMAND ----------

# df_journal_scores = (
#     df_topY
#         .groupBy("srcid", "sourcetitle")
#         .agg(
#             F.expr(
#                 "percentile_approx(author_rank, array(0.25, 0.5, 0.75))"
#             ).alias("q"),
#             F.avg("author_rank").alias("topY_mean_rank"),
#             F.count("*").alias("topY_selected")
#         )
#         .select(
#             "srcid",
#             "sourcetitle",
#             F.col("q")[0].alias("topY_q1_rank"),
#             F.col("q")[1].alias("topY_median_rank"),
#             F.col("q")[2].alias("topY_q3_rank"),
#             "topY_mean_rank",
#             "topY_selected"
#         )
# )


df_journal_scores = (
    df_topY
        .groupBy("srcid", "sourcetitle")
        .agg(
            F.expr(
                "percentile_approx(author_rank, array(0.25, 0.5, 0.75))"
            ).alias("q"),
            F.avg("author_rank").alias("topY_mean_rank"),
            F.min("author_rank").alias("topY_Best_rank"),
            F.count("*").alias("topY_selected"),
            F.sum(
                F.col("author_has_rank").cast("int")
            ).alias("topY_with_rank"),
            F.sum(
                F.col("top_listed").cast("int")
            ).alias("topY_in_TopListed")
        )
        .select(
            "srcid",
            "sourcetitle",
            F.col("q")[0].alias("topY_q1_rank"),
            F.col("q")[1].alias("topY_median_rank"),
            F.col("q")[2].alias("topY_q3_rank"),
            "topY_mean_rank",
            "topY_selected",
            "topY_in_TopListed",
            "topY_with_rank"
        )
        .filter(F.col("topY_selected") > 14)
)

# COMMAND ----------

df_journal_totals = (
    df_auth_journals
        .groupBy("srcid", "sourcetitle")
        .agg(
            F.countDistinct("Eid").alias("total_n_pubs_period"),
            F.countDistinct("auid").alias("total_n_authors_period")
        )
)

# COMMAND ----------

df_journal_rankings = (
    df_journal_scores
        .join(
            df_journal_totals,
            ["srcid", "sourcetitle"],
            "left"
        )
        .withColumn("period_start", F.lit(YEAR_START))
        .withColumn("period_end", F.lit(YEAR_END))
        .withColumn("topY", F.lit(TOP_Y))
        .orderBy(F.asc("topY_median_rank"))
)

df_journal_rankings.cache()
df_journal_rankings.count()

# COMMAND ----------

(df_journal_rankings
 .filter(F.col("topY_with_rank") == 15)
 ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Create some plots

# COMMAND ----------

import matplotlib.pyplot as plt 
import seaborn as sns
import pandas as pd



# COMMAND ----------

#pdf = df_journal_rankings.filter(F.col('topY_with_rank')==15).toPandas()
pdf = df_journal_rankings.toPandas() 
pdf['coverage'] = pdf['topY_in_TopListed'] / pdf['topY_with_rank']

# COMMAND ----------

topN = 20

top20 = (
    pdf
    .sort_values('topY_median_rank', ascending=True)
    .head(topN)
)

# COMMAND ----------

plt.figure(figsize=(8, 6))

plt.barh(
    top20['sourcetitle'],
    top20['topY_median_rank']
)

plt.gca().invert_yaxis()  # best at top
plt.xlabel('Median author rank')
plt.title('Top 20 journals by median rank')

plt.tight_layout()
plt.show()


# COMMAND ----------

plt.figure(figsize=(7, 4))

plt.hist(
    pdf['coverage'].dropna(),
    bins=20
)

plt.xlabel('Coverage = Top 2 % / Number of Ranked Authors')
plt.title('Coverage of Top 2% Authors among ranked authors')

plt.tight_layout()
plt.show()

# COMMAND ----------

(
    df_journal_rankings
    .filter(F.col('topY_with_rank').isNotNull())
    .orderBy('topY_with_rank')
    .select('srcid', 'sourcetitle', 'topY_median_rank', 'topY_in_TopListed','topY_with_rank')
    .limit(20)
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check the top 15 for JBC

# COMMAND ----------

a = df_counts

# COMMAND ----------

df_top15 = (
    df_counts
    
        .filter(F.col('srcid') == '17592') 
        .join(df_author_rank, ["auid"], "left")
        .orderBy(
            F.desc("n_pubs"),
            F.asc("author_rank"),
            F.asc("auid")
        )
        .limit(TOP_Y)
        .select(
            "srcid",
            "sourcetitle",
            "auid",
            "n_pubs",
            "author_rank"
        )
)

df_top15.cache()
df_top15.count()


# COMMAND ----------

(df_top15
.join(df_apr_short, "auid", "left")

).display()
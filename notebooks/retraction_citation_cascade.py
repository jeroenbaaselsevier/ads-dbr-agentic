# Databricks notebook source
# Retraction Citation Cascade — Heliyon (2016–2025)
#
# Research question: How far does the influence of retracted Heliyon papers propagate
# through the citation network?
#
# For each publication year of the retracted paper we count:
#   Order 0 : retracted Heliyon papers published that year
#   Order 1 : distinct non-preprint papers that directly cite any of those retracted papers
#   Order 2–N: each subsequent generation of citing papers
#
# Data   : Scopus ANI snapshot 20250301
# Journal: Heliyon (source ID 21100411756 — https://www.scopus.com/sourceid/21100411756)
# RWDB   : Retraction Watch DB via CrossRef/GitLab

# COMMAND ----------

from pyspark.sql import functions as F
import sys
import os

sys.path.append('/Workspace/rads/library/')
import column_functions
import dataframe_functions
import rwdb_functions

# ── Configuration ─────────────────────────────────────────────────────────────
ani_stamp        = '20250301'
source_srcid     = 21100411756     # Heliyon
years            = (2016, 2025)
max_order        = 4
str_path_project    = '/mnt/els/rads-projects/temporary_to_be_deleted/1d/retraction_citation_cast'
cache_folder        = os.path.join(str_path_project, 'cache')
journal_cache_folder = os.path.join(cache_folder, f'srcid_{source_srcid}')

# COMMAND ----------
# ── 1. Heliyon papers (cached: selective subset of ANI) ───────────────────────

df_journal = dataframe_functions.df_cached(
    spark.table(f'scopus.ani_{ani_stamp}')
        .filter(F.col('source.srcid') == source_srcid)
        .filter(F.col('sort_year').between(years[0], years[1]))
        .filter(column_functions.nopp())
        .select('eid', 'sort_year'),
    os.path.join(journal_cache_folder, 'journal_papers'),
)
print(f'Heliyon papers {years[0]}–{years[1]}: {df_journal.count():,}')

# COMMAND ----------
# ── 2. Retracted Heliyon papers via Retraction Watch ──────────────────────────

rw_cache_folder = os.path.join(str_path_project, 'rw_cache')

# Fetch (or reuse cached) Retraction Watch dump — refreshes if older than 30 days
path_rw_csv = rwdb_functions.check_and_fetch_rwdb_csv(rw_cache_folder)

# Match RWDB to ANI; filters journal errors, withdrawn-out-of-date, retract-and-replace
df_retracted_all = rwdb_functions.get_clean_rw_df(path_rw_csv, ani_stamp, rw_cache_folder)

# Inner-join with our Lancet set (cached: very selective)
df_retracted_journal = dataframe_functions.df_cached(
    df_journal.join(df_retracted_all.select('eid'), 'eid'),
    os.path.join(journal_cache_folder, 'retracted_journal'),
)
print(f'Retracted Heliyon papers {years[0]}–{years[1]}: {df_retracted_journal.count():,}')
df_retracted_journal.groupBy('sort_year').count().orderBy('sort_year').show()

# COMMAND ----------
# ── 3. Citation edge list (non-preprint citing sources only) ──────────────────
# citations is an array of cited EIDs per document (may contain duplicates).
# We explode + distinct to build a clean citing → cited edge table.
# This is large but computed once and reused across all years / orders.

df_ani_full = spark.table(f'scopus.ani_{ani_stamp}')

df_citations = dataframe_functions.df_cached(
    df_ani_full
        .filter(column_functions.nopp())
        .select(
            F.col('eid').alias('citing_eid'),
            F.explode(F.col('citations')).alias('cited_eid'),
        )
        .distinct(),
    os.path.join(cache_folder, 'citation_edges'),
    partitions=50,
)
print(f'Distinct citation edges: {df_citations.count():,}')

# COMMAND ----------
# ── 4. Citation cascade (orders 0–N), all years in parallel ──────────────────
# sort_year is carried through each hop so a single groupBy gives per-year counts.
# Cache is one parquet per order (not per year×order).

from functools import reduce

# Order 0: retracted papers themselves, tagged by publication year
df_current = df_retracted_journal.select('eid', 'sort_year')

order_dfs = [
    df_current
        .groupBy('sort_year')
        .agg(F.countDistinct('eid').alias('n'))
        .withColumn('order', F.lit(0))
]

for order in range(1, max_order + 1):
    df_next = dataframe_functions.df_cached(
        df_citations
            .join(df_current.withColumnRenamed('eid', 'cited_eid'), 'cited_eid')
            .select(F.col('citing_eid').alias('eid'), 'sort_year')
            .distinct(),
        os.path.join(journal_cache_folder, f'cascade/order_{order}'),
    )
    order_dfs.append(
        df_next
            .groupBy('sort_year')
            .agg(F.countDistinct('eid').alias('n'))
            .withColumn('order', F.lit(order))
    )
    print(f'Order {order}: {df_next.count():,} citing papers (across all years)')
    df_current = df_next

# COMMAND ----------
# ── 5. Pivot results into a summary table ─────────────────────────────────────

df_result = reduce(lambda a, b: a.union(b), order_dfs)

df_pivot = (
    df_result
    .groupBy('sort_year')
    .pivot('order', list(range(0, max_order + 1)))
    .agg(F.first('n'))
    .fillna(0)
    .withColumnRenamed('sort_year', 'year')
)

col_map = {0: 'retracted', **{i: f'citing_order_{i}' for i in range(1, max_order + 1)}}
for old, new in col_map.items():
    df_pivot = df_pivot.withColumnRenamed(str(old), new)

df_pivot = df_pivot.orderBy('year')
df_pivot.show(truncate=False)

# COMMAND ----------
# ── 6. Export result table as CSV ─────────────────────────────────────────────

dataframe_functions.export_df_csv(
    df_pivot,
    name=f'retraction_cascade_srcid_{source_srcid}.csv',
    path_storage=str_path_project,
    compressed=False,
    excel_format=True,
)

# Databricks notebook source
# MAGIC %md
# MAGIC # Country co-affiliation analysis
# MAGIC
# MAGIC Examines how often **individual authors list affiliations from two or
# MAGIC more different countries on the same paper**.
# MAGIC
# MAGIC Analyses produced:
# MAGIC 1. **Paper level** – trend over time (1996+) and by SM subfield.
# MAGIC 2. **Author level** – using the 5+ ar/cp/re pool from the top-cited-scholars
# MAGIC    pipeline; trend over time and by SM subfield.
# MAGIC 3. **Country-pair frequencies** – overall, by SM field, and by year (parquet).
# MAGIC 4. **Random 200-paper sample** – for manual audit of the mechanism.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, LongType
)
import sys, os
sys.path.append('/Workspace/rads/library/')
import column_functions, dataframe_functions

# ── Stamps & paths ──────────────────────────────────────────────────────────
ani_stamp          = '20260301'
sm_mapping_date    = '20250801'   # last confirmed-available SM mapping

# Top-cited-scholars author pool (career: 5+ ar/cp/re, full career citations)
author_pool_path = (
    'dbfs:/mnt/els/rads-projects/short_term/2021/2021_top_cited_scholars/'
    '20250801/Table-S1_20250801_1960_1996_2024_1788_ppFalse_minarcpre_5_'
    'minnpY1Y3_2_maxcityear_2024.parquet'
)

str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_country_coaffiliation'
cache_folder     = os.path.join(str_path_project, 'cache')

min_year = 1996

# COMMAND ----------
# MAGIC %md ## 1. Discover available Article_Collaboration_orgdb stamp

# COMMAND ----------

# Prefer the current ANI stamp; fall back to the most recent previously run one
for _stamp in [ani_stamp, '20250801', '20250101']:
    try:
        spark.table(f'fca_ds.ani_eid_orgdb_collab_{_stamp}').limit(1).collect()
        collab_stamp = _stamp
        print(f'Using article_collaboration_orgdb stamp: {collab_stamp}')
        break
    except Exception:
        pass
else:
    raise RuntimeError('No ani_eid_orgdb_collab table found. Run the metrics pipeline first.')

# COMMAND ----------
# MAGIC %md ## 2. Load source tables

# COMMAND ----------

# ── Article_Collaboration_orgdb ──────────────────────────────────────────────
df_collab = spark.table(f'fca_ds.ani_eid_orgdb_collab_{collab_stamp}')
print('collab schema:')
df_collab.printSchema()

# COMMAND ----------

# ── ANI (nopp, 1996+) for sort_year and citation_title ──────────────────────
df_ani_base = (
    spark.table(f'scopus.ani_{ani_stamp}')
    .filter(column_functions.nopp())
    .filter(F.col('sort_year') >= min_year)
)

df_ani_years = df_ani_base.select(
    F.col('Eid').alias('eid'),
    'sort_year'
)

# COMMAND ----------

# ── SM subfield classification at article level ──────────────────────────────
df_smc_labels = spark.read.format('delta').load(
    'dbfs:/mnt/els/rads-main/legacy_sm_hw/rads_pscopus/classification'
)

df_smc_full = (
    spark.read.format('delta')
    .load(
        'dbfs:/mnt/els/rads-main/legacy_sm_hw/rads_pscopus/'
        f'sm_classification_eid_complete_mapping_{sm_mapping_date}'
    )
    .withColumn('subfield_match', F.lower(F.trim('subfield_hybrid')))
    .join(
        df_smc_labels.withColumn('subfield_match', F.lower(F.trim('subfield'))),
        'subfield_match'
    )
    .select(
        F.col('Eid').alias('eid'),
        'Domain', 'Field', 'Subfield'
    )
)

# One (primary) classification per paper
df_smc = (
    df_smc_full
    .groupBy('eid')
    .agg(
        F.first('Domain').alias('Domain'),
        F.first('Field').alias('Field'),
        F.first('Subfield').alias('Subfield'),
    )
)

# COMMAND ----------

# ── Author pool (5+ ar/cp/re career, ~11M authors) ──────────────────────────
df_pool_raw = spark.read.parquet(author_pool_path)
print('Author pool schema:')
df_pool_raw.printSchema()

# COMMAND ----------

# The Table-S1 parquet stores each author's subfields as an array of structs.
# Extract the primary subfield (subfieldRank == 1) and join to the SM labels
# classification table to recover Field and Domain.

df_pool_sub1 = (
    df_pool_raw
    .select('author_id', F.explode('subFields').alias('sf'))
    .filter(F.col('sf.subfieldRank') == 1)
    .select(
        F.col('author_id').alias('auid'),
        F.lower(F.trim(F.col('sf.subfield'))).alias('subfield_key'),
    )
)

# df_smc_labels has columns: Domain, Field, Subfield (and id columns)
# Match on lowercase subfield name
df_smc_labels_keyed = df_smc_labels.withColumn(
    'subfield_key', F.lower(F.trim(F.col('subfield')))
)

df_author_pool = (
    df_pool_sub1
    .join(
        df_smc_labels_keyed.select('subfield_key', 'Subfield', 'Field', 'Domain').distinct(),
        'subfield_key', 'left'
    )
    .select(
        'auid',
        F.col('Subfield').alias('author_subfield'),
        F.col('Field').alias('author_field'),
        F.col('Domain').alias('author_domain'),
    )
)

# Reusable keyset: authors in the 5+ paper pool
df_pool_auids = df_author_pool.select('auid').distinct()

# COMMAND ----------
# MAGIC %md ## 3. Build core intermediate: (eid, auid, countries_array)
# MAGIC
# MAGIC For every (paper, author) pair we collect the set of **distinct countries**
# MAGIC that author is listed under in that paper, using the OrgDB-resolved country
# MAGIC codes already present in Article_Collaboration_orgdb.

# COMMAND ----------

# UDF: generate sorted canonical pairs from an array of countries
@F.udf(returnType=ArrayType(StructType([
    StructField('c1', StringType()),
    StructField('c2', StringType()),
])))
def make_country_pairs(countries):
    """Return all unique ordered pairs (c1 < c2) from the input country list."""
    if not countries:
        return []
    cs = sorted({c.strip() for c in countries if c and c.strip()})
    return [(cs[i], cs[j]) for i in range(len(cs)) for j in range(i + 1, len(cs))]

# COMMAND ----------

# Step 1: explode org array → one row per (eid, org)
# Step 2: explode auids within each org → one row per (eid, auid, country)
# Step 3: deduplicate (eid, auid, country) then aggregate to per-author-per-paper
df_auid_country_raw = (
    df_collab
    .filter(F.col('org').isNotNull())
    .select('eid', F.explode('org').alias('org_item'))
    .select(
        'eid',
        F.col('org_item.country').alias('country'),
        F.col('org_item.auids').alias('auids'),
    )
    .withColumn('country', F.trim(F.col('country')))
    .filter(F.col('country').isNotNull() & (F.col('country') != ''))
    .withColumn('auid', F.explode('auids'))
    .filter((F.col('auid').isNotNull()) & (F.col('auid') != 0))
    .select('eid', 'auid', 'country')
    .distinct()
)

# Cache at (eid, auid, countries) level — ~several hundred million rows before dedup
df_author_countries = dataframe_functions.df_cached(
    df_auid_country_raw
    .groupBy('eid', 'auid')
    .agg(
        F.collect_set('country').alias('countries'),
        F.countDistinct('country').alias('country_count'),
    ),
    str_path=os.path.join(cache_folder, f'author_countries_per_paper_{collab_stamp}'),
    partitions=150,
)

# COMMAND ----------

print('Rows in author_countries:', df_author_countries.count())
print('Multi-country author-paper pairs:',
      df_author_countries.filter('country_count >= 2').count())

# COMMAND ----------
# MAGIC %md ## 4. Paper-level analysis

# COMMAND ----------

# Flag papers where ≥1 author has 2+ countries (all authors)
df_paper_multi_flag_any = (
    df_author_countries
    .filter('country_count >= 2')
    .groupBy('eid')
    .agg(
        F.count('*').alias('n_multi_country_authors_any'),
    )
    .withColumn('has_multi_country_author', F.lit(True))
)

# Sensitivity flag: papers where ≥1 MCA author is in the 5+ pool
df_paper_multi_flag_pool = (
    df_author_countries
    .filter('country_count >= 2')
    .join(df_pool_auids, 'auid', 'inner')
    .groupBy('eid')
    .agg(
        F.count('*').alias('n_multi_country_authors_pool'),
    )
    .withColumn('has_multi_country_author_pool', F.lit(True))
)

# Join multi-country flag to nopp + 1996+ ANI universe
df_paper_analysis = dataframe_functions.df_cached(
    df_ani_years
    .join(df_paper_multi_flag_any, 'eid', 'left')
    .join(df_paper_multi_flag_pool, 'eid', 'left')
    .withColumn(
        'has_multi_country_author',
        F.coalesce(F.col('has_multi_country_author'), F.lit(False))
    )
    .withColumn(
        'has_multi_country_author_pool',
        F.coalesce(F.col('has_multi_country_author_pool'), F.lit(False))
    )
    .join(df_smc, 'eid', 'left'),
    str_path=os.path.join(cache_folder, f'paper_analysis_{ani_stamp}_v2_pool_sensitivity'),
    partitions=100,
)

# COMMAND ----------
# MAGIC %md ### 4a. Paper trend by year

# COMMAND ----------

df_paper_year = dataframe_functions.df_cached(
    df_paper_analysis
    .groupBy('sort_year')
    .agg(
        F.count('*').alias('total_papers'),
        F.sum(F.col('has_multi_country_author').cast(LongType())).alias('papers_with_mca'),
        F.sum(F.col('has_multi_country_author_pool').cast(LongType())).alias('papers_with_mca_pool'),
    )
    .withColumn('pct_papers_mca', F.round(
        100.0 * F.col('papers_with_mca') / F.col('total_papers'), 4
    ))
    .withColumn('pct_papers_mca_pool', F.round(
        100.0 * F.col('papers_with_mca_pool') / F.col('total_papers'), 4
    ))
    .withColumn('delta_pct_pool_vs_any', F.round(
        F.col('pct_papers_mca_pool') - F.col('pct_papers_mca'), 4
    ))
    .orderBy('sort_year'),
    str_path=os.path.join(cache_folder, f'paper_year_{ani_stamp}_v2_pool_sensitivity'),
    partitions=1,
)

display(df_paper_year)

# COMMAND ----------
# MAGIC %md ### 4b. Paper breakdown by SM subfield (all years combined)

# COMMAND ----------

df_paper_subfield = dataframe_functions.df_cached(
    df_paper_analysis
    .groupBy('Domain', 'Field', 'Subfield')
    .agg(
        F.count('*').alias('total_papers'),
        F.sum(F.col('has_multi_country_author').cast(LongType())).alias('papers_with_mca'),
        F.sum(F.col('has_multi_country_author_pool').cast(LongType())).alias('papers_with_mca_pool'),
    )
    .withColumn('pct_papers_mca', F.round(
        100.0 * F.col('papers_with_mca') / F.col('total_papers'), 4
    ))
    .withColumn('pct_papers_mca_pool', F.round(
        100.0 * F.col('papers_with_mca_pool') / F.col('total_papers'), 4
    ))
    .withColumn('delta_pct_pool_vs_any', F.round(
        F.col('pct_papers_mca_pool') - F.col('pct_papers_mca'), 4
    ))
    .orderBy(F.col('pct_papers_mca').desc()),
    str_path=os.path.join(cache_folder, f'paper_subfield_{ani_stamp}_v2_pool_sensitivity'),
    partitions=1,
)

display(df_paper_subfield)

# COMMAND ----------
# MAGIC %md ### 4c. Paper trend by year × SM Field (22 fields)

# COMMAND ----------

df_paper_year_field = dataframe_functions.df_cached(
    df_paper_analysis
    .filter(F.col('Field').isNotNull())
    .groupBy('sort_year', 'Domain', 'Field')
    .agg(
        F.count('*').alias('total_papers'),
        F.sum(F.col('has_multi_country_author').cast(LongType())).alias('papers_with_mca'),
        F.sum(F.col('has_multi_country_author_pool').cast(LongType())).alias('papers_with_mca_pool'),
    )
    .withColumn('pct_papers_mca', F.round(
        100.0 * F.col('papers_with_mca') / F.col('total_papers'), 4
    ))
    .withColumn('pct_papers_mca_pool', F.round(
        100.0 * F.col('papers_with_mca_pool') / F.col('total_papers'), 4
    ))
    .withColumn('delta_pct_pool_vs_any', F.round(
        F.col('pct_papers_mca_pool') - F.col('pct_papers_mca'), 4
    ))
    .orderBy('sort_year', 'Field'),
    str_path=os.path.join(cache_folder, f'paper_year_field_{ani_stamp}_v2_pool_sensitivity'),
    partitions=5,
)

# COMMAND ----------
# MAGIC %md ### 4d. Paper trend by year × SM Subfield (~170 subfields)

# COMMAND ----------

df_paper_year_subfield = dataframe_functions.df_cached(
    df_paper_analysis
    .filter(F.col('Subfield').isNotNull())
    .groupBy('sort_year', 'Domain', 'Field', 'Subfield')
    .agg(
        F.count('*').alias('total_papers'),
        F.sum(F.col('has_multi_country_author').cast(LongType())).alias('papers_with_mca'),
        F.sum(F.col('has_multi_country_author_pool').cast(LongType())).alias('papers_with_mca_pool'),
    )
    .withColumn('pct_papers_mca', F.round(
        100.0 * F.col('papers_with_mca') / F.col('total_papers'), 4
    ))
    .withColumn('pct_papers_mca_pool', F.round(
        100.0 * F.col('papers_with_mca_pool') / F.col('total_papers'), 4
    ))
    .withColumn('delta_pct_pool_vs_any', F.round(
        F.col('pct_papers_mca_pool') - F.col('pct_papers_mca'), 4
    ))
    .orderBy('sort_year', 'Subfield'),
    str_path=os.path.join(cache_folder, f'paper_year_subfield_{ani_stamp}_v2_pool_sensitivity'),
    partitions=10,
)

# COMMAND ----------
# MAGIC %md ## 5. Author-level analysis (5+ ar/cp/re pool)

# COMMAND ----------

# For each pool author: how many of their papers (1996+, nopp) show multi-country?
df_author_multi_papers = (
    df_author_countries
    .filter('country_count >= 2')
    .join(df_ani_years, 'eid', 'inner')   # restrict to nopp + 1996+ papers
    .groupBy('auid')
    .agg(
        F.countDistinct('eid').alias('n_multi_country_papers'),
        F.min('sort_year').alias('first_mca_year'),
        F.max('sort_year').alias('last_mca_year'),
    )
)

# Merge with pool → every pool author gets a row (null = never had mca)
df_author_summary = dataframe_functions.df_cached(
    df_author_pool
    .join(df_author_multi_papers, 'auid', 'left')
    .withColumn('has_mca_paper', F.col('n_multi_country_papers').isNotNull()),
    str_path=os.path.join(cache_folder, f'author_summary_{collab_stamp}'),
    partitions=50,
)

# COMMAND ----------

n_pool  = df_author_summary.count()
n_multi = df_author_summary.filter('has_mca_paper').count()
print(f'Total authors in 5+ pool : {n_pool:,}')
print(f'Authors with ≥1 MCA paper : {n_multi:,}  ({100*n_multi/n_pool:.2f}%)')

# COMMAND ----------
# MAGIC %md ### 5a. Author subfield breakdown

# COMMAND ----------

df_author_subfield = dataframe_functions.df_cached(
    df_author_summary
    .groupBy('author_domain', 'author_field', 'author_subfield')
    .agg(
        F.count('*').alias('total_authors'),
        F.sum(F.col('has_mca_paper').cast(LongType())).alias('authors_with_mca'),
    )
    .withColumn('pct_authors_mca', F.round(
        100.0 * F.col('authors_with_mca') / F.col('total_authors'), 4
    ))
    .orderBy(F.col('pct_authors_mca').desc()),
    str_path=os.path.join(cache_folder, f'author_subfield_{collab_stamp}'),
    partitions=1,
)

display(df_author_subfield)

# COMMAND ----------
# MAGIC %md ### 5b. Author time trend
# MAGIC Per year: how many pool authors had at least one MCA paper published in that year?

# COMMAND ----------

df_denom_by_year = (
    df_author_countries
    .join(df_ani_years, 'eid', 'inner')
    .join(df_pool_auids, 'auid', 'inner')
    .groupBy('sort_year')
    .agg(F.countDistinct('auid').alias('pool_authors_active'))
)

# Numerator: pool authors with at least 1 MCA paper in that year
df_numer_by_year = (
    df_author_countries
    .filter('country_count >= 2')
    .join(df_ani_years, 'eid', 'inner')
    .join(df_pool_auids, 'auid', 'inner')
    .groupBy('sort_year')
    .agg(F.countDistinct('auid').alias('pool_authors_with_mca'))
)

df_author_year = dataframe_functions.df_cached(
    df_denom_by_year
    .join(df_numer_by_year, 'sort_year', 'left')
    .withColumn('pool_authors_with_mca',
                F.coalesce('pool_authors_with_mca', F.lit(0)))
    .withColumn('pct_authors_mca', F.round(
        100.0 * F.col('pool_authors_with_mca') / F.col('pool_authors_active'), 4
    ))
    .orderBy('sort_year'),
    str_path=os.path.join(cache_folder, f'author_year_{collab_stamp}'),
    partitions=1,
)

display(df_author_year)

# COMMAND ----------
# MAGIC %md ### 5d. Author time trend by year × SM Subfield
# MAGIC Per year × subfield: pool authors active vs. those with ≥1 MCA paper.

# COMMAND ----------

# Denominator per (year, subfield): pool authors who published in that year
df_denom_by_year_sf = (
    df_author_countries
    .join(df_ani_years, 'eid', 'inner')
    .join(df_author_pool.select('auid', 'author_subfield', 'author_field', 'author_domain'), 'auid', 'inner')
    .groupBy('sort_year', 'author_domain', 'author_field', 'author_subfield')
    .agg(F.countDistinct('auid').alias('pool_authors_active'))
)

# Numerator per (year, subfield): pool authors with ≥1 MCA paper that year
df_numer_by_year_sf = (
    df_author_countries
    .filter('country_count >= 2')
    .join(df_ani_years, 'eid', 'inner')
    .join(df_author_pool.select('auid', 'author_subfield', 'author_field', 'author_domain'), 'auid', 'inner')
    .groupBy('sort_year', 'author_domain', 'author_field', 'author_subfield')
    .agg(F.countDistinct('auid').alias('pool_authors_with_mca'))
)

df_author_year_subfield = dataframe_functions.df_cached(
    df_denom_by_year_sf
    .join(df_numer_by_year_sf,
          ['sort_year', 'author_domain', 'author_field', 'author_subfield'], 'left')
    .withColumn('pool_authors_with_mca',
                F.coalesce('pool_authors_with_mca', F.lit(0)))
    .withColumn('pct_authors_mca', F.round(
        100.0 * F.col('pool_authors_with_mca') / F.col('pool_authors_active'), 4
    ))
    .orderBy('sort_year', 'author_subfield'),
    str_path=os.path.join(cache_folder, f'author_year_subfield_{collab_stamp}'),
    partitions=10,
)

# COMMAND ----------
# MAGIC %md ### 5d. Year of first MCA paper (cohort view)

# COMMAND ----------

df_author_first_mca = dataframe_functions.df_cached(
    df_author_summary
    .filter('has_mca_paper')
    .groupBy('first_mca_year', 'author_domain', 'author_field')
    .agg(F.count('*').alias('authors_first_mca_that_year'))
    .orderBy('first_mca_year', 'author_field'),
    str_path=os.path.join(cache_folder, f'author_first_mca_year_{collab_stamp}'),
    partitions=1,
)

display(df_author_first_mca)

# COMMAND ----------
# MAGIC %md ## 6. Country-pair analysis

# COMMAND ----------

# Expand multi-country author-paper records into canonical pairs
df_pairs_base = dataframe_functions.df_cached(
    df_author_countries
    .filter('country_count >= 2')
    .join(df_ani_years, 'eid', 'inner')
    .join(df_smc, 'eid', 'left')
    .withColumn('pair', F.explode(make_country_pairs(F.col('countries'))))
    .select(
        'eid', 'auid', 'sort_year',
        'Domain', 'Field', 'Subfield',
        F.col('pair.c1').alias('country1'),
        F.col('pair.c2').alias('country2'),
    ),
    str_path=os.path.join(cache_folder, f'pairs_base_{ani_stamp}'),
    partitions=100,
)

# COMMAND ----------
# MAGIC %md ### 6a. Overall country-pair frequencies (all years, all fields)

# COMMAND ----------

df_country_pairs_overall = dataframe_functions.df_cached(
    df_pairs_base
    .groupBy('country1', 'country2')
    .agg(
        F.count('*').alias('n_author_paper_events'),
        F.countDistinct('eid').alias('n_papers'),
        F.countDistinct('auid').alias('n_authors'),
    )
    .orderBy(F.col('n_author_paper_events').desc()),
    str_path=os.path.join(cache_folder, f'country_pairs_overall_{ani_stamp}'),
    partitions=5,
)

display(df_country_pairs_overall.limit(50))

# COMMAND ----------
# MAGIC %md ### 6b. Country-pair frequencies by SM Field

# COMMAND ----------

df_country_pairs_field = dataframe_functions.df_cached(
    df_pairs_base
    .filter(F.col('Field').isNotNull())
    .groupBy('Domain', 'Field', 'country1', 'country2')
    .agg(
        F.count('*').alias('n_author_paper_events'),
        F.countDistinct('eid').alias('n_papers'),
        F.countDistinct('auid').alias('n_authors'),
    )
    .orderBy('Field', F.col('n_author_paper_events').desc()),
    str_path=os.path.join(cache_folder, f'country_pairs_field_{ani_stamp}'),
    partitions=10,
)

# COMMAND ----------
# MAGIC %md ### 6c. Country-pair trend by year (full: written as parquet for local post-processing)

# COMMAND ----------

df_country_pairs_year = dataframe_functions.df_cached(
    df_pairs_base
    .groupBy('sort_year', 'country1', 'country2')
    .agg(
        F.count('*').alias('n_author_paper_events'),
        F.countDistinct('eid').alias('n_papers'),
        F.countDistinct('auid').alias('n_authors'),
    )
    .orderBy('sort_year', F.col('n_author_paper_events').desc()),
    str_path=os.path.join(cache_folder, f'country_pairs_year_{ani_stamp}'),
    partitions=20,
)

# COMMAND ----------
# MAGIC %md ## 7. Random 200-paper sample for manual audit

# COMMAND ----------

df_sample = (
    df_author_countries
    .filter('country_count >= 2')
    .join(df_ani_years, 'eid', 'inner')
    .join(
        spark.table(f'scopus.ani_{ani_stamp}')
        .filter(column_functions.nopp())
        .select(
            F.col('Eid').alias('eid'),
            # citation_title is ARRAY<STRUCT<title,lang,original>>; pick element [0].title
            F.col('citation_title')[0]['title'].alias('title'),
            'doi',
            F.col('source.sourcetitle').alias('journal'),
        ),
        'eid', 'inner'
    )
    .join(df_smc.select('eid', 'Field', 'Subfield'), 'eid', 'left')
    .select(
        'eid', 'sort_year', 'title', 'doi', 'journal',
        'Field', 'Subfield',
        'auid',
        F.array_join('countries', '|').alias('countries'),
        'country_count',
    )
    .orderBy(F.rand(seed=42))
    .limit(200)
)

dataframe_functions.export_df_csv(
    df_sample,
    name='sample_200_papers_multi_country_author',
    path_storage=str_path_project,
    compressed=False,
    excel_format=True,
)

# COMMAND ----------
# MAGIC %md ## 8. Country-level summary (for null model analysis)

# COMMAND ----------

# Per country: total authors appearing with that country + mca authors
df_author_by_country = (
    df_author_countries
    .select('auid', F.explode('countries').alias('country'))
    .groupBy('country')
    .agg(
        F.countDistinct('auid').alias('total_authors'),
    )
)

df_mca_author_by_country = (
    df_author_countries
    .filter('country_count >= 2')
    .select('auid', F.explode('countries').alias('country'))
    .groupBy('country')
    .agg(
        F.countDistinct('auid').alias('authors_with_mca'),
    )
)

df_country_summary = dataframe_functions.df_cached(
    df_author_by_country
    .join(df_mca_author_by_country, 'country', 'left')
    .withColumn('authors_with_mca', 
                F.coalesce('authors_with_mca', F.lit(0)))
    .withColumn('pct_authors_mca',
                F.round(100.0 * F.col('authors_with_mca') / F.col('total_authors'), 4))
    .orderBy(F.col('authors_with_mca').desc()),
    str_path=os.path.join(cache_folder, f'country_summary_{collab_stamp}'),
    partitions=1,
)

display(df_country_summary.limit(30))

# COMMAND ----------
# MAGIC %md ## 9. Export all summary tables as CSV

# COMMAND ----------

for _df, _name in [
    (df_paper_year,              'paper_trend_by_year'),
    (df_paper_subfield,          'paper_breakdown_by_subfield'),
    (df_paper_year_field,        'paper_trend_by_year_and_field'),
    (df_paper_year_subfield,     'paper_trend_by_year_and_subfield'),
    (df_author_year,             'author_trend_by_year'),
    (df_author_subfield,         'author_breakdown_by_subfield'),
    (df_author_year_subfield,    'author_trend_by_year_and_subfield'),
    (df_author_first_mca,        'author_first_mca_cohort_by_year_field'),
    (df_country_summary,         'country_summary'),
    (df_country_pairs_overall,   'country_pairs_overall'),
    (df_country_pairs_field,     'country_pairs_by_field'),
    (df_country_pairs_year,      'country_pairs_by_year'),
]:
    dataframe_functions.export_df_csv(
        _df, name=_name,
        path_storage=str_path_project,
        compressed=False,
        excel_format=True,
    )
    print(f'Exported: {_name}')

# COMMAND ----------
# MAGIC %md ## 9. Summary stats

# COMMAND ----------

total_papers = df_paper_year.agg(F.sum('total_papers')).collect()[0][0]
mca_papers   = df_paper_year.agg(F.sum('papers_with_mca')).collect()[0][0]
mca_papers_pool = df_paper_year.agg(F.sum('papers_with_mca_pool')).collect()[0][0]
print(f'Papers (nopp, 1996–, all years)')
print(f'  Total             : {total_papers:,}')
print(f'  With MCA author   : {mca_papers:,}  ({100*mca_papers/total_papers:.2f}%)')
print(f'  With MCA author (pool-only sensitivity) : {mca_papers_pool:,}  ({100*mca_papers_pool/total_papers:.2f}%)')
print(f'  Gap vs all-authors indicator            : {mca_papers_pool - mca_papers:,} papers  ({100*(mca_papers_pool - mca_papers)/total_papers:.2f} pp)')
print()
n_pool_total = df_author_summary.count()
n_pool_mca   = df_author_summary.filter('has_mca_paper').count()
print(f'Authors in 5+ pool')
print(f'  Total             : {n_pool_total:,}')
print(f'  With ≥1 MCA paper : {n_pool_mca:,}  ({100*n_pool_mca/n_pool_total:.2f}%)')
print()
top_pairs = df_country_pairs_overall.limit(5).collect()
print('Top 5 country pairs (by author-paper events):')
for r in top_pairs:
    print(f'  {r.country1.upper()} – {r.country2.upper()} : '
          f'{r.n_author_paper_events:,} events, '
          f'{r.n_papers:,} papers, '
          f'{r.n_authors:,} distinct authors')

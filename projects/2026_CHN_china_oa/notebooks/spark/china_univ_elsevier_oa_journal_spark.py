# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys
import os

sys.path.append('/Workspace/rads/library/')
import column_functions
import dataframe_functions
import snapshot_functions

ani_stamp = '20260301'
str_path_project = '/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_china_univ_elsevier_oa_journal'
cache_folder = os.path.join(str_path_project, 'cache_v1')

input_university_csv = 'dbfs:/tmp/china_strategy/china_universities_260318.csv'

# COMMAND ----------
# Load target institution list from the email attachment.
df_target_institutions = (
    spark.read.option('header', True).csv(input_university_csv)
    .select(
        F.col('institution_id').cast('long').alias('institution_id'),
        F.col('institution_name').alias('requested_institution_name'),
        F.col('sector'),
        F.col('country_region'),
    )
    .dropna(subset=['institution_id'])
    .dropDuplicates(['institution_id'])
)

print(f'Target institutions in input file: {df_target_institutions.count():,}')

# COMMAND ----------
# SciVal institution mapping: institution_id -> afid.
df_scival_institution = snapshot_functions.scival.get_table('institution').select(
    F.col('institution_id').cast('long').alias('institution_id'),
    F.col('afid').cast('long').alias('afid'),
    F.col('institution_name').alias('scival_institution_name'),
)

df_scival_metadata = snapshot_functions.scival.get_table('institution_metadata').select(
    F.col('institutionId').cast('long').alias('institution_id'),
    F.col('institutionName').alias('institution_name_clean'),
)

# Keep only afids for requested institutions.
df_target_afids = (
    df_target_institutions
    .join(df_scival_institution, ['institution_id'], 'left')
    .join(df_scival_metadata, ['institution_id'], 'left')
    .select(
        'institution_id',
        F.coalesce('institution_name_clean', 'requested_institution_name', 'scival_institution_name').alias('institution_name'),
        'sector',
        'country_region',
        'afid',
    )
    .dropna(subset=['afid'])
    .dropDuplicates(['institution_id', 'afid'])
)

df_target_afids = dataframe_functions.df_cached(
    df_target_afids,
    os.path.join(cache_folder, 'target_afids'),
    partitions=8,
)

print(f'Target institution-afid links: {df_target_afids.count():,}')

# COMMAND ----------
# ANI base: mandatory nopp() first, then time window.
df_ani_base = (
    spark.table(f'scopus.ani_{ani_stamp}')
    .filter(column_functions.nopp())
    .filter(F.col('sort_year').between(2023, 2025))
    .select(
        F.col('Eid').alias('eid'),
        'sort_year',
        'Af',
        'free_to_read_status_list',
        F.col('source.srcid').cast('string').alias('srcid'),
        F.col('source.sourcetitle').alias('source_title_ani'),
        F.col('source.publishername').alias('publisher_ani'),
    )
)

df_ani_base = dataframe_functions.df_cached(
    df_ani_base,
    os.path.join(cache_folder, 'ani_base_2023_2025'),
    partitions=32,
)

print(f'ANI base rows: {df_ani_base.count():,}')

# COMMAND ----------
# Map each paper to institutions via afid; de-duplicate to one row per eid+institution.
df_eid_institution = (
    df_ani_base
    .select('eid', F.explode_outer('Af').alias('af'))
    .select('eid', F.col('af.afid').cast('long').alias('afid'))
    .dropna(subset=['afid'])
    .join(df_target_afids, ['afid'], 'inner')
    .select('eid', 'institution_id', 'institution_name', 'sector', 'country_region')
    .dropDuplicates(['eid', 'institution_id'])
)

df_eid_institution = dataframe_functions.df_cached(
    df_eid_institution,
    os.path.join(cache_folder, 'eid_institution_links'),
    partitions=32,
)

print(f'EID-institution links: {df_eid_institution.count():,}')

# COMMAND ----------
# Join ANI paper data to institution links.
df_papers = (
    df_eid_institution
    .join(df_ani_base.drop('Af'), ['eid'], 'inner')
)

df_papers = dataframe_functions.df_cached(
    df_papers,
    os.path.join(cache_folder, 'papers_in_target_institutions'),
    partitions=32,
)

print(f'Institution-paper rows: {df_papers.count():,}')

# COMMAND ----------
# Rosetta enrichment: load once, filter for Elsevier publishers.
df_rosetta = snapshot_functions.rosetta.get_table(current_only=True).select(
    F.col('srcid').cast('string').alias('srcid'),
    F.col('publisher').alias('publisher_rosetta'),
    F.col('title').alias('source_title_rosetta'),
).filter(F.lower(F.coalesce(F.col('publisher_rosetta'), F.lit(''))).contains('elsevier'))

df_papers_enriched = (
    df_papers
    .join(df_rosetta, ['srcid'], 'left')
    .withColumn('publisher_norm', F.coalesce('publisher_rosetta', 'publisher_ani'))
    .withColumn('source_title', F.coalesce('source_title_rosetta', 'source_title_ani'))
)

# OA tag mapping copied from scd_functions.csv_formatted_from_selected_ani.
oa_tags = F.array_distinct(
    F.transform(
        F.filter(
            F.coalesce(F.col('free_to_read_status_list'), F.array().cast('array<string>')),
            lambda x: x != F.lit('all'),
        ),
        lambda x: (
            F.when(x == F.lit('repositoryam'), F.lit('Green'))
            .when(x == F.lit('repositoryvor'), F.lit('Green'))
            .when(x == F.lit('repository'), F.lit('Green'))
            .when(x == F.lit('publisherfullgold'), F.lit('Gold'))
            .when(x == F.lit('publisherhybridgold'), F.lit('Gold'))
            .when(x == F.lit('publisherfree2read'), F.lit('Bronze'))
            .otherwise(x)
        ),
    )
)

df_papers_enriched = (
    df_papers_enriched
    .withColumn('oa_tags_mapped', oa_tags)
    .withColumn(
        'oa_tag',
        F.when(F.array_contains(F.col('oa_tags_mapped'), F.lit('Gold')), F.lit('Gold'))
        .when(F.array_contains(F.col('oa_tags_mapped'), F.lit('Green')), F.lit('Green'))
        .when(F.array_contains(F.col('oa_tags_mapped'), F.lit('Bronze')), F.lit('Bronze'))
        .when(F.size(F.col('oa_tags_mapped')) > 0, F.lit('Other OA'))
        .otherwise(F.lit('Closed/Unknown')),
    )
)

df_papers_enriched = dataframe_functions.df_cached(
    df_papers_enriched,
    os.path.join(cache_folder, 'papers_elsevier_with_oa'),
    partitions=32,
)

print(f'Elsevier institution-paper rows: {df_papers_enriched.count():,}')

# COMMAND ----------
# Main requested output: aggregate by institution x journal x year, with OA tag.
df_journal_inst_agg = (
    df_papers_enriched
    .groupBy(
        'institution_id',
        'institution_name',
        'sector',
        'country_region',
        'sort_year',
        'srcid',
        'source_title',
        'publisher_norm',
        'oa_tag',
    )
    .agg(F.countDistinct('eid').alias('article_count'))
    .orderBy('institution_name', 'sort_year', F.desc('article_count'))
)

df_journal_inst_agg = dataframe_functions.df_cached(
    df_journal_inst_agg,
    os.path.join(cache_folder, 'journal_institution_year_oa_agg'),
    partitions=16,
)

# COMMAND ----------
# Secondary summary for revenue proxy checks at institution-year level.
df_inst_year_summary = (
    df_papers_enriched
    .groupBy('institution_id', 'institution_name', 'sector', 'country_region', 'sort_year')
    .agg(
        F.countDistinct('eid').alias('elsevier_total_articles'),
        F.countDistinct(F.when(F.col('oa_tag') != F.lit('Closed/Unknown'), F.col('eid'))).alias('elsevier_oa_articles'),
        F.countDistinct(F.when(F.col('oa_tag') == F.lit('Gold'), F.col('eid'))).alias('elsevier_gold_articles'),
        F.countDistinct(F.when(F.col('oa_tag') == F.lit('Green'), F.col('eid'))).alias('elsevier_green_articles'),
        F.countDistinct(F.when(F.col('oa_tag') == F.lit('Bronze'), F.col('eid'))).alias('elsevier_bronze_articles'),
    )
    .orderBy('institution_name', 'sort_year')
)

# COMMAND ----------
# Export CSV deliverables.
output_path = os.path.join(str_path_project, 'output')

dataframe_functions.export_df_csv(
    df_journal_inst_agg,
    name='china_elsevier_journal_by_university_2023_2025',
    path_storage=output_path,
    compressed=True,
    partitions=1,
    excel_format=True,
)

dataframe_functions.export_df_csv(
    df_inst_year_summary,
    name='china_elsevier_university_year_summary_2023_2025',
    path_storage=output_path,
    compressed=True,
    partitions=1,
    excel_format=True,
)

# COMMAND ----------
print('Export complete.')
print(f'Project path: {str_path_project}')
print('Files written under: <project>/output')
print(f'Rows in journal-level aggregate: {df_journal_inst_agg.count():,}')
print(f'Rows in institution-year summary: {df_inst_year_summary.count():,}')

display(df_journal_inst_agg.limit(20))

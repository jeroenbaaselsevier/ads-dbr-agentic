# Databricks notebook source
# MAGIC %md
# MAGIC # Export Slim Retraction Modeling Input (No Regression)
# MAGIC
# MAGIC This notebook only prepares and exports a compact transformed dataset for local modeling.
# MAGIC It does not fit any logistic regression models.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

# COMMAND ----------

AUTHOR_FINAL_PATH = "/mnt/els/rads-users/robergeg/gender_retractions/df_author_final_20240801"
AUTHOR_COUNTRY_HISTORY_TABLE = "fca_ds.author_country_history_ani_20240801"

OUTPUT_BASE = "/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_retraction/output/retraction_or_tables_v3_20260320"
LOCAL_EXPORT_BASE = f"{OUTPUT_BASE}/local_model_input"

LOCAL_EXPORT_COLUMNS = [
    "label",
    "gender_clean",
    "career_age_clean",
    "income_clean",
    "field_clean",
    "top_cited_yes",
    "papers_raw",
    "papers_log10",
    "paper_years_raw",
    "paper_years_log10",
]

# COMMAND ----------


def _extract_primary_category(df, col_name, all_label):
    dtype = next((f.dataType for f in df.schema.fields if f.name == col_name), None)
    if isinstance(dtype, ArrayType):
        return F.expr(f"filter({col_name}, x -> x != '{all_label}')[0]")
    return F.col(col_name)


def load_author_base(author_final_path):
    df = spark.read.parquet(author_final_path)
    df = (
        df
        .withColumn("gender_clean", _extract_primary_category(df, "gender", "All Genders"))
        .withColumn("career_age_clean", _extract_primary_category(df, "career_age", "All Career Ages"))
        .withColumn("income_clean", _extract_primary_category(df, "binary_income_groups", "All Income Levels"))
        .withColumn("field_clean", F.trim(_extract_primary_category(df, "field_sm", "All Fields SM")))
        .withColumn("citation_group_clean", _extract_primary_category(df, "citation_group", "All Citation Groups"))
        .withColumn("label", F.col("has_retraction").cast("double"))
        .withColumn(
            "top_cited_yes",
            F.when(F.col("citation_group_clean") == F.lit("Highly Cited Selection"), F.lit(1.0)).otherwise(F.lit(0.0)),
        )
        .select(
            "auid",
            "label",
            F.lower(F.col("gender_clean")).alias("gender_clean"),
            "career_age_clean",
            F.coalesce(F.col("income_clean"), F.lit("unknown")).alias("income_clean"),
            "field_clean",
            F.col("np").cast("double").alias("np"),
            "top_cited_yes",
        )
    )
    return df.filter(F.col("label").isNotNull())


def add_publication_columns(df):
    ach = spark.table(AUTHOR_COUNTRY_HISTORY_TABLE)
    dpy = (
        ach
        .select("auid", F.explode("country_history").alias("ch"))
        .select("auid", F.col("ch.sort_year").alias("sort_year"))
        .distinct()
        .groupBy("auid")
        .agg(F.count("*").cast("double").alias("paper_years_raw"))
    )

    return (
        df
        .join(dpy, ["auid"], "left")
        .withColumn("papers_raw", F.col("np").cast("double"))
        .withColumn("papers_log10", F.log10(F.greatest(F.col("papers_raw"), F.lit(1.0))))
        .withColumn(
            "paper_years_log10",
            F.when(
                F.col("paper_years_raw").isNotNull(),
                F.log10(F.greatest(F.col("paper_years_raw"), F.lit(1.0))),
            ),
        )
    )


def export_local_model_inputs(df):
    slim_path = f"{LOCAL_EXPORT_BASE}/row_level_parquet"
    grouped_path = f"{LOCAL_EXPORT_BASE}/grouped_parquet"

    slim_df = df.select(*LOCAL_EXPORT_COLUMNS)
    grouped_df = slim_df.groupBy(*LOCAL_EXPORT_COLUMNS).agg(F.count("*").cast("long").alias("n_obs"))

    slim_df.write.mode("overwrite").parquet(slim_path)
    grouped_df.write.mode("overwrite").parquet(grouped_path)

    print(f"Local row-level export saved to: {slim_path}")
    print(f"Local grouped export saved to: {grouped_path}")
    print("Local row-level rows:", slim_df.count())
    print("Local grouped rows:", grouped_df.count())


# COMMAND ----------

base_df = load_author_base(AUTHOR_FINAL_PATH)
base_df = add_publication_columns(base_df)

# Keep broad coverage for local model-specific filtering.
# Do not globally drop rows by predictor completeness here.
analysis_df_base = base_df

print("Rows in analysis_df_base:", analysis_df_base.count())
print("Non-null paper-years:", analysis_df_base.filter(F.col("paper_years_raw").isNotNull()).count())
print("Non-null gender:", analysis_df_base.filter(F.col("gender_clean").isNotNull()).count())
print("Non-null career_age:", analysis_df_base.filter(F.col("career_age_clean").isNotNull()).count())
print("Non-null field:", analysis_df_base.filter(F.col("field_clean").isNotNull()).count())

export_local_model_inputs(analysis_df_base)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Outputs
# MAGIC
# MAGIC - `LOCAL_EXPORT_BASE/row_level_parquet`
# MAGIC - `LOCAL_EXPORT_BASE/grouped_parquet` (includes `n_obs`)

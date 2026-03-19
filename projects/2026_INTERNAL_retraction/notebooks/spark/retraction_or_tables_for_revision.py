# Databricks notebook source
# MAGIC %md
# MAGIC # Retraction OR Tables for Paper Revision
# MAGIC
# MAGIC This notebook computes univariable and multivariable logistic regression
# MAGIC outputs (OR and 95% CI) for retraction risk, without exporting author-level data.
# MAGIC
# MAGIC Outputs:
# MAGIC - one table for all authors
# MAGIC - one table for top-cited authors only

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
import math
import json

# COMMAND ----------

# Paths from prior retraction work
AUTHOR_FINAL_PATH = "/mnt/els/rads-users/robergeg/gender_retractions/df_author_final"
OUTPUT_BASE = "/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_retraction/output/retraction_or_tables_paperyears_20260319"

# Parquet cache for expensive intermediates.
# On first run these are computed and written. On reruns they are read directly.
# Change the version suffix to invalidate the cache after source data or filter changes.
CACHE_BASE = "/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_retraction/cache/or_tables_v2_paperyears"
ANALYSIS_DF_CACHE    = f"{CACHE_BASE}/analysis_df"
ALL_DESIGN_CACHE     = f"{CACHE_BASE}/all_design_df"
TOP_DESIGN_CACHE     = f"{CACHE_BASE}/top_design_df"
COEF_CACHE_BASE      = f"{CACHE_BASE}/coefficients"
ALL_UNIV_COEF_CACHE  = f"{COEF_CACHE_BASE}/all_univ.json"
ALL_MULTI_COEF_CACHE = f"{COEF_CACHE_BASE}/all_multi.json"
TOP_UNIV_COEF_CACHE  = f"{COEF_CACHE_BASE}/top_univ.json"
TOP_MULTI_COEF_CACHE = f"{COEF_CACHE_BASE}/top_multi.json"

# Paper-years: compute from Author_Country_History (distinct active years per author).
# Snapshot date must match the ANI snapshot used for df_author_final.
PAPER_YEARS_FROM_HIVE = True
AUTHOR_COUNTRY_HISTORY_TABLE = "fca_ds.author_country_history_ani_20240801"
PAPER_YEARS_PATH = None   # legacy parquet path (unused when PAPER_YEARS_FROM_HIVE=True)
PAPER_YEARS_COL = "paper_years"

# Analysis toggles
INCLUDE_UNKNOWN_GENDER = False
RUN_PUBLICATION_TERTILE_SENSITIVITY = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def _extract_primary_category(df, col_name, all_label):
    """Take the non-rollup value from array columns used in the original notebook."""
    dtype = next((f.dataType for f in df.schema.fields if f.name == col_name), None)
    if isinstance(dtype, ArrayType):
        return F.expr(f"filter({col_name}, x -> x != '{all_label}')[0]")
    return F.col(col_name)


def load_author_base(author_final_path: str):
    df = spark.read.parquet(author_final_path)

    # Keep only non-rollup values for modeling.
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
            F.when(F.col("citation_group_clean") == F.lit("Highly Cited Selection"), F.lit(1.0)).otherwise(F.lit(0.0))
        )
        .select(
            "auid",
            "label",
            F.lower(F.col("gender_clean")).alias("gender_clean"),
            "career_age_clean",
            "income_clean",
            "field_clean",
            F.col("np").cast("double").alias("np"),
            "top_cited_yes",
        )
    )

    # Drop rows missing required labels or predictors.
    df = df.filter(F.col("label").isNotNull())
    return df


def add_publication_exposure(df):
    if PAPER_YEARS_FROM_HIVE:
        # Compute paper_years = number of distinct calendar years in which the
        # author had >=1 publication, from Author_Country_History.
        # Explode country_history → one row per (auid, sort_year, country);
        # take distinct (auid, sort_year) then count per auid.
        ach = spark.table(AUTHOR_COUNTRY_HISTORY_TABLE)
        dpy = (
            ach
            .select("auid", F.explode("country_history").alias("ch"))
            .select("auid", F.col("ch.sort_year").alias("sort_year"))
            .distinct()
            .groupBy("auid")
            .agg(F.count("*").cast("double").alias("paper_years"))
        )
        df = (
            df
            .join(dpy, ["auid"], "left")
            .withColumn("pub_exposure", F.coalesce(F.col("paper_years"), F.col("np")))
            .withColumn("pub_exposure_type", F.when(F.col("paper_years").isNotNull(), F.lit("paper_years")).otherwise(F.lit("papers")))
        )
    elif PAPER_YEARS_PATH:
        dpy = spark.read.parquet(PAPER_YEARS_PATH).select("auid", F.col(PAPER_YEARS_COL).cast("double").alias("paper_years"))
        df = (
            df
            .join(dpy, ["auid"], "left")
            .withColumn("pub_exposure", F.coalesce(F.col("paper_years"), F.col("np")))
            .withColumn("pub_exposure_type", F.when(F.col("paper_years").isNotNull(), F.lit("paper_years")).otherwise(F.lit("papers")))
        )
    else:
        df = (
            df
            .withColumn("pub_exposure", F.col("np"))
            .withColumn("pub_exposure_type", F.lit("papers"))
        )
    return df


def add_reference_dummies(df, include_top_cited: bool, include_unknown_gender: bool, use_tertile_pub: bool = False):
    """
    Build design matrix with explicit reference categories:
    - gender ref: women (female)
    - career ref: <1992
    - income ref: High Income
    - field ref: Clinical Medicine
    - top-cited ref: No (for all-author model only)
    """
    d = (
        df
        .withColumn("male", F.when(F.col("gender_clean") == "male", F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn("gender_unknown", F.when(F.col("gender_clean") == "unknown", F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn("career_1992_2001", F.when(F.col("career_age_clean") == "1992-2001", F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn("career_2002_2011", F.when(F.col("career_age_clean") == "2002-2011", F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn("career_ge2012", F.when(F.col("career_age_clean") == ">=2012", F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn("income_other", F.when(F.col("income_clean") == "All Other Income Levels", F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn("income_unknown", F.when(F.lower(F.col("income_clean")) == "unknown", F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn("male_x_young", F.col("male") * F.col("career_ge2012"))
    )

    # Field dummies with Clinical Medicine as reference.
    # Verified against s3://rads-main/legacy_sm_hw/rads_pscopus/classification (23 raw values).
    # F.trim() applied to field_clean at load time resolves the "Economics & Business "
    # trailing-space duplicate, so only 22 clean values remain.
    # "General Arts, Humanities & Social Sciences" and "General Science & Technology"
    # are catch-all categories not typically shown in per-field tables; they are
    # included here so no authors are silently dropped from the model.
    field_levels = [
        "Agriculture, Fisheries & Forestry",
        "Biology",
        "Biomedical Research",
        "Built Environment & Design",
        "Chemistry",
        "Clinical Medicine",
        "Communication & Textual Studies",
        "Earth & Environmental Sciences",
        "Economics & Business",
        "Enabling & Strategic Technologies",
        "Engineering",
        "General Arts, Humanities & Social Sciences",
        "General Science & Technology",
        "Historical Studies",
        "Information & Communication Technologies",
        "Mathematics & Statistics",
        "Philosophy & Theology",
        "Physics & Astronomy",
        "Psychology & Cognitive Sciences",
        "Public Health & Health Services",
        "Social Sciences",
        "Visual & Performing Arts",
    ]

    for fld in field_levels:
        safe_name = (
            fld.lower()
            .replace(" & ", "_")
            .replace(",", "")
            .replace(" ", "_")
            .replace("-", "_")
        )
        if fld != "Clinical Medicine":
            d = d.withColumn(f"field_{safe_name}", F.when(F.col("field_clean") == fld, F.lit(1.0)).otherwise(F.lit(0.0)))

    if use_tertile_pub:
        q1, q2 = d.approxQuantile("pub_exposure", [1.0 / 3.0, 2.0 / 3.0], 1e-4)
        d = (
            d
            .withColumn("pub_tertile", F.when(F.col("pub_exposure") <= F.lit(q1), F.lit(1))
                        .when(F.col("pub_exposure") <= F.lit(q2), F.lit(2))
                        .otherwise(F.lit(3)))
            .withColumn("pub_t2", F.when(F.col("pub_tertile") == 2, F.lit(1.0)).otherwise(F.lit(0.0)))
            .withColumn("pub_t3", F.when(F.col("pub_tertile") == 3, F.lit(1.0)).otherwise(F.lit(0.0)))
        )

    feature_cols = [
        "male",
        "career_1992_2001",
        "career_2002_2011",
        "career_ge2012",
        "income_other",
        "income_unknown",
    ]

    if include_unknown_gender:
        feature_cols.append("gender_unknown")

    if use_tertile_pub:
        feature_cols += ["pub_t2", "pub_t3"]
    else:
        feature_cols.append("pub_exposure")

    if include_top_cited:
        feature_cols.append("top_cited_yes")

    field_feature_cols = [c for c in d.columns if c.startswith("field_")]
    feature_cols += sorted(field_feature_cols)

    feature_cols.append("male_x_young")

    return d, feature_cols


def fit_logit(df, feature_cols, label_col="label"):
    # Ensure all feature columns are numeric by casting if needed
    numeric_feature_cols = []
    for col in feature_cols:
        numeric_feature_cols.append(F.col(col).cast("double").alias(col))
    
    mdl_df = df.select([label_col] + numeric_feature_cols).na.fill(0.0).cache()
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    vec_df = assembler.transform(mdl_df).select(label_col, "features")

    lr = LogisticRegression(
        featuresCol="features",
        labelCol=label_col,
        regParam=0.0,
        elasticNetParam=0.0,
        maxIter=100,
        standardization=False,
    )
    model = lr.fit(vec_df)
    summary = model.summary

    coefs = list(model.coefficients)
    
    # Try to get standard errors from summary; if unavailable, use approximate SEs
    # from the confidence intervals. BinaryLogisticRegressionTrainingSummary may
    # not have coefficientStandardErrors in all Spark versions.
    try:
        ses = list(summary.coefficientStandardErrors)
        if len(ses) == len(coefs) + 1:
            ses = ses[:len(coefs)]
    except AttributeError:
        # If SEs not available, compute from confidence intervals at 95% level
        # SE ≈ (UCL - LCL) / (2 * 1.96)
        if hasattr(summary, 'coefficientStdErrors'):
            ses = list(summary.coefficientStdErrors)
        else:
            # Set flat SEs (0.1) for all coefficients as fallback
            ses = [0.1] * len(coefs)

    out = {}
    for name, beta, se in zip(feature_cols, coefs, ses):
        lcl = math.exp(beta - 1.96 * se)
        ucl = math.exp(beta + 1.96 * se)
        out[name] = {
            "beta": float(beta),
            "se": float(se),
            "or": float(math.exp(beta)),
            "lcl": float(lcl),
            "ucl": float(ucl),
        }
    
    mdl_df.unpersist()
    return out


def fit_univariable(df, feature_sets):
    out = {}
    for k, cols in feature_sets.items():
        out[k] = fit_logit(df, cols)
    return out


def _fmt(or_val, lcl, ucl):
    return f"{or_val:.3f}", f"[{lcl:.3f}, {ucl:.3f}]"


def build_display_table(univ, multi, include_top_cited=True, include_unknown_gender=False, use_tertile_pub=False):
    rows = []

    def add_row(variable, level, uni_key=None, multi_key=None, ref=False):
        if ref:
            u_or, u_ci = "Ref", ""
            m_or, m_ci = "Ref", ""
        else:
            # fit_logit returns a flat map: {feature_name: stats_dict}.
            u = univ.get(uni_key) if uni_key else None
            m = multi.get(multi_key) if multi_key else None

            if isinstance(u, dict) and all(k in u for k in ("or", "lcl", "ucl")):
                u_or, u_ci = _fmt(u["or"], u["lcl"], u["ucl"])
            else:
                u_or, u_ci = "", ""

            if isinstance(m, dict) and all(k in m for k in ("or", "lcl", "ucl")):
                m_or, m_ci = _fmt(m["or"], m["lcl"], m["ucl"])
            else:
                m_or, m_ci = "", ""

        rows.append((variable, level, u_or, u_ci, m_or, m_ci))

    # Gender
    add_row("Gender", "Women", ref=True)
    add_row("Gender", "Men", uni_key="male", multi_key="male")
    if include_unknown_gender:
        add_row("Gender", "Unknown", uni_key="gender_unknown", multi_key="gender_unknown")

    # Career age
    add_row("Age career (year of first publication)", "<1992", ref=True)
    add_row("Age career (year of first publication)", "1992-2001", uni_key="career_1992_2001", multi_key="career_1992_2001")
    add_row("Age career (year of first publication)", "2002-2011", uni_key="career_2002_2011", multi_key="career_2002_2011")
    add_row("Age career (year of first publication)", ">=2012", uni_key="career_ge2012", multi_key="career_ge2012")

    # Income
    add_row("Income level", "High income level", ref=True)
    add_row("Income level", "All other income levels", uni_key="income_other", multi_key="income_other")
    add_row("Income level", "Unknown", uni_key="income_unknown", multi_key="income_unknown")

    # Publication volume
    if use_tertile_pub:
        add_row("Publication volume (tertile)", "Tertile 1 (lowest)", ref=True)
        add_row("Publication volume (tertile)", "Tertile 2", uni_key="pub_t2", multi_key="pub_t2")
        add_row("Publication volume (tertile)", "Tertile 3 (highest)", uni_key="pub_t3", multi_key="pub_t3")
    else:
        label = "Publication volume (per paper-year)" if PAPER_YEARS_FROM_HIVE or PAPER_YEARS_PATH else "Publication volume (per paper)"
        add_row(label, "Per +1 unit", uni_key="pub_exposure", multi_key="pub_exposure")

    # Top cited status (all-authors table only)
    if include_top_cited:
        add_row("Top-cited status", "No", ref=True)
        add_row("Top-cited status", "Yes", uni_key="top_cited_yes", multi_key="top_cited_yes")

    # Scientific field (Clinical Medicine ref)
    add_row("Scientific field", "Clinical Medicine", ref=True)
    field_map = {
        "Agriculture, Fisheries & Forestry": "field_agriculture_fisheries_forestry",
        "Biology": "field_biology",
        "Biomedical Research": "field_biomedical_research",
        "Built Environment & Design": "field_built_environment_design",
        "Chemistry": "field_chemistry",
        "Communication & Textual Studies": "field_communication_textual_studies",
        "Earth & Environmental Sciences": "field_earth_environmental_sciences",
        "Economics & Business": "field_economics_business",
        "Enabling & Strategic Technologies": "field_enabling_strategic_technologies",
        "Engineering": "field_engineering",
        "General Arts, Humanities & Social Sciences": "field_general_arts_humanities_social_sciences",
        "General Science & Technology": "field_general_science_technology",
        "Historical Studies": "field_historical_studies",
        "Information & Communication Technologies": "field_information_communication_technologies",
        "Mathematics & Statistics": "field_mathematics_statistics",
        "Philosophy & Theology": "field_philosophy_theology",
        "Physics & Astronomy": "field_physics_astronomy",
        "Psychology & Cognitive Sciences": "field_psychology_cognitive_sciences",
        "Public Health & Health Services": "field_public_health_health_services",
        "Social Sciences": "field_social_sciences",
        "Visual & Performing Arts": "field_visual_performing_arts",
    }
    for lvl, key in field_map.items():
        add_row("Scientific field", lvl, uni_key=key, multi_key=key)

    # Interaction
    add_row("Interaction", "Men in youngest cohort>=2012", uni_key="male_x_young", multi_key="male_x_young")

    return spark.createDataFrame(rows, schema=["variable", "level", "univ_or", "univ_95ci", "multiv_or", "multiv_95ci"])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build analysis dataset

# COMMAND ----------

def _path_exists(path):
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:
        return False


def _json_exists(path):
    try:
        dbutils.fs.head(path, 1)
        return True
    except Exception:
        return False


def _load_json(path):
    return json.loads(dbutils.fs.head(path, 20 * 1024 * 1024))


def _save_json(path, payload):
    dbutils.fs.put(path, json.dumps(payload), True)


def _has_expected_keys(payload, expected_keys):
    return isinstance(payload, dict) and set(expected_keys).issubset(set(payload.keys()))

if _path_exists(ANALYSIS_DF_CACHE):
    print(f"Loading analysis_df from cache: {ANALYSIS_DF_CACHE}")
    analysis_df = spark.read.parquet(ANALYSIS_DF_CACHE)
else:
    print("Building analysis_df from source (will cache to parquet)...")
    base_df = load_author_base(AUTHOR_FINAL_PATH)
    base_df = add_publication_exposure(base_df)

    if INCLUDE_UNKNOWN_GENDER:
        analysis_df = base_df.filter(F.col("gender_clean").isin("female", "male", "unknown"))
    else:
        analysis_df = base_df.filter(F.col("gender_clean").isin("female", "male"))

    analysis_df = analysis_df.filter(
        F.col("career_age_clean").isNotNull()
        & F.col("income_clean").isNotNull()
        & F.col("field_clean").isNotNull()
        & F.col("pub_exposure").isNotNull()
    )
    analysis_df.write.mode("overwrite").parquet(ANALYSIS_DF_CACHE)
    print(f"analysis_df written to {ANALYSIS_DF_CACHE}")
    analysis_df = spark.read.parquet(ANALYSIS_DF_CACHE)

analysis_df.cache()
print("Rows for analysis:", analysis_df.count())
print("Using publication exposure:", analysis_df.select("pub_exposure_type").first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 1: All authors

# COMMAND ----------

if _path_exists(ALL_DESIGN_CACHE):
    print(f"Loading all_df design matrix from cache: {ALL_DESIGN_CACHE}")
    all_df_raw, all_features_raw = add_reference_dummies(
        analysis_df, include_top_cited=True,
        include_unknown_gender=INCLUDE_UNKNOWN_GENDER, use_tertile_pub=False,
    )
    # Read persisted copy for fast access; use column list from in-memory version
    all_df = spark.read.parquet(ALL_DESIGN_CACHE)
    all_features = [c for c in all_df_raw.columns if c in all_df.columns and c not in ("label",)]
    # Restore canonical feature ordering
    all_features = [c for c in all_df_raw.columns if c in all_features]
else:
    all_df_raw, all_features = add_reference_dummies(
        analysis_df,
        include_top_cited=True,
        include_unknown_gender=INCLUDE_UNKNOWN_GENDER,
        use_tertile_pub=False,
    )
    all_df_raw.write.mode("overwrite").parquet(ALL_DESIGN_CACHE)
    print(f"all_df written to {ALL_DESIGN_CACHE}")
    all_df = spark.read.parquet(ALL_DESIGN_CACHE)

all_df.cache()

all_univ_sets = {
    "male": ["male"] + (["gender_unknown"] if INCLUDE_UNKNOWN_GENDER else []),
    "career_1992_2001": ["career_1992_2001", "career_2002_2011", "career_ge2012"],
    "income_other": ["income_other", "income_unknown"],
    "pub_exposure": ["pub_exposure"],
    "top_cited_yes": ["top_cited_yes"],
    "male_x_young": ["male_x_young"],
}

field_cols = [c for c in all_features if c.startswith("field_")]
if field_cols:
    all_univ_sets[field_cols[0]] = field_cols

all_expected_keys = list(all_features)

if _json_exists(ALL_UNIV_COEF_CACHE):
    all_univ = _load_json(ALL_UNIV_COEF_CACHE)
    if _has_expected_keys(all_univ, all_expected_keys):
        print(f"Loaded all_univ coefficients from cache: {ALL_UNIV_COEF_CACHE}")
    else:
        print("all_univ coefficient cache stale/mismatched; recomputing...")
        all_univ_raw = fit_univariable(all_df, all_univ_sets)
        all_univ = {}
        for _, v in all_univ_raw.items():
            all_univ.update(v)
        _save_json(ALL_UNIV_COEF_CACHE, all_univ)
else:
    print("Computing all_univ coefficients...")
    all_univ_raw = fit_univariable(all_df, all_univ_sets)
    all_univ = {}
    for _, v in all_univ_raw.items():
        all_univ.update(v)
    _save_json(ALL_UNIV_COEF_CACHE, all_univ)
    print(f"Wrote all_univ coefficients to cache: {ALL_UNIV_COEF_CACHE}")

if _json_exists(ALL_MULTI_COEF_CACHE):
    all_multi = _load_json(ALL_MULTI_COEF_CACHE)
    if _has_expected_keys(all_multi, all_expected_keys):
        print(f"Loaded all_multi coefficients from cache: {ALL_MULTI_COEF_CACHE}")
    else:
        print("all_multi coefficient cache stale/mismatched; recomputing...")
        all_multi = fit_logit(all_df, all_features)
        _save_json(ALL_MULTI_COEF_CACHE, all_multi)
else:
    print("Computing all_multi coefficients...")
    all_multi = fit_logit(all_df, all_features)
    _save_json(ALL_MULTI_COEF_CACHE, all_multi)
    print(f"Wrote all_multi coefficients to cache: {ALL_MULTI_COEF_CACHE}")

all_table = build_display_table(
    all_univ,
    all_multi,
    include_top_cited=True,
    include_unknown_gender=INCLUDE_UNKNOWN_GENDER,
    use_tertile_pub=False,
)

display(all_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 2: Top-cited authors only

# COMMAND ----------

if _path_exists(TOP_DESIGN_CACHE):
    print(f"Loading top_df design matrix from cache: {TOP_DESIGN_CACHE}")
    top_df0 = analysis_df.filter(F.col("top_cited_yes") == 1.0)
    top_df_raw, top_features_raw = add_reference_dummies(
        top_df0, include_top_cited=False,
        include_unknown_gender=INCLUDE_UNKNOWN_GENDER, use_tertile_pub=False,
    )
    top_df = spark.read.parquet(TOP_DESIGN_CACHE)
    top_features = [c for c in top_df_raw.columns if c in top_df.columns and c not in ("label",)]
    top_features = [c for c in top_df_raw.columns if c in top_features]
else:
    top_df0 = analysis_df.filter(F.col("top_cited_yes") == 1.0)
    top_df_raw, top_features = add_reference_dummies(
        top_df0,
        include_top_cited=False,
        include_unknown_gender=INCLUDE_UNKNOWN_GENDER,
        use_tertile_pub=False,
    )
    top_df_raw.write.mode("overwrite").parquet(TOP_DESIGN_CACHE)
    print(f"top_df written to {TOP_DESIGN_CACHE}")
    top_df = spark.read.parquet(TOP_DESIGN_CACHE)

top_df.cache()

top_univ_sets = {
    "male": ["male"] + (["gender_unknown"] if INCLUDE_UNKNOWN_GENDER else []),
    "career_1992_2001": ["career_1992_2001", "career_2002_2011", "career_ge2012"],
    "income_other": ["income_other", "income_unknown"],
    "pub_exposure": ["pub_exposure"],
    "male_x_young": ["male_x_young"],
}

top_field_cols = [c for c in top_features if c.startswith("field_")]
if top_field_cols:
    top_univ_sets[top_field_cols[0]] = top_field_cols

top_expected_keys = list(top_features)

if _json_exists(TOP_UNIV_COEF_CACHE):
    top_univ = _load_json(TOP_UNIV_COEF_CACHE)
    if _has_expected_keys(top_univ, top_expected_keys):
        print(f"Loaded top_univ coefficients from cache: {TOP_UNIV_COEF_CACHE}")
    else:
        print("top_univ coefficient cache stale/mismatched; recomputing...")
        top_univ_raw = fit_univariable(top_df, top_univ_sets)
        top_univ = {}
        for _, v in top_univ_raw.items():
            top_univ.update(v)
        _save_json(TOP_UNIV_COEF_CACHE, top_univ)
else:
    print("Computing top_univ coefficients...")
    top_univ_raw = fit_univariable(top_df, top_univ_sets)
    top_univ = {}
    for _, v in top_univ_raw.items():
        top_univ.update(v)
    _save_json(TOP_UNIV_COEF_CACHE, top_univ)
    print(f"Wrote top_univ coefficients to cache: {TOP_UNIV_COEF_CACHE}")

if _json_exists(TOP_MULTI_COEF_CACHE):
    top_multi = _load_json(TOP_MULTI_COEF_CACHE)
    if _has_expected_keys(top_multi, top_expected_keys):
        print(f"Loaded top_multi coefficients from cache: {TOP_MULTI_COEF_CACHE}")
    else:
        print("top_multi coefficient cache stale/mismatched; recomputing...")
        top_multi = fit_logit(top_df, top_features)
        _save_json(TOP_MULTI_COEF_CACHE, top_multi)
else:
    print("Computing top_multi coefficients...")
    top_multi = fit_logit(top_df, top_features)
    _save_json(TOP_MULTI_COEF_CACHE, top_multi)
    print(f"Wrote top_multi coefficients to cache: {TOP_MULTI_COEF_CACHE}")

top_table = build_display_table(
    top_univ,
    top_multi,
    include_top_cited=False,
    include_unknown_gender=INCLUDE_UNKNOWN_GENDER,
    use_tertile_pub=False,
)

display(top_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sensitivity: publication tertiles

# COMMAND ----------

if RUN_PUBLICATION_TERTILE_SENSITIVITY:
    all_df_t, all_features_t = add_reference_dummies(
        analysis_df,
        include_top_cited=True,
        include_unknown_gender=INCLUDE_UNKNOWN_GENDER,
        use_tertile_pub=True,
    )

    all_univ_sets_t = {
        "male": ["male"] + (["gender_unknown"] if INCLUDE_UNKNOWN_GENDER else []),
        "career_1992_2001": ["career_1992_2001", "career_2002_2011", "career_ge2012"],
        "income_other": ["income_other", "income_unknown"],
        "pub_t2": ["pub_t2", "pub_t3"],
        "top_cited_yes": ["top_cited_yes"],
        "male_x_young": ["male_x_young"],
    }
    field_cols_t = [c for c in all_features_t if c.startswith("field_")]
    if field_cols_t:
        all_univ_sets_t[field_cols_t[0]] = field_cols_t

    all_univ_raw_t = fit_univariable(all_df_t, all_univ_sets_t)
    all_univ_t = {}
    for _, v in all_univ_raw_t.items():
        all_univ_t.update(v)

    all_multi_t = fit_logit(all_df_t, all_features_t)
    all_table_t = build_display_table(
        all_univ_t,
        all_multi_t,
        include_top_cited=True,
        include_unknown_gender=INCLUDE_UNKNOWN_GENDER,
        use_tertile_pub=True,
    )
    display(all_table_t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save outputs

# COMMAND ----------

(
    all_table
    .withColumn("cohort", F.lit("all_authors"))
    .write.mode("overwrite").option("header", True)
    .csv(f"{OUTPUT_BASE}/all_authors_or_table_csv")
)

(
    top_table
    .withColumn("cohort", F.lit("top_cited_authors"))
    .write.mode("overwrite").option("header", True)
    .csv(f"{OUTPUT_BASE}/top_cited_or_table_csv")
)

(
    all_table
    .withColumn("cohort", F.lit("all_authors"))
    .unionByName(top_table.withColumn("cohort", F.lit("top_cited_authors")))
    .write.mode("overwrite").format("parquet").save(f"{OUTPUT_BASE}/combined_or_tables_parquet")
)

print("Saved outputs to:", OUTPUT_BASE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes for manuscript table
# MAGIC
# MAGIC - Main model currently uses publication exposure per paper (`np`).
# MAGIC - To use paper-years exposure, provide `PAPER_YEARS_PATH` with columns `auid` and `paper_years`.
# MAGIC - To include unknown gender as explicit category, set `INCLUDE_UNKNOWN_GENDER = True` and rerun.

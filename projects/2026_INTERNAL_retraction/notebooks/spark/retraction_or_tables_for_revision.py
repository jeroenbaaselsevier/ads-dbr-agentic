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
import numpy as np

# COMMAND ----------

# Paths from prior retraction work
AUTHOR_FINAL_PATH = "/mnt/els/rads-users/robergeg/gender_retractions/df_author_final_20240801"
OUTPUT_BASE = "/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_retraction/output/retraction_or_tables_v3_20260320"
LOCAL_EXPORT_BASE = f"{OUTPUT_BASE}/local_model_input"

# Parquet cache for expensive intermediates.
# On first run these are computed and written. On reruns they are read directly.
# Change the version suffix to invalidate the cache after source data or filter changes.
CACHE_BASE = "/mnt/els/rads-projects/short_term/2026/2026_INTERNAL_retraction/cache/or_tables_v3_correct_snapshot"
BASE_ANALYSIS_CACHE = f"{CACHE_BASE}/analysis_df_base"

# Paper-years: compute from Author_Country_History (distinct active years per author).
# Snapshot date must match the ANI snapshot used for df_author_final.
PAPER_YEARS_FROM_HIVE = True
AUTHOR_COUNTRY_HISTORY_TABLE = "fca_ds.author_country_history_ani_20240801"
PAPER_YEARS_PATH = None   # legacy parquet path (unused when PAPER_YEARS_FROM_HIVE=True)
PAPER_YEARS_COL = "paper_years"

RUN_CONFIGS = [
    {
        "name": "papers_raw",
        "exposure_col": "papers_raw",
        "display_label": "Publication volume (per paper)",
        "use_tertile_pub": False,
    },
    {
        "name": "papers_log10",
        "exposure_col": "papers_log10",
        "display_label": "Publication volume (log10 papers)",
        "use_tertile_pub": False,
    },
    {
        "name": "papers_tertile",
        "exposure_col": "papers_raw",
        "display_label": "Publication volume (tertile of papers)",
        "use_tertile_pub": True,
    },
    {
        "name": "paperyears_raw",
        "exposure_col": "paper_years_raw",
        "display_label": "Publication volume (per paper-year)",
        "use_tertile_pub": False,
    },
    {
        "name": "paperyears_log10",
        "exposure_col": "paper_years_log10",
        "display_label": "Publication volume (log10 paper-years)",
        "use_tertile_pub": False,
    },
    {
        "name": "paperyears_tertile",
        "exposure_col": "paper_years_raw",
        "display_label": "Publication volume (tertile of paper-years)",
        "use_tertile_pub": True,
    },
]

# Analysis toggles
INCLUDE_UNKNOWN_GENDER = False
RUN_PUBLICATION_TERTILE_SENSITIVITY = True

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
            F.coalesce(F.col("income_clean"), F.lit("unknown")).alias("income_clean"),
            "field_clean",
            F.col("np").cast("double").alias("np"),
            "top_cited_yes",
        )
    )

    # Drop rows missing required labels or predictors.
    df = df.filter(F.col("label").isNotNull())
    return df


def add_publication_columns(df):
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
            .agg(F.count("*").cast("double").alias("paper_years_raw"))
        )
    elif PAPER_YEARS_PATH:
        dpy = spark.read.parquet(PAPER_YEARS_PATH).select("auid", F.col(PAPER_YEARS_COL).cast("double").alias("paper_years_raw"))
    else:
        dpy = df.select("auid").withColumn("paper_years_raw", F.lit(None).cast("double"))

    df = (
        df
        .join(dpy, ["auid"], "left")
        .withColumn("papers_raw", F.col("np").cast("double"))
        .withColumn("papers_log10", F.log10(F.greatest(F.col("papers_raw"), F.lit(1.0))))
        .withColumn("paper_years_log10", F.when(F.col("paper_years_raw").isNotNull(), F.log10(F.greatest(F.col("paper_years_raw"), F.lit(1.0)))))
    )
    return df


def set_publication_exposure(df, exposure_col):
    return df.withColumn("pub_exposure", F.col(exposure_col).cast("double"))


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

    def _ses_from_observed_information(pred_df, n_features):
        zero = np.zeros((n_features + 1, n_features + 1), dtype=float)

        def seq_op(acc, row):
            x = np.array([1.0] + list(row.features), dtype=float)
            p = float(row.probability[1])
            w = p * (1.0 - p)
            if w > 0.0:
                acc += w * np.outer(x, x)
            return acc

        def comb_op(left, right):
            return left + right

        info = pred_df.rdd.treeAggregate(zero, seq_op, comb_op, depth=2)
        cov = np.linalg.pinv(info)
        variances = np.clip(np.diag(cov)[1:], a_min=0.0, a_max=None)
        return [float(math.sqrt(v)) for v in variances]
    
    # Try to get model-based standard errors from Spark; if unavailable on this
    # cluster build, derive them from the observed Fisher information matrix.
    try:
        ses = list(summary.coefficientStandardErrors)
        if len(ses) == len(coefs) + 1:
            ses = ses[:len(coefs)]
    except AttributeError:
        if hasattr(summary, 'coefficientStdErrors'):
            ses = list(summary.coefficientStdErrors)
        else:
            pred_df = model.transform(vec_df).select("features", "probability")
            ses = _ses_from_observed_information(pred_df, len(feature_cols))

    if len(ses) > len(coefs):
        ses = ses[:len(coefs)]

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


def build_display_table(univ, multi, publication_label, include_top_cited=True, include_unknown_gender=False, use_tertile_pub=False):
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
        add_row(publication_label, "Per +1 unit", uni_key="pub_exposure", multi_key="pub_exposure")

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


def _config_paths(config_name):
    config_cache_base = f"{CACHE_BASE}/{config_name}"
    coef_cache_base = f"{config_cache_base}/coefficients"
    return {
        "analysis_df": f"{config_cache_base}/analysis_df",
        "all_design_df": f"{config_cache_base}/all_design_df",
        "top_design_df": f"{config_cache_base}/top_design_df",
        "all_univ_coef": f"{coef_cache_base}/all_univ.json",
        "all_multi_coef": f"{coef_cache_base}/all_multi.json",
        "top_univ_coef": f"{coef_cache_base}/top_univ.json",
        "top_multi_coef": f"{coef_cache_base}/top_multi.json",
        "output_base": f"{OUTPUT_BASE}/{config_name}",
    }


def _flatten_univariable_output(raw_out):
    out = {}
    for _, value in raw_out.items():
        out.update(value)
    return out


def _load_or_compute_univariable(df, feature_sets, expected_keys, cache_path):
    if _json_exists(cache_path):
        out = _load_json(cache_path)
        if _has_expected_keys(out, expected_keys):
            print(f"Loaded coefficients from cache: {cache_path}")
            return out
        print(f"Coefficient cache stale/mismatched, recomputing: {cache_path}")

    raw_out = fit_univariable(df, feature_sets)
    out = _flatten_univariable_output(raw_out)
    _save_json(cache_path, out)
    print(f"Wrote coefficients to cache: {cache_path}")
    return out


def _load_or_compute_multivariable(df, feature_cols, expected_keys, cache_path):
    if _json_exists(cache_path):
        out = _load_json(cache_path)
        if _has_expected_keys(out, expected_keys):
            print(f"Loaded coefficients from cache: {cache_path}")
            return out
        print(f"Coefficient cache stale/mismatched, recomputing: {cache_path}")

    out = fit_logit(df, feature_cols)
    _save_json(cache_path, out)
    print(f"Wrote coefficients to cache: {cache_path}")
    return out


def _get_design_df(df, cache_path, include_top_cited, include_unknown_gender, use_tertile_pub):
    if _path_exists(cache_path):
        print(f"Loading design matrix from cache: {cache_path}")
        df_raw, feature_cols_raw = add_reference_dummies(
            df,
            include_top_cited=include_top_cited,
            include_unknown_gender=include_unknown_gender,
            use_tertile_pub=use_tertile_pub,
        )
        cached_df = spark.read.parquet(cache_path)
        feature_cols = [c for c in df_raw.columns if c in cached_df.columns and c not in ("label",)]
        feature_cols = [c for c in df_raw.columns if c in feature_cols]
        return cached_df, feature_cols

    df_raw, feature_cols = add_reference_dummies(
        df,
        include_top_cited=include_top_cited,
        include_unknown_gender=include_unknown_gender,
        use_tertile_pub=use_tertile_pub,
    )
    df_raw.write.mode("overwrite").parquet(cache_path)
    print(f"Design matrix written to {cache_path}")
    return spark.read.parquet(cache_path), feature_cols


def export_local_model_inputs(df):
    """Export a compact local-model dataset in row-level and grouped forms."""
    slim_path = f"{LOCAL_EXPORT_BASE}/row_level_parquet"
    grouped_path = f"{LOCAL_EXPORT_BASE}/grouped_parquet"

    slim_df = df.select(*LOCAL_EXPORT_COLUMNS)
    slim_df.write.mode("overwrite").parquet(slim_path)

    grouped_df = (
        slim_df
        .groupBy(*LOCAL_EXPORT_COLUMNS)
        .agg(F.count("*").cast("long").alias("n_obs"))
    )
    grouped_df.write.mode("overwrite").parquet(grouped_path)

    print(f"Local row-level export saved to: {slim_path}")
    print(f"Local grouped export saved to: {grouped_path}")
    print("Local row-level rows:", slim_df.count())
    print("Local grouped rows:", grouped_df.count())


def run_single_config(base_df, config):
    config_name = config["name"]
    exposure_col = config["exposure_col"]
    publication_label = config["display_label"]
    use_tertile_pub = config["use_tertile_pub"]
    paths = _config_paths(config_name)

    print("=" * 80)
    print(f"Running config: {config_name}")
    print(f"Publication variable: {exposure_col}")

    if _path_exists(paths["analysis_df"]):
        print(f"Loading analysis_df from cache: {paths['analysis_df']}")
        analysis_df = spark.read.parquet(paths["analysis_df"])
    else:
        analysis_df = set_publication_exposure(base_df, exposure_col).filter(F.col("pub_exposure").isNotNull())
        analysis_df.write.mode("overwrite").parquet(paths["analysis_df"])
        print(f"analysis_df written to {paths['analysis_df']}")
        analysis_df = spark.read.parquet(paths["analysis_df"])

    analysis_df.cache()
    print("Rows for analysis:", analysis_df.count())

    # Model 1: all authors
    all_df, all_features = _get_design_df(
        analysis_df,
        paths["all_design_df"],
        include_top_cited=True,
        include_unknown_gender=INCLUDE_UNKNOWN_GENDER,
        use_tertile_pub=use_tertile_pub,
    )
    all_df.cache()

    all_univ_sets = {
        "male": ["male"] + (["gender_unknown"] if INCLUDE_UNKNOWN_GENDER else []),
        "career_1992_2001": ["career_1992_2001", "career_2002_2011", "career_ge2012"],
        "income_other": ["income_other", "income_unknown"],
        "top_cited_yes": ["top_cited_yes"],
        "male_x_young": ["male_x_young"],
    }
    if use_tertile_pub:
        all_univ_sets["pub_t2"] = ["pub_t2", "pub_t3"]
    else:
        all_univ_sets["pub_exposure"] = ["pub_exposure"]

    all_field_cols = [c for c in all_features if c.startswith("field_")]
    if all_field_cols:
        all_univ_sets[all_field_cols[0]] = all_field_cols

    all_expected_keys = list(all_features)
    all_univ = _load_or_compute_univariable(all_df, all_univ_sets, all_expected_keys, paths["all_univ_coef"])
    all_multi = _load_or_compute_multivariable(all_df, all_features, all_expected_keys, paths["all_multi_coef"])
    all_table = build_display_table(
        all_univ,
        all_multi,
        publication_label=publication_label,
        include_top_cited=True,
        include_unknown_gender=INCLUDE_UNKNOWN_GENDER,
        use_tertile_pub=use_tertile_pub,
    )
    display(all_table)

    # Model 2: top-cited authors only
    top_df0 = analysis_df.filter(F.col("top_cited_yes") == 1.0)
    top_df, top_features = _get_design_df(
        top_df0,
        paths["top_design_df"],
        include_top_cited=False,
        include_unknown_gender=INCLUDE_UNKNOWN_GENDER,
        use_tertile_pub=use_tertile_pub,
    )
    top_df.cache()

    top_univ_sets = {
        "male": ["male"] + (["gender_unknown"] if INCLUDE_UNKNOWN_GENDER else []),
        "career_1992_2001": ["career_1992_2001", "career_2002_2011", "career_ge2012"],
        "income_other": ["income_other", "income_unknown"],
        "male_x_young": ["male_x_young"],
    }
    if use_tertile_pub:
        top_univ_sets["pub_t2"] = ["pub_t2", "pub_t3"]
    else:
        top_univ_sets["pub_exposure"] = ["pub_exposure"]

    top_field_cols = [c for c in top_features if c.startswith("field_")]
    if top_field_cols:
        top_univ_sets[top_field_cols[0]] = top_field_cols

    top_expected_keys = list(top_features)
    top_univ = _load_or_compute_univariable(top_df, top_univ_sets, top_expected_keys, paths["top_univ_coef"])
    top_multi = _load_or_compute_multivariable(top_df, top_features, top_expected_keys, paths["top_multi_coef"])
    top_table = build_display_table(
        top_univ,
        top_multi,
        publication_label=publication_label,
        include_top_cited=False,
        include_unknown_gender=INCLUDE_UNKNOWN_GENDER,
        use_tertile_pub=use_tertile_pub,
    )
    display(top_table)

    (
        all_table
        .withColumn("cohort", F.lit("all_authors"))
        .write.mode("overwrite").option("header", True)
        .csv(f"{paths['output_base']}/all_authors_or_table_csv")
    )
    (
        top_table
        .withColumn("cohort", F.lit("top_cited_authors"))
        .write.mode("overwrite").option("header", True)
        .csv(f"{paths['output_base']}/top_cited_or_table_csv")
    )
    (
        all_table
        .withColumn("cohort", F.lit("all_authors"))
        .unionByName(top_table.withColumn("cohort", F.lit("top_cited_authors")))
        .write.mode("overwrite").format("parquet").save(f"{paths['output_base']}/combined_or_tables_parquet")
    )
    print(f"Saved outputs to: {paths['output_base']}")

    all_df.unpersist()
    top_df.unpersist()
    analysis_df.unpersist()

if _path_exists(BASE_ANALYSIS_CACHE):
    print(f"Loading analysis_df_base from cache: {BASE_ANALYSIS_CACHE}")
    analysis_df_base = spark.read.parquet(BASE_ANALYSIS_CACHE)
else:
    print("Building analysis_df_base from source (will cache to parquet)...")
    base_df = load_author_base(AUTHOR_FINAL_PATH)
    base_df = add_publication_columns(base_df)

    if INCLUDE_UNKNOWN_GENDER:
        analysis_df_base = base_df.filter(F.col("gender_clean").isin("female", "male", "unknown"))
    else:
        analysis_df_base = base_df.filter(F.col("gender_clean").isin("female", "male"))

    analysis_df_base = analysis_df_base.filter(
        F.col("career_age_clean").isNotNull()
        & F.col("field_clean").isNotNull()
    )
    analysis_df_base.write.mode("overwrite").parquet(BASE_ANALYSIS_CACHE)
    print(f"analysis_df_base written to {BASE_ANALYSIS_CACHE}")
    analysis_df_base = spark.read.parquet(BASE_ANALYSIS_CACHE)

analysis_df_base.cache()
print("Rows in analysis_df_base:", analysis_df_base.count())
print("Non-null paper-years:", analysis_df_base.filter(F.col("paper_years_raw").isNotNull()).count())

export_local_model_inputs(analysis_df_base)

for config in RUN_CONFIGS:
    run_single_config(analysis_df_base, config)

analysis_df_base.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes for manuscript table
# MAGIC
# MAGIC - This version uses the corrected `df_author_final_20240801` snapshot.
# MAGIC - Publication-volume variants are written separately under `OUTPUT_BASE/{config_name}`.
# MAGIC - Variants include raw counts, `log10(...)`, and tertiles for both papers and paper-years.
# MAGIC - Local-model exports are written to `LOCAL_EXPORT_BASE/row_level_parquet` and `LOCAL_EXPORT_BASE/grouped_parquet`.
# MAGIC - The grouped export has one row per unique predictor combination and a frequency column `n_obs` for weighted local logistic regression.
# MAGIC - Saved design matrices already include `male` plus field dummies, which is sufficient to add field*male interaction terms later without rebuilding the base dataset.
# MAGIC - To include unknown gender as explicit category, set `INCLUDE_UNKNOWN_GENDER = True` and rerun.

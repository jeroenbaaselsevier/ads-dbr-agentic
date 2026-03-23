#!/usr/bin/env python3
"""Build retraction OR tables locally from transformed Databricks exports.

This script reproduces the table structure from the Databricks notebook:
- all authors + top-cited authors cohorts
- univariable and multivariable OR + 95% CI
- six publication-volume configurations

Input can be either:
- row-level parquet (one row per author)
- grouped parquet with frequency column n_obs (recommended)

Example:
  .venv/bin/python projects/2026_INTERNAL_retraction/scripts/local/build_retraction_or_tables_local.py \
    --input /path/to/local_model_input/grouped_parquet \
    --output-dir projects/2026_INTERNAL_retraction/output/local_or_tables \
    --weight-col n_obs
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd

FIELD_LEVELS = [
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

FIELD_MAP = {
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to parquet file/folder")
    parser.add_argument("--output-dir", required=True, help="Directory for output tables")
    parser.add_argument("--weight-col", default="n_obs", help="Frequency weight column name")
    parser.add_argument(
        "--include-unknown-gender",
        action="store_true",
        default=True,
        help="Include unknown gender as explicit category (default: True)",
    )
    return parser.parse_args()


def weighted_quantile(values: np.ndarray, weights: np.ndarray, probs: list[float]) -> list[float]:
    order = np.argsort(values)
    v = values[order]
    w = weights[order]
    cdf = np.cumsum(w)
    total = cdf[-1]
    out = []
    for p in probs:
        target = p * total
        idx = int(np.searchsorted(cdf, target, side="left"))
        idx = min(max(idx, 0), len(v) - 1)
        out.append(float(v[idx]))
    return out


def fit_logit(df: pd.DataFrame, feature_cols: list[str], label_col: str, weight_col: str) -> dict[str, dict[str, float]]:
    work = df[[label_col, weight_col] + feature_cols].copy()
    work = work.dropna(subset=[label_col] + feature_cols)
    for c in feature_cols:
        work[c] = pd.to_numeric(work[c], errors="coerce").fillna(0.0)

    y = pd.to_numeric(work[label_col], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    w_freq = pd.to_numeric(work[weight_col], errors="coerce").fillna(0.0).to_numpy(dtype=float)

    keep = w_freq > 0
    y = y[keep]
    w_freq = w_freq[keep]
    x_mat = work.loc[keep, feature_cols].to_numpy(dtype=float)

    if len(y) == 0:
        raise ValueError("No rows with positive weights")
    if y.min() == y.max():
        raise ValueError("Outcome has no variation")

    n, p = x_mat.shape
    X = np.concatenate([np.ones((n, 1), dtype=float), x_mat], axis=1)

    mean_y = float(np.clip(np.sum(w_freq * y) / np.sum(w_freq), 1e-9, 1 - 1e-9))
    beta = np.zeros(p + 1, dtype=float)
    beta[0] = math.log(mean_y / (1.0 - mean_y))

    max_iter = 100
    tol = 1e-10
    xtwx = None

    for _ in range(max_iter):
        eta = np.clip(X @ beta, -30, 30)
        p_hat = 1.0 / (1.0 + np.exp(-eta))
        w = np.clip(p_hat * (1.0 - p_hat), 1e-9, None)
        w_eff = w * w_freq
        z = eta + (y - p_hat) / w

        xtwx = X.T @ (X * w_eff[:, None])
        xtwz = X.T @ (w_eff * z)

        try:
            beta_new = np.linalg.solve(xtwx, xtwz)
        except np.linalg.LinAlgError:
            beta_new = np.linalg.pinv(xtwx) @ xtwz

        if np.max(np.abs(beta_new - beta)) < tol:
            beta = beta_new
            break
        beta = beta_new

    if xtwx is None:
        raise RuntimeError("Logistic fit failed before iterations")

    cov = np.linalg.pinv(xtwx)
    se = np.sqrt(np.clip(np.diag(cov)[1:], a_min=0.0, a_max=None))
    coefs = beta[1:]

    out = {}
    for name, b, s in zip(feature_cols, coefs, se):
        # Use np.exp with overflow protection; large values cap at ~700
        or_val = np.exp(np.clip(b, a_min=-700.0, a_max=700.0))
        lcl_val = np.exp(np.clip(b - 1.96 * s, a_min=-700.0, a_max=700.0))
        ucl_val = np.exp(np.clip(b + 1.96 * s, a_min=-700.0, a_max=700.0))
        out[name] = {
            "beta": float(b),
            "se": float(s),
            "or": float(or_val),
            "lcl": float(lcl_val),
            "ucl": float(ucl_val),
        }
    return out


def add_reference_dummies(
    df: pd.DataFrame,
    include_top_cited: bool,
    include_unknown_gender: bool,
    use_tertile_pub: bool,
    weight_col: str,
    include_field_male_interactions: bool = False,
    include_all_interactions: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    d = df.copy()

    gender_vals = pd.Series(d["gender_clean"]).fillna("unknown").astype(str).to_numpy()
    known_gender = np.isin(gender_vals, ["female", "male", "unknown"])
    d["male"] = np.where(gender_vals == "male", 1.0, np.where(known_gender, 0.0, np.nan))
    d["gender_unknown"] = np.where(gender_vals == "unknown", 1.0, np.where(known_gender, 0.0, np.nan))

    career_vals = pd.Series(d["career_age_clean"]).fillna("unknown").astype(str).to_numpy()
    known_career = np.isin(career_vals, ["<1992", "1992-2001", "2002-2011", ">=2012"])
    d["career_1992_2001"] = np.where(career_vals == "1992-2001", 1.0, np.where(known_career, 0.0, np.nan))
    d["career_2002_2011"] = np.where(career_vals == "2002-2011", 1.0, np.where(known_career, 0.0, np.nan))
    d["career_ge2012"] = np.where(career_vals == ">=2012", 1.0, np.where(known_career, 0.0, np.nan))

    d["income_other"] = (d["income_clean"] == "All Other Income Levels").astype(float)
    d["income_unknown"] = (d["income_clean"].str.lower() == "unknown").astype(float)
    d["male_x_young"] = d["male"] * d["career_ge2012"]

    for fld in FIELD_LEVELS:
        if fld == "Clinical Medicine":
            continue
        safe_name = (
            fld.lower()
            .replace(" & ", "_")
            .replace(",", "")
            .replace(" ", "_")
            .replace("-", "_")
        )
        field_vals = pd.Series(d["field_clean"]).fillna("unknown").astype(str).to_numpy()
        known_field = np.isin(field_vals, FIELD_LEVELS)
        d[f"field_{safe_name}"] = np.where(field_vals == fld, 1.0, np.where(known_field, 0.0, np.nan))

    if use_tertile_pub:
        vals = pd.to_numeric(d["pub_exposure"], errors="coerce").to_numpy(dtype=float)
        w = pd.to_numeric(d[weight_col], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        q1, q2 = weighted_quantile(vals, w, [1.0 / 3.0, 2.0 / 3.0])

        d["pub_tertile"] = np.where(d["pub_exposure"] <= q1, 1, np.where(d["pub_exposure"] <= q2, 2, 3))
        d["pub_t2"] = (d["pub_tertile"] == 2).astype(float)
        d["pub_t3"] = (d["pub_tertile"] == 3).astype(float)

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

    field_feature_cols = sorted([c for c in d.columns if c.startswith("field_") and c != "field_clean"])
    feature_cols += field_feature_cols

    # Male × young career interaction — only added when interactions are requested
    if include_all_interactions or include_field_male_interactions:
        feature_cols.append("male_x_young")

    if include_field_male_interactions or include_all_interactions:
        for col in field_feature_cols:
            int_col = f"male_x_{col}"
            d[int_col] = d["male"] * d[col]
            feature_cols.append(int_col)

    if include_all_interactions:
        # Male × income
        d["male_x_income_other"] = d["male"] * d["income_other"]
        d["male_x_income_unknown"] = d["male"] * d["income_unknown"]
        feature_cols += ["male_x_income_other", "male_x_income_unknown"]
        # Male × publication volume
        if use_tertile_pub:
            d["male_x_pub_t2"] = d["male"] * d["pub_t2"]
            d["male_x_pub_t3"] = d["male"] * d["pub_t3"]
            feature_cols += ["male_x_pub_t2", "male_x_pub_t3"]
        else:
            d["male_x_pub"] = d["male"] * d["pub_exposure"]
            feature_cols.append("male_x_pub")
        # Male × top-cited (only relevant for all-authors model)
        if include_top_cited:
            d["male_x_top_cited"] = d["male"] * d["top_cited_yes"]
            feature_cols.append("male_x_top_cited")

    return d, feature_cols


def fit_univariable(df: pd.DataFrame, feature_sets: dict[str, list[str]], label_col: str, weight_col: str) -> dict[str, dict[str, float]]:
    out = {}
    for _, cols in feature_sets.items():
        out.update(fit_logit(df, cols, label_col=label_col, weight_col=weight_col))
    return out


def _fmt_num(val: float) -> str:
    if not np.isfinite(val):
        return "NA"
    if val <= 0:
        return "0.00"
    if val < 0.01:
        return "<0.01"
    if val > 999.99:
        return ">999.99"
    return f"{val:.2f}"


def _fmt(or_val: float, lcl: float, ucl: float) -> tuple[str, str]:
    return _fmt_num(or_val), f"[{_fmt_num(lcl)}, {_fmt_num(ucl)}]"


def build_display_table(
    univ: dict[str, dict[str, float]],
    multi: dict[str, dict[str, float]],
    publication_label: str,
    include_top_cited: bool,
    include_unknown_gender: bool,
    use_tertile_pub: bool,
    include_interactions: bool = False,
) -> pd.DataFrame:
    rows: list[tuple[str, str, str, str, str, str]] = []

    def add_row(variable: str, level: str, uni_key: str | None = None, multi_key: str | None = None, ref: bool = False) -> None:
        if ref:
            rows.append((variable, level, "Ref", "", "Ref", ""))
            return

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

    add_row("Gender", "Women", ref=True)
    add_row("Gender", "Men", uni_key="male", multi_key="male")
    if include_unknown_gender:
        add_row("Gender", "Unknown", uni_key="gender_unknown", multi_key="gender_unknown")

    add_row("Age career (year of first publication)", "<1992", ref=True)
    add_row("Age career (year of first publication)", "1992-2001", uni_key="career_1992_2001", multi_key="career_1992_2001")
    add_row("Age career (year of first publication)", "2002-2011", uni_key="career_2002_2011", multi_key="career_2002_2011")
    add_row("Age career (year of first publication)", ">=2012", uni_key="career_ge2012", multi_key="career_ge2012")

    add_row("Income level", "High income level", ref=True)
    add_row("Income level", "All other income levels", uni_key="income_other", multi_key="income_other")
    add_row("Income level", "Unknown", uni_key="income_unknown", multi_key="income_unknown")

    if use_tertile_pub:
        add_row("Publication volume (tertile)", "Tertile 1 (lowest)", ref=True)
        add_row("Publication volume (tertile)", "Tertile 2", uni_key="pub_t2", multi_key="pub_t2")
        add_row("Publication volume (tertile)", "Tertile 3 (highest)", uni_key="pub_t3", multi_key="pub_t3")
    else:
        add_row(publication_label, "Per +1 unit", uni_key="pub_exposure", multi_key="pub_exposure")

    if include_top_cited:
        add_row("Top-cited status", "No", ref=True)
        add_row("Top-cited status", "Yes", uni_key="top_cited_yes", multi_key="top_cited_yes")

    add_row("Scientific field", "Clinical Medicine", ref=True)
    for lvl, key in FIELD_MAP.items():
        add_row("Scientific field", lvl, uni_key=key, multi_key=key)

    if include_interactions:
        add_row("Interaction", "Men in youngest cohort (>=2012)", uni_key="male_x_young", multi_key="male_x_young")
        add_row("Interaction", "Men in all other income levels", uni_key="male_x_income_other", multi_key="male_x_income_other")
        add_row("Interaction", "Men in unknown income", uni_key="male_x_income_unknown", multi_key="male_x_income_unknown")
        if use_tertile_pub:
            add_row("Interaction", "Men in Tertile 2 (pub volume)", uni_key="male_x_pub_t2", multi_key="male_x_pub_t2")
            add_row("Interaction", "Men in Tertile 3 (pub volume)", uni_key="male_x_pub_t3", multi_key="male_x_pub_t3")
        else:
            add_row("Interaction", "Men × publication volume", uni_key="male_x_pub", multi_key="male_x_pub")
        if include_top_cited:
            add_row("Interaction", "Men × top-cited", uni_key="male_x_top_cited", multi_key="male_x_top_cited")
        for fld, key in FIELD_MAP.items():
            add_row("Interaction", f"Men × {fld}", uni_key=f"male_x_{key}", multi_key=f"male_x_{key}")

    return pd.DataFrame(
        rows,
        columns=["variable", "level", "univ_or", "univ_95ci", "multiv_or", "multiv_95ci"],
    )


def compute_counts(
    df: pd.DataFrame,
    publication_label: str,
    use_tertile_pub: bool,
    include_top_cited: bool,
    include_unknown_gender: bool,
    label_col: str = "label",
    weight_col: str = "n_obs",
) -> pd.DataFrame:
    """Return (variable, level, n, n_ret) counts from the raw grouped DataFrame."""

    def _c(mask: pd.Series) -> tuple[int, int]:
        sub = df[mask]
        n = int(sub[weight_col].sum())
        n_ret = int((sub[label_col] * sub[weight_col]).sum())
        return n, n_ret

    rows: list[tuple] = []

    g = df["gender_clean"].fillna("unknown")
    for raw, lvl in [("female", "Women"), ("male", "Men")]:
        rows.append(("Gender", lvl, *_c(g == raw)))
    if include_unknown_gender:
        rows.append(("Gender", "Unknown", *_c(g == "unknown")))

    c = df["career_age_clean"].fillna("unknown")
    for lvl in ["<1992", "1992-2001", "2002-2011", ">=2012"]:
        rows.append(("Age career (year of first publication)", lvl, *_c(c == lvl)))

    inc = df["income_clean"].fillna("unknown")
    rows.append(("Income level", "High income level", *_c(inc.str.lower() == "high income")))
    rows.append(("Income level", "All other income levels", *_c(inc == "All Other Income Levels")))
    rows.append(("Income level", "Unknown", *_c(inc.str.lower() == "unknown")))

    # Pub volume: show total n for the single continuous row; tertile rows skipped
    # (tertile thresholds are only available in the encoded df)
    if not use_tertile_pub:
        n_total, n_ret_total = _c(pd.Series(True, index=df.index))
        rows.append((publication_label, "Per +1 unit", n_total, n_ret_total))

    if include_top_cited:
        rows.append(("Top-cited status", "No", *_c(df["top_cited_yes"] == 0.0)))
        rows.append(("Top-cited status", "Yes", *_c(df["top_cited_yes"] == 1.0)))

    fld = df["field_clean"].fillna("unknown")
    rows.append(("Scientific field", "Clinical Medicine", *_c(fld == "Clinical Medicine")))
    for lvl in FIELD_MAP.keys():
        rows.append(("Scientific field", lvl, *_c(fld == lvl)))

    return pd.DataFrame(rows, columns=["variable", "level", "n", "n_ret"])


def build_interaction_table(multi_int: dict[str, dict[str, float]]) -> pd.DataFrame:
    rows: list[tuple[str, str, str]] = []
    rows.append(("Clinical Medicine", "Ref", ""))
    for fld, key in FIELD_MAP.items():
        int_key = f"male_x_{key}"
        m = multi_int.get(int_key, {})
        if m and all(k in m for k in ("or", "lcl", "ucl")):
            or_str, ci_str = _fmt(m["or"], m["lcl"], m["ucl"])
        else:
            or_str, ci_str = "", ""
        rows.append((fld, or_str, ci_str))
    return pd.DataFrame(rows, columns=["field", "interaction_or", "interaction_95ci"])


def build_combined_csv(
    all_tbl: pd.DataFrame,
    top_tbl: pd.DataFrame,
    all_counts: pd.DataFrame | None = None,
    top_counts: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Merge all-authors and top-cited tables into a 6-column combined CSV.

    Columns: variable, level,
             all_univ_or+ci, all_multiv_or+ci,
             top_univ_or+ci, top_multiv_or+ci
    OR and 95% CI are merged into a single string "OR (LCL-UCL)" for display.
    Ref rows stay as "Ref".
    """
    def _merge_or_ci(or_val: str, ci_val: str) -> str:
        if or_val == "Ref":
            return "Ref"
        if not or_val:
            return ""
        ci_fmt = ci_val.strip("[]").replace(", ", "-") if ci_val else ""
        return f"{or_val} ({ci_fmt})" if ci_fmt else or_val

    all_d = {(r["variable"], r["level"]): r.to_dict() for _, r in all_tbl.iterrows()}
    top_d = {(r["variable"], r["level"]): r.to_dict() for _, r in top_tbl.iterrows()}

    # Use all_tbl row order as template; supplement with top-only rows at the bottom
    seen = set()
    combined_rows = []
    for _, row in all_tbl.iterrows():
        key = (row["variable"], row["level"])
        seen.add(key)
        top_row = top_d.get(key, {})
        combined_rows.append(dict(
            variable=row["variable"],
            level=row["level"],
            all_univ=_merge_or_ci(row.get("univ_or", ""), row.get("univ_95ci", "")),
            all_multiv=_merge_or_ci(row.get("multiv_or", ""), row.get("multiv_95ci", "")),
            top_univ=_merge_or_ci(top_row.get("univ_or", ""), top_row.get("univ_95ci", "")) if top_row else "",
            top_multiv=_merge_or_ci(top_row.get("multiv_or", ""), top_row.get("multiv_95ci", "")) if top_row else "",
        ))
    # Rows in top_tbl that aren't in all_tbl (e.g. if interaction sets differ)
    for _, row in top_tbl.iterrows():
        key = (row["variable"], row["level"])
        if key not in seen:
            combined_rows.append(dict(
                variable=row["variable"],
                level=row["level"],
                all_univ="", all_multiv="",
                top_univ=_merge_or_ci(row.get("univ_or", ""), row.get("univ_95ci", "")),
                top_multiv=_merge_or_ci(row.get("multiv_or", ""), row.get("multiv_95ci", "")),
            ))
    # Merge counts if provided
    combined = pd.DataFrame(combined_rows, columns=["variable", "level", "all_univ", "all_multiv", "top_univ", "top_multiv"])
    if all_counts is not None:
        all_counts_r = all_counts.rename(columns={"n": "all_n", "n_ret": "all_n_ret"})
        combined = combined.merge(all_counts_r[["variable", "level", "all_n", "all_n_ret"]], on=["variable", "level"], how="left")
    else:
        combined["all_n"] = pd.NA
        combined["all_n_ret"] = pd.NA
    if top_counts is not None:
        top_counts_r = top_counts.rename(columns={"n": "top_n", "n_ret": "top_n_ret"})
        combined = combined.merge(top_counts_r[["variable", "level", "top_n", "top_n_ret"]], on=["variable", "level"], how="left")
    else:
        combined["top_n"] = pd.NA
        combined["top_n_ret"] = pd.NA
    col_order = ["variable", "level", "all_n", "all_n_ret", "all_univ", "all_multiv", "top_n", "top_n_ret", "top_univ", "top_multiv"]
    return combined[col_order]


def run_single_config(base_df: pd.DataFrame, config: dict, include_unknown_gender: bool, weight_col: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    analysis_df = base_df.copy()
    analysis_df["pub_exposure"] = pd.to_numeric(analysis_df[config["exposure_col"]], errors="coerce")
    analysis_df = analysis_df[analysis_df["pub_exposure"].notna()].copy()

    all_df, all_features = add_reference_dummies(
        analysis_df,
        include_top_cited=True,
        include_unknown_gender=include_unknown_gender,
        use_tertile_pub=config["use_tertile_pub"],
        weight_col=weight_col,
    )

    all_univ_sets = {
        "male": ["male"] + (["gender_unknown"] if include_unknown_gender else []),
        "career_1992_2001": ["career_1992_2001", "career_2002_2011", "career_ge2012"],
        "income_other": ["income_other", "income_unknown"],
        "top_cited_yes": ["top_cited_yes"],
    }
    if config["use_tertile_pub"]:
        all_univ_sets["pub_t2"] = ["pub_t2", "pub_t3"]
    else:
        all_univ_sets["pub_exposure"] = ["pub_exposure"]

    all_field_cols = [c for c in all_features if c.startswith("field_")]
    if all_field_cols:
        all_univ_sets[all_field_cols[0]] = all_field_cols

    all_univ = fit_univariable(all_df, all_univ_sets, label_col="label", weight_col=weight_col)
    all_multi = fit_logit(all_df, all_features, label_col="label", weight_col=weight_col)

    all_table = build_display_table(
        all_univ,
        all_multi,
        publication_label=config["display_label"],
        include_top_cited=True,
        include_unknown_gender=include_unknown_gender,
        use_tertile_pub=config["use_tertile_pub"],
        include_interactions=False,
    )

    top_df0 = analysis_df[analysis_df["top_cited_yes"] == 1.0].copy()
    top_df, top_features = add_reference_dummies(
        top_df0,
        include_top_cited=False,
        include_unknown_gender=include_unknown_gender,
        use_tertile_pub=config["use_tertile_pub"],
        weight_col=weight_col,
    )

    top_univ_sets = {
        "male": ["male"] + (["gender_unknown"] if include_unknown_gender else []),
        "career_1992_2001": ["career_1992_2001", "career_2002_2011", "career_ge2012"],
        "income_other": ["income_other", "income_unknown"],
    }
    if config["use_tertile_pub"]:
        top_univ_sets["pub_t2"] = ["pub_t2", "pub_t3"]
    else:
        top_univ_sets["pub_exposure"] = ["pub_exposure"]

    top_field_cols = [c for c in top_features if c.startswith("field_")]
    if top_field_cols:
        top_univ_sets[top_field_cols[0]] = top_field_cols

    top_univ = fit_univariable(top_df, top_univ_sets, label_col="label", weight_col=weight_col)
    top_multi = fit_logit(top_df, top_features, label_col="label", weight_col=weight_col)

    top_table = build_display_table(
        top_univ,
        top_multi,
        publication_label=config["display_label"],
        include_top_cited=False,
        include_unknown_gender=include_unknown_gender,
        use_tertile_pub=config["use_tertile_pub"],
        include_interactions=False,
    )

    # ---- Full-interaction models ----
    all_df_fi, all_features_fi = add_reference_dummies(
        analysis_df,
        include_top_cited=True,
        include_unknown_gender=include_unknown_gender,
        use_tertile_pub=config["use_tertile_pub"],
        weight_col=weight_col,
        include_all_interactions=True,
    )
    all_multi_fi = fit_logit(all_df_fi, all_features_fi, label_col="label", weight_col=weight_col)

    # Univariable estimates for every interaction term (each fitted as a single-predictor model)
    all_int_cols_fi = [c for c in all_features_fi if c.startswith("male_x_")]
    all_int_univ_sets = {c: [c] for c in all_int_cols_fi}
    all_int_univ = fit_univariable(all_df_fi, all_int_univ_sets, label_col="label", weight_col=weight_col)
    all_univ_fi = {**all_univ, **all_int_univ}

    all_table_fi = build_display_table(
        all_univ_fi,
        all_multi_fi,
        publication_label=config["display_label"],
        include_top_cited=True,
        include_unknown_gender=include_unknown_gender,
        use_tertile_pub=config["use_tertile_pub"],
        include_interactions=True,
    )

    top_df_fi, top_features_fi = add_reference_dummies(
        top_df0,
        include_top_cited=False,
        include_unknown_gender=include_unknown_gender,
        use_tertile_pub=config["use_tertile_pub"],
        weight_col=weight_col,
        include_all_interactions=True,
    )
    top_multi_fi = fit_logit(top_df_fi, top_features_fi, label_col="label", weight_col=weight_col)

    # Univariable estimates for every interaction term (top-cited subset)
    top_int_cols_fi = [c for c in top_features_fi if c.startswith("male_x_")]
    top_int_univ_sets = {c: [c] for c in top_int_cols_fi}
    top_int_univ = fit_univariable(top_df_fi, top_int_univ_sets, label_col="label", weight_col=weight_col)
    top_univ_fi = {**top_univ, **top_int_univ}

    top_table_fi = build_display_table(
        top_univ_fi,
        top_multi_fi,
        publication_label=config["display_label"],
        include_top_cited=False,
        include_unknown_gender=include_unknown_gender,
        use_tertile_pub=config["use_tertile_pub"],
        include_interactions=True,
    )

    # ---- Field-only interaction tables (legacy) ----
    all_df_int, all_features_int = add_reference_dummies(
        analysis_df,
        include_top_cited=True,
        include_unknown_gender=include_unknown_gender,
        use_tertile_pub=config["use_tertile_pub"],
        weight_col=weight_col,
        include_field_male_interactions=True,
    )
    all_multi_int = fit_logit(all_df_int, all_features_int, label_col="label", weight_col=weight_col)
    all_int_table = build_interaction_table(all_multi_int)

    top_df_int, top_features_int = add_reference_dummies(
        top_df0,
        include_top_cited=False,
        include_unknown_gender=include_unknown_gender,
        use_tertile_pub=config["use_tertile_pub"],
        weight_col=weight_col,
        include_field_male_interactions=True,
    )
    top_multi_int = fit_logit(top_df_int, top_features_int, label_col="label", weight_col=weight_col)
    top_int_table = build_interaction_table(top_multi_int)

    # Counts from raw data (before encoding)
    all_counts = compute_counts(
        analysis_df,
        publication_label=config["display_label"],
        use_tertile_pub=config["use_tertile_pub"],
        include_top_cited=True,
        include_unknown_gender=True,
        weight_col=weight_col,
    )
    top_counts = compute_counts(
        top_df0,
        publication_label=config["display_label"],
        use_tertile_pub=config["use_tertile_pub"],
        include_top_cited=False,
        include_unknown_gender=True,
        weight_col=weight_col,
    )

    return all_table, top_table, all_table_fi, top_table_fi, all_int_table, top_int_table, all_counts, top_counts


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    out_base = Path(args.output_dir)
    out_base.mkdir(parents=True, exist_ok=True)

    base_df = pd.read_parquet(input_path)

    if args.weight_col not in base_df.columns:
        base_df[args.weight_col] = 1.0

    base_df["gender_clean"] = base_df["gender_clean"].astype(str).str.lower()
    base_df["field_clean"] = base_df["field_clean"].astype("string").str.strip()
    base_df["income_clean"] = base_df["income_clean"].fillna("unknown").astype(str)
    base_df["career_age_clean"] = base_df["career_age_clean"].astype("string")
    base_df["top_cited_yes"] = pd.to_numeric(base_df["top_cited_yes"], errors="coerce").fillna(0.0)
    base_df["label"] = pd.to_numeric(base_df["label"], errors="coerce")
    base_df = base_df[base_df["label"].notna()].copy()

    for cfg in RUN_CONFIGS:
        cfg_dir = out_base / cfg["name"]
        cfg_dir.mkdir(parents=True, exist_ok=True)

        all_table, top_table, all_table_fi, top_table_fi, all_int_table, top_int_table, all_counts, top_counts = run_single_config(
            base_df,
            cfg,
            include_unknown_gender=True,  # Always include unknown gender as explicit category
            weight_col=args.weight_col,
        )

        all_table_out = cfg_dir / "all_authors_or_table.csv"
        top_table_out = cfg_dir / "top_cited_or_table.csv"
        combined_out = cfg_dir / "combined_or_table.csv"

        all_table.to_csv(all_table_out, index=False)
        top_table.to_csv(top_table_out, index=False)
        pd.concat(
            [
                all_table.assign(cohort="all_authors"),
                top_table.assign(cohort="top_cited_authors"),
            ],
            ignore_index=True,
        ).to_csv(combined_out, index=False)

        print(f"[{cfg['name']}] wrote {all_table_out}")
        print(f"[{cfg['name']}] wrote {top_table_out}")
        print(f"[{cfg['name']}] wrote {combined_out}")

        all_int_out = cfg_dir / "all_authors_field_male_interactions.csv"
        top_int_out = cfg_dir / "top_cited_field_male_interactions.csv"
        all_int_table.to_csv(all_int_out, index=False)
        top_int_table.to_csv(top_int_out, index=False)
        print(f"[{cfg['name']}] wrote {all_int_out}")
        print(f"[{cfg['name']}] wrote {top_int_out}")

        # Combined CSV: main effects from additive model + interactions from full-interaction model
        if cfg["name"] in {"papers_log10", "paperyears_log10"}:
            # Main effects: use the no-interaction model (interpretable population-average ORs)
            # Interactions: use the full-interaction model (where those terms are properly estimated)
            all_table_mixed = pd.concat([
                all_table[all_table["variable"] != "Interaction"],
                all_table_fi[all_table_fi["variable"] == "Interaction"],
            ], ignore_index=True)
            top_table_mixed = pd.concat([
                top_table[top_table["variable"] != "Interaction"],
                top_table_fi[top_table_fi["variable"] == "Interaction"],
            ], ignore_index=True)
            combined_out = cfg_dir / "combined.csv"
            build_combined_csv(all_table_mixed, top_table_mixed, all_counts, top_counts).to_csv(combined_out, index=False)
            print(f"[{cfg['name']}] wrote {combined_out}")


if __name__ == "__main__":
    main()

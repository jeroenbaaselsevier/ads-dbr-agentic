#!/usr/bin/env python3
"""
Standalone validation: reproduce the papers_log10 OR table.

Reads the grouped CSV (one row per unique combination of predictors, n_obs = number
of authors in that combination) and fits weighted logistic regression using freq_weights
directly on the ~760k grouped rows - identical results to expanding to 10M rows but
orders of magnitude faster.

Sections (top-cited first - smaller sample, fastest sanity check):
  1. TOP-CITED: univariable, additive, interactions
  2. ALL AUTHORS: univariable, additive, interactions

Requirements: pandas, numpy, statsmodels
"""
import re
import time
import datetime
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from statsmodels.genmod.families import Binomial

# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA = "../output/grouped_gender_careerage_income_field_topcited_papers.csv"
MAP  = "../output/grouped_gender_careerage_income_field_topcited_papers_map.csv"

# ---------------------------------------------------------------------------
# Load data and build label lookup  {column: {integer_code: label_string}}
# ---------------------------------------------------------------------------
log("Loading data ...")
df   = pd.read_csv(DATA)
vmap = pd.read_csv(MAP)

label_lookup: dict[str, dict[int, str]] = {}
for col, grp in vmap.groupby("column"):
    label_lookup[col] = dict(zip(grp["integer_code"].astype(int), grp["label"]))

# Use freq_weights on grouped rows - no expansion needed, identical likelihood
n_total     = int(df["n_obs"].sum())
n_retracted = int((df.loc[df["label"] == 1, "n_obs"]).sum())
log(f"All authors : {n_total:>12,}")
log(f"Retracted   : {n_retracted:>12,}  ({100 * n_retracted / n_total:.3f} %)")

# top_cited code 2 = "1 - top-cited"
df_top      = df[df["top_cited"] == 2].copy()
n_top       = int(df_top["n_obs"].sum())
n_top_ret   = int(df_top.loc[df_top["label"] == 1, "n_obs"].sum())
log(f"Top-cited   : {n_top:>12,}")
log(f"Retracted   : {n_top_ret:>12,}  ({100 * n_top_ret / n_top:.3f} %)")

# ---------------------------------------------------------------------------
# Helper: readable term name  e.g. "C(gender)[T.2]" → "Male"
# ---------------------------------------------------------------------------
_TERM_RE = re.compile(r"C\((\w+)\)\[T\.(\d+)\]")

def _readable_part(part: str) -> str:
    """Translate a single (non-interaction) term like C(gender)[T.2] → 'male'."""
    m = _TERM_RE.fullmatch(part.strip())
    if m:
        col, code = m.group(1), int(m.group(2))
        return label_lookup.get(col, {}).get(code, part.strip())
    if part.strip() == "papers_log10":
        return "pub_vol(log10)"
    return part.strip()

def _readable(term: str) -> str:
    """Translate a term that may contain interaction (':') separators."""
    if ":" in term:
        return " × ".join(_readable_part(p) for p in term.split(":"))
    return _readable_part(term)

# ---------------------------------------------------------------------------
# Helper: fit weighted GLM (Binomial) and print OR table
# freq_weights treats each row as representing n_obs identical observations -
# mathematically identical to expanding the data.
# ---------------------------------------------------------------------------
def or_table(title: str, formula: str, data: pd.DataFrame,
             only_interactions: bool = False) -> None:
    log(f"Fitting: {title}")
    t0 = time.time()
    try:
        result = smf.glm(
            formula, data=data,
            family=Binomial(),
            freq_weights=data["n_obs"],
        ).fit(disp=False, maxiter=200)
    except Exception as exc:
        log(f"  [model failed: {exc}]")
        return
    log(f"  done in {time.time() - t0:.1f}s  (iterations: {result.fit_history['iteration']})")

    ci = result.conf_int()
    rows = []
    for term in result.params.index:
        if term == "Intercept":
            continue
        is_interaction = ":" in term
        if only_interactions and not is_interaction:
            continue
        lo = np.exp(ci.loc[term, 0])
        hi = np.exp(ci.loc[term, 1])
        rows.append({
            "Term":    _readable(term),
            "OR":      f"{np.exp(result.params[term]):.3f}",
            "95% CI":  f"({lo:.3f} \u2013 {hi:.3f})",
            "p-value": f"{result.pvalues[term]:.4g}",
        })

    if not rows:
        log("  (no terms to display)")
        return

    print(pd.DataFrame(rows).set_index("Term").to_string())
    print()


# ===========================================================================
# TOP-CITED AUTHORS ONLY  (run first - smaller, fast sanity check)
# ===========================================================================
print("\n" + "=" * 72)
log("TOP-CITED AUTHORS ONLY")
print("=" * 72)

for var, label in [
    ("C(gender)",     "Gender"),
    ("C(career_age)", "Career age"),
    ("C(income)",     "Income level"),
    ("papers_log10",  "Publication volume (log10)"),
    ("C(field)",      "Scientific field"),
]:
    or_table(f"Univariable: {label}", f"label ~ {var}", df_top)

or_table(
    "Multivariable ADDITIVE - main effects",
    "label ~ C(gender) + C(career_age) + C(income) + papers_log10 + C(field)",
    df_top,
)
or_table(
    "Multivariable × gender - INTERACTION TERMS ONLY",
    ("label ~ C(gender) * C(career_age)"
     " + C(gender) * C(income)"
     " + C(gender) * papers_log10"
     " + C(gender) * C(field)"),
    df_top, only_interactions=True,
)


# ===========================================================================
# ALL AUTHORS
# ===========================================================================
print("\n" + "=" * 72)
log("ALL AUTHORS")
print("=" * 72)

for var, label in [
    ("C(gender)",     "Gender"),
    ("C(career_age)", "Career age"),
    ("C(income)",     "Income level"),
    ("C(top_cited)",  "Top-cited status"),
    ("papers_log10",  "Publication volume (log10)"),
    ("C(field)",      "Scientific field"),
]:
    or_table(f"Univariable: {label}", f"label ~ {var}", df)

or_table(
    "Multivariable ADDITIVE - main effects",
    ("label ~ C(gender) + C(career_age) + C(income)"
     " + C(top_cited) + papers_log10 + C(field)"),
    df,
)
or_table(
    "Multivariable × gender - INTERACTION TERMS ONLY",
    ("label ~ C(gender) * C(career_age)"
     " + C(gender) * C(income)"
     " + C(gender) * papers_log10"
     " + C(gender) * C(top_cited)"
     " + C(gender) * C(field)"),
    df, only_interactions=True,
)

log("All done.")

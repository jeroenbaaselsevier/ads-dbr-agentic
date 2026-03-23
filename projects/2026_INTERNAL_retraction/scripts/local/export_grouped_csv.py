#!/usr/bin/env python3
"""Export grouped parquet as a fully numeric CSV for external validation.

Each categorical column is integer-indexed with 1 = reference category.
Continuous and count columns are kept as-is.
A companion map CSV is written alongside the data file.

Output files
------------
grouped_gender_careerage_income_field_topcited_papers.csv
grouped_gender_careerage_income_field_topcited_papers_map.csv
"""
from __future__ import annotations

import argparse
import glob
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Encoding definitions  (ref = 1 always)
# ---------------------------------------------------------------------------

ENCODINGS: dict[str, list[tuple[int, str]]] = {
    "gender": [
        (1, "female"),          # reference
        (2, "male"),
        (3, "unknown"),
    ],
    "career_age": [
        (1, "<1992"),           # reference
        (2, "1992-2001"),
        (3, "2002-2011"),
        (4, ">=2012"),
    ],
    "income": [
        (1, "High Income"),     # reference
        (2, "All Other Income Levels"),
        (3, "unknown"),
    ],
    "field": [
        (1,  "Clinical Medicine"),                          # reference
        (2,  "Agriculture, Fisheries & Forestry"),
        (3,  "Biology"),
        (4,  "Biomedical Research"),
        (5,  "Built Environment & Design"),
        (6,  "Chemistry"),
        (7,  "Communication & Textual Studies"),
        (8,  "Earth & Environmental Sciences"),
        (9,  "Economics & Business"),
        (10, "Enabling & Strategic Technologies"),
        (11, "Engineering"),
        (12, "General Arts, Humanities & Social Sciences"),
        (13, "General Science & Technology"),
        (14, "Historical Studies"),
        (15, "Information & Communication Technologies"),
        (16, "Mathematics & Statistics"),
        (17, "Philosophy & Theology"),
        (18, "Physics & Astronomy"),
        (19, "Psychology & Cognitive Sciences"),
        (20, "Public Health & Health Services"),
        (21, "Social Sciences"),
        (22, "Visual & Performing Arts"),
        (23, "Unknown / not assigned"),
    ],
    "top_cited": [
        (1, "0 — not top-cited"),   # reference
        (2, "1 — top-cited"),
    ],
}

SOURCE_COL_MAP = {
    "gender":     "gender_clean",
    "career_age": "career_age_clean",
    "income":     "income_clean",
    "field":      "field_clean",
    "top_cited":  "top_cited_yes",
}


def build_lookup(enc: list[tuple[int, str]]) -> dict:
    return {label: code for code, label in enc}


def encode_column(series: pd.Series, enc: list[tuple[int, str]], col_name: str) -> pd.Series:
    lookup = build_lookup(enc)
    # top_cited source is float 0.0/1.0 — convert to string key form
    if col_name == "top_cited":
        lookup_f = {0.0: 1, 1.0: 2}
        return series.map(lookup_f).astype("Int64")
    # Fill NaN with sentinel before mapping so nulls get their own code
    filled = series.fillna("__NULL__")
    # Add null mapping if the encoding includes an "Unknown" sentinel entry
    null_code = next((code for code, label in enc if "unknown" in label.lower() or "not assigned" in label.lower()
                      and code == max(c for c, _ in enc)), None)
    if null_code is not None:
        lookup["__NULL__"] = null_code
    encoded = filled.map(lookup)
    n_missing = encoded.isna().sum()
    if n_missing > 0:
        unmapped = series[encoded.isna()].unique().tolist()[:10]
        print(f"  WARNING: {col_name} — {n_missing} rows could not be mapped: {unmapped}")
    return encoded.astype("Int64")


def build_map_df() -> pd.DataFrame:
    rows = []
    for col_name, enc in ENCODINGS.items():
        for code, label in enc:
            rows.append({
                "column": col_name,
                "integer_code": code,
                "label": label,
                "is_reference": code == 1,
            })
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", default="tmp/retraction_local_input/grouped_parquet",
        help="Path to grouped parquet directory (default: tmp/retraction_local_input/grouped_parquet)",
    )
    parser.add_argument(
        "--output-dir", default="projects/2026_INTERNAL_retraction/output",
        help="Directory to write output files",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    stem = "grouped_gender_careerage_income_field_topcited_papers"
    data_out = out_dir / f"{stem}.csv"
    map_out  = out_dir / f"{stem}_map.csv"

    # ---- Load ----
    parquet_files = sorted(glob.glob(str(input_path / "*.parquet")))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {input_path}")
    print(f"Loading {len(parquet_files)} parquet files ...")
    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    print(f"  Loaded {len(df):,} rows, {df['n_obs'].sum():,.0f} total authors")

    # ---- Build output ----
    out = pd.DataFrame()
    out["label"]         = df["label"].astype(int)   # 1 = has retraction, 0 = no retraction
    for out_col, src_col in SOURCE_COL_MAP.items():
        print(f"  Encoding {out_col} (source: {src_col}) ...")
        out[out_col] = encode_column(df[src_col], ENCODINGS[out_col], out_col)
    out["papers_log10"] = df["papers_log10"].round(6)
    out["n_obs"]         = df["n_obs"].astype(int)

    # Drop rows where any categorical could not be mapped (should be zero if all nulls handled)
    n_before = len(out)
    out = out.dropna(subset=list(SOURCE_COL_MAP.keys()))
    n_dropped = n_before - len(out)
    if n_dropped:
        print(f"  WARNING: Dropped {n_dropped:,} rows with unmappable categories")

    # ---- Write data ----
    out.to_csv(data_out, index=False)
    print(f"\nData written: {data_out}  ({len(out):,} rows)")

    # ---- Write map ----
    map_df = build_map_df()
    map_df.to_csv(map_out, index=False)
    print(f"Map  written: {map_out}  ({len(map_df)} entries)")

    # ---- Summary ----
    print("\nColumn summary:")
    for col in out.columns:
        if col in ("papers_log10",):
            print(f"  {col}: continuous, range [{out[col].min():.3f}, {out[col].max():.3f}]")
        else:
            print(f"  {col}: {sorted(out[col].dropna().unique().tolist())}")


if __name__ == "__main__":
    main()

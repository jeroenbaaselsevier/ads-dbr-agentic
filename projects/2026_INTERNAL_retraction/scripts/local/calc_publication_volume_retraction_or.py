#!/usr/bin/env python3
"""Calculate the odds ratio for having >=1 retraction by publication volume.

This fits a univariable logistic regression:
    I(np6023_rw >= 1) ~ np6023

Example:
    .venv/bin/python projects/2026_INTERNAL_retraction/scripts/local/calc_publication_volume_retraction_or.py \
        --input projects/2026_INTERNAL_retraction/tmp/Table_1_Authors_career_2023_pubs_since_1788_wopp_extracted_202408.xlsx \
        --output projects/2026_INTERNAL_retraction/output/publication_volume_retraction_or.json

The input can be either Excel or parquet. For Excel, the script will auto-detect
the first sheet containing the requested columns unless --sheet is provided.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to input .xlsx file")
    parser.add_argument(
        "--sheet",
        default=None,
        help="Sheet name to read. Defaults to the first sheet containing the required columns.",
    )
    parser.add_argument(
        "--predictor",
        default="np6023",
        help="Continuous predictor column for publication volume",
    )
    parser.add_argument(
        "--transform",
        choices=["raw", "log10"],
        default="raw",
        help="Optional transform applied to the predictor before fitting",
    )
    parser.add_argument(
        "--outcome",
        default="np6023_rw",
        help="Outcome count column; rows with value >= threshold are treated as cases",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=1.0,
        help="Outcome threshold used to define cases (default: >= 1)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional JSON output path",
    )
    parser.add_argument(
        "--weight-col",
        default=None,
        help="Optional frequency-weight column (for grouped data, e.g. n_obs)",
    )
    return parser.parse_args()


def find_sheet(path: Path, requested_sheet: str | None, required_columns: set[str]) -> str:
    workbook = pd.ExcelFile(path)

    if requested_sheet is not None:
        if requested_sheet not in workbook.sheet_names:
            raise ValueError(
                f"Sheet '{requested_sheet}' not found. Available sheets: {workbook.sheet_names}"
            )
        return requested_sheet

    for sheet_name in workbook.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet_name, nrows=5)
        if required_columns.issubset(df.columns):
            return sheet_name

    raise ValueError(
        "Could not find a sheet containing columns "
        f"{sorted(required_columns)}. Available sheets: {workbook.sheet_names}"
    )


def read_input_frame(path: Path, requested_sheet: str | None, required_columns: set[str]) -> tuple[pd.DataFrame, str]:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        sheet_name = find_sheet(path, requested_sheet, required_columns)
        return pd.read_excel(path, sheet_name=sheet_name), sheet_name
    if suffix == ".parquet":
        return pd.read_parquet(path), "parquet"
    raise ValueError(f"Unsupported input type for {path}. Expected Excel or parquet.")


def load_analysis_frame(
    path: Path,
    sheet_name: str | None,
    predictor: str,
    outcome: str,
    threshold: float,
    transform: str,
    weight_col: str | None,
) -> tuple[pd.DataFrame, np.ndarray, np.ndarray, str]:
    required_cols = {predictor, outcome}
    if weight_col:
        required_cols.add(weight_col)

    df, resolved_source = read_input_frame(path, sheet_name, required_cols)

    missing = [column for column in required_cols if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    keep_cols = [predictor, outcome] + ([weight_col] if weight_col else [])
    analysis = df[keep_cols].copy()
    analysis[predictor] = pd.to_numeric(analysis[predictor], errors="coerce")
    analysis[outcome] = pd.to_numeric(analysis[outcome], errors="coerce")
    analysis = analysis.dropna(subset=[predictor, outcome])

    transformed_predictor = predictor
    if transform == "log10":
        if (analysis[predictor] <= 0).any():
            raise ValueError("log10 transform requires all predictor values to be > 0")
        transformed_predictor = f"log10_{predictor}"
        analysis[transformed_predictor] = np.log10(analysis[predictor])

    x = analysis[transformed_predictor].to_numpy(dtype=float)
    y = (analysis[outcome].to_numpy(dtype=float) >= threshold).astype(float)

    if len(analysis) == 0:
        raise ValueError("No complete rows remain after dropping missing values")

    if y.min() == y.max():
        raise ValueError("Outcome has no variation after thresholding; odds ratio is undefined")

    return analysis, x, y, resolved_source


def fit_univariable_logistic(x: np.ndarray, y: np.ndarray, freq_weights: np.ndarray) -> dict[str, float | int | bool]:
    x_centered = x - x.mean()
    design = np.column_stack([np.ones_like(x_centered), x_centered])

    weighted_n = float(np.sum(freq_weights))
    weighted_mean_y = float(np.clip(np.sum(freq_weights * y) / weighted_n, 1e-9, 1 - 1e-9))
    mean_y = weighted_mean_y
    beta = np.array([math.log(mean_y / (1 - mean_y)), 0.0], dtype=float)

    converged = False
    max_iter = 100

    for iteration in range(1, max_iter + 1):
        eta = design @ beta
        eta = np.clip(eta, -30, 30)
        p = 1.0 / (1.0 + np.exp(-eta))
        w = np.clip(p * (1.0 - p), 1e-9, None)
        w_eff = w * freq_weights
        z = eta + (y - p) / w

        xtwx = design.T @ (design * w_eff[:, None])
        xtwz = design.T @ (w_eff * z)
        beta_new = np.linalg.solve(xtwx, xtwz)

        if np.max(np.abs(beta_new - beta)) < 1e-10:
            beta = beta_new
            converged = True
            break

        beta = beta_new

    if not converged:
        raise RuntimeError("Logistic regression did not converge")

    eta = np.clip(design @ beta, -30, 30)
    p = 1.0 / (1.0 + np.exp(-eta))
    w = np.clip(p * (1.0 - p), 1e-9, None)
    w_eff = w * freq_weights
    fisher_information = design.T @ (design * w_eff[:, None])
    covariance = np.linalg.inv(fisher_information)

    slope = float(beta[1])
    slope_se = float(np.sqrt(covariance[1, 1]))
    ci_low = math.exp(slope - 1.96 * slope_se)
    ci_high = math.exp(slope + 1.96 * slope_se)

    return {
        "iterations": iteration,
        "converged": converged,
        "beta": slope,
        "beta_se": slope_se,
        "odds_ratio": math.exp(slope),
        "odds_ratio_ci_low": ci_low,
        "odds_ratio_ci_high": ci_high,
    }


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    analysis, x, y, resolved_source = load_analysis_frame(
        input_path,
        sheet_name=args.sheet,
        predictor=args.predictor,
        outcome=args.outcome,
        threshold=args.threshold,
        transform=args.transform,
        weight_col=args.weight_col,
    )
    if args.weight_col:
        if args.weight_col not in analysis.columns:
            raise ValueError(f"Weight column '{args.weight_col}' not found in input")
        freq_weights = pd.to_numeric(analysis[args.weight_col], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        if (freq_weights < 0).any():
            raise ValueError("Weight column contains negative values")
        keep = freq_weights > 0
        analysis = analysis.loc[keep].copy()
        x = x[keep]
        y = y[keep]
        freq_weights = freq_weights[keep]
        if len(analysis) == 0:
            raise ValueError("No rows remain after applying positive-weight filter")
    else:
        freq_weights = np.ones(len(analysis), dtype=float)

    fit = fit_univariable_logistic(x, y, freq_weights)

    result = {
        "input": str(input_path),
        "sheet": resolved_source,
        "predictor": args.predictor,
        "transform": args.transform,
        "outcome": args.outcome,
        "outcome_threshold": args.threshold,
        "n_rows": int(len(analysis)),
        "n_cases": int(y.sum()),
        "n_non_cases": int(len(y) - y.sum()),
        "effective_n": float(freq_weights.sum()),
        "weight_col": args.weight_col,
        "predictor_mean": float(np.mean(x)),
        "predictor_median": float(np.median(x)),
        **fit,
    }

    print(f"Input: {result['input']}")
    print(f"Sheet: {result['sheet']}")
    print(f"Predictor transform: {result['transform']}")
    print(
        f"Outcome definition: {result['outcome']} >= {result['outcome_threshold']}"
    )
    print(f"Rows analysed: {result['n_rows']}")
    print(
        f"Cases / non-cases: {result['n_cases']} / {result['n_non_cases']}"
    )
    print(
        "Odds ratio for publication volume "
        f"({result['predictor']}, {result['transform']} scale, per +1 unit): {result['odds_ratio']:.6f} "
        f"(95% CI {result['odds_ratio_ci_low']:.6f} to {result['odds_ratio_ci_high']:.6f})"
    )

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(f"Saved JSON: {output_path}")


if __name__ == "__main__":
    main()
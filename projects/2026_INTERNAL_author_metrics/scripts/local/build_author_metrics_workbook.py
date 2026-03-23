#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs as pafs


SUMMARY_COLUMNS = [
    'Scopus ID',
    'Nr.',
    'Last Name',
    'First Name',
    'First Year Publication',
    'Scholary Output',
    'Citations',
    'Top citation percentiles (top 10 %)',
    'Top journal percentiles (Top 10%)',
    'h-index',
    'International Collaboration',
    'Cumulative SNIP (2024)',
    'Cumulative CiteScore (2024)',
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='Parquet folder with author-eid level rows')
    parser.add_argument('--output', required=True, help='Output workbook path (.xlsx)')
    return parser.parse_args()


def load_parquet(input_path: str) -> pd.DataFrame:
    if input_path.startswith('s3://'):
        parsed = urlparse(input_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        s3 = pafs.S3FileSystem(region='us-east-1')
        dataset = ds.dataset(f'{bucket}/{key}', filesystem=s3, format='parquet')
    else:
        dataset = ds.dataset(input_path, format='parquet')
    return dataset.to_table().to_pandas()


def compute_h_index(citations: pd.Series) -> int:
    ordered = sorted((int(x) for x in citations.fillna(0).tolist()), reverse=True)
    h_index = 0
    for idx, value in enumerate(ordered, start=1):
        if value >= idx:
            h_index = idx
        else:
            break
    return h_index


def join_unique(values: pd.Series) -> str:
    cleaned = []
    seen = set()
    for value in values.dropna():
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return '; '.join(cleaned)


def join_unique_names(values: pd.Series) -> str:
    cleaned = []
    seen = set()
    for value in values.dropna():
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return ' / '.join(cleaned)


def build_summary(df_raw: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = [
        'citations_nowindow',
        'fwci_4y',
        'fwci_4y_percentile',
        'is_top10_fwci_4y',
        'best_citescore_percentile',
        'is_top10_journal',
        'best_citescore_percentile_2024',
        'is_top10_journal_2024',
        'citescore_2024',
        'snip_2024',
        'sort_year',
    ]
    for column in numeric_cols:
        if column in df_raw.columns:
            df_raw[column] = pd.to_numeric(df_raw[column], errors='coerce')

    df_raw['nr'] = pd.to_numeric(df_raw['nr'], errors='raise').astype(int)
    df_raw['auid'] = pd.to_numeric(df_raw['auid'], errors='raise').astype('Int64')
    df_raw['eid'] = pd.to_numeric(df_raw['eid'], errors='raise').astype('Int64')

    best_pct_col = 'best_citescore_percentile' if 'best_citescore_percentile' in df_raw.columns else 'best_citescore_percentile_2024'
    top_journal_col = 'is_top10_journal' if 'is_top10_journal' in df_raw.columns else 'is_top10_journal_2024'

    df_dedup = (
        df_raw
        .sort_values(['nr', 'eid', 'auid'])
        .drop_duplicates(['nr', 'eid'], keep='first')
        .copy()
    )

    summary_rows = []
    base_meta = df_raw.groupby('nr', sort=True)
    metrics = df_dedup.groupby('nr', sort=True)

    for nr, df_group in metrics:
        df_meta = base_meta.get_group(nr)

        # Deduplicate by srcid within this Nr group for cumulative journal metrics
        df_unique_journals = df_group.drop_duplicates(subset=['srcid'])

        journal_metric_count = df_group[best_pct_col].notna().sum()
        if journal_metric_count == 0:
            top_journal_value = '-'
        else:
            top_journal_value = round(float(df_group[top_journal_col].fillna(0).mean() * 100.0), 1)

        # Scholary Output should count all unique papers in this Nr group.
        n_total_all = int(df_group['eid'].nunique())

        # Top citation %: ignore papers with zero citations in percentile computation.
        df_cited = df_group[df_group['citations_nowindow'].fillna(0) > 0]
        n_total_cited = int(df_cited['eid'].nunique())
        n_top_cited = int(
            df_cited[
                (df_cited['fwci_4y'].fillna(0) > 0) &
                (df_cited['is_top10_fwci_4y'] == 1)
            ]['eid'].nunique()
        )
        top_cited_pct = round(n_top_cited / n_total_cited * 100.0, 1) if n_total_cited > 0 else 0.0

        summary_rows.append({
            'Scopus ID': join_unique(df_meta['auid'].astype('Int64').astype(str)),
            'Nr.': int(nr),
            'Last Name': join_unique_names(df_meta['last_name']),
            'First Name': join_unique_names(df_meta['first_name']),
            'First Year Publication': int(df_group['sort_year'].min()) if df_group['sort_year'].notna().any() else pd.NA,
            'Scholary Output': n_total_all,
            'Citations': int(round(df_group['citations_nowindow'].fillna(0).sum())),
            'Top citation percentiles (top 10 %)': top_cited_pct,
            'Top journal percentiles (Top 10%)': top_journal_value,
            'h-index': compute_h_index(df_group['citations_nowindow']),
            'International Collaboration': round(float(df_group['collaboration_level'].eq('INTERNATIONAL').mean() * 100.0), 1),
            'Cumulative SNIP (2024)': round(float(df_unique_journals['snip_2024'].fillna(0).sum()), 2),
            'Cumulative CiteScore (2024)': round(float(df_unique_journals['citescore_2024'].fillna(0).sum()), 1),
        })

    return pd.DataFrame(summary_rows, columns=SUMMARY_COLUMNS).sort_values('Nr.').reset_index(drop=True)


def build_raw_sheet(df_raw: pd.DataFrame) -> pd.DataFrame:
    ordered_columns = [
        'nr',
        'auid',
        'last_name',
        'first_name',
        'eid',
        'sort_year',
        'srcid',
        'source_title',
        'citations_nowindow',
        'fwci_4y',
        'fwci_4y_percentile',
        'is_top10_fwci_4y',
        'best_citescore_percentile',
        'is_top10_journal',
        'best_citescore_percentile_2024',
        'is_top10_journal_2024',
        'citescore_2024',
        'snip_2024',
        'collaboration_level',
    ]
    existing_columns = [column for column in ordered_columns if column in df_raw.columns]
    return df_raw[existing_columns].sort_values(['nr', 'auid', 'sort_year', 'eid']).reset_index(drop=True)


def write_workbook(summary_df: pd.DataFrame, raw_df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='summary', index=False)
        raw_df.to_excel(writer, sheet_name='raw_author_eid', index=False)

        ws_summary = writer.sheets['summary']
        ws_raw = writer.sheets['raw_author_eid']

        for worksheet in [ws_summary, ws_raw]:
            worksheet.freeze_panes = 'A2'

        column_widths = {
            'A': 24, 'B': 8, 'C': 18, 'D': 18, 'E': 22, 'F': 16, 'G': 12,
            'H': 28, 'I': 28, 'J': 10, 'K': 24, 'L': 20, 'M': 24,
        }
        for column, width in column_widths.items():
            ws_summary.column_dimensions[column].width = width

        raw_widths = {
            'A': 8, 'B': 16, 'C': 18, 'D': 18, 'E': 16, 'F': 10, 'G': 14,
            'H': 40, 'I': 16, 'J': 12, 'K': 18, 'L': 14, 'M': 22, 'N': 16,
            'O': 16, 'P': 12, 'Q': 24,
        }
        for column, width in raw_widths.items():
            ws_raw.column_dimensions[column].width = width


def main() -> None:
    args = parse_args()
    df_raw = load_parquet(args.input)
    summary_df = build_summary(df_raw.copy())
    raw_df = build_raw_sheet(df_raw.copy())
    write_workbook(summary_df, raw_df, Path(args.output))
    print(f'Wrote workbook: {args.output}')


if __name__ == '__main__':
    main()
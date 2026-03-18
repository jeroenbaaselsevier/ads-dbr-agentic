"""
Build null model for country pairs and compute per-country statistics.
Requires country_summary.csv from Databricks export.
"""

import pandas as pd
import numpy as np
from pathlib import Path

SRC = Path('tmp/coaffil')

# Load country summary and pairs
print("Loading data...")
country_summary = pd.read_csv(SRC / 'country_summary.csv')
country_pairs = pd.read_csv(SRC / 'country_pairs_overall.csv')

# ── Country summary: clean and prepare ──────────────────────────────────────
country_summary = country_summary.rename(columns={
    'country': 'country_code',
    'total_authors': 'total_authors_in_country',
    'authors_with_mca': 'authors_with_mca_in_country',
    'pct_authors_mca': 'pct_authors_mca_in_country',
})

print(f"Countries in summary: {len(country_summary)}")
print(f"Total MCA authors (sum): {country_summary['authors_with_mca_in_country'].sum():,}")

# ── Null model calculation ──────────────────────────────────────────────────
# For each pair (c1, c2):
# expected = (mca[c1] / total_mca) * (mca[c2] / total_mca) * N_total_events
# where N = total author-paper events

N_total_mca_events = country_pairs['n_author_paper_events'].sum()
total_mca_authors = country_summary['authors_with_mca_in_country'].sum()

print(f"Total MCA author-paper events: {N_total_mca_events:,}")
print(f"Total MCA authors (by country): {total_mca_authors:,}")

# Create lookup: country -> mca_authors
mca_by_country = dict(zip(
    country_summary['country_code'],
    country_summary['authors_with_mca_in_country']
))

# Add null model columns to pairs
def expected_frequency(row):
    c1_mca = mca_by_country.get(row['country1'], 0)
    c2_mca = mca_by_country.get(row['country2'], 0)
    if c1_mca == 0 or c2_mca == 0:
        return np.nan
    # Symmetric: (c1_mca / total) * (c2_mca / total) * N
    # But account for ordering: if c1 == c2, don't double-count
    if row['country1'] == row['country2']:
        # Same country: binomial, don't use this usually
        return np.nan
    # Different countries
    exp = (c1_mca / total_mca_authors) * (c2_mca / total_mca_authors) * N_total_mca_events
    return exp

country_pairs['expected_events'] = country_pairs.apply(expected_frequency, axis=1)
country_pairs['observed_expected_ratio'] = (
    country_pairs['n_author_paper_events'] / country_pairs['expected_events']
).round(3)

# Simple enrichment: log2(obs/exp)
country_pairs['log2_enrichment'] = np.log2(country_pairs['observed_expected_ratio'])

# Sort by enrichment
country_pairs_sorted = country_pairs[country_pairs['expected_events'].notna()].copy().sort_values(
    'observed_expected_ratio', ascending=False
)

# Display top and bottom
print("\n=== TOP 20 ENRICHED PAIRS (OVER-REPRESENTED) ===")
print(country_pairs_sorted[['country1', 'country2', 'n_author_paper_events', 'expected_events', 'observed_expected_ratio']].head(20).to_string())

print("\n=== BOTTOM 20 ENRICHED PAIRS (UNDER-REPRESENTED) ===")
print(country_pairs_sorted[['country1', 'country2', 'n_author_paper_events', 'expected_events', 'observed_expected_ratio']].tail(20).to_string())

# ── Per-country summary with top partners ────────────────────────────────────
# For each country, find top 5 partners by enrichment
print("\n=== BUILDING PER-COUNTRY PARTNER SUMMARY ===")

country_stats = []
for _, row in country_summary.iterrows():
    cc = row['country_code']
    
    # Find pairs involving this country
    involving_this = country_pairs[
        (country_pairs['country1'] == cc) | (country_pairs['country2'] == cc)
    ].copy()
    
    if len(involving_this) == 0:
        country_stats.append({
            'country_code': cc,
            'total_authors': row['total_authors_in_country'],
            'authors_mca': row['authors_with_mca_in_country'],
            'pct_mca': row['pct_authors_mca_in_country'],
            'top_5_partners': '',
            'top_5_enrichment': '',
        })
        continue
    
    # Sort by enrichment
    involving_this_sorted = involving_this[
        involving_this['expected_events'].notna()
    ].sort_values('observed_expected_ratio', ascending=False).head(5)
    
    partners = ', '.join([
        f"{r['country1'] if r['country1'] != cc else r['country2']}({r['observed_expected_ratio']:.1f}x)"
        for _, r in involving_this_sorted.iterrows()
    ])
    
    country_stats.append({
        'country_code': cc,
        'total_authors': row['total_authors_in_country'],
        'authors_mca': row['authors_with_mca_in_country'],
        'pct_mca': row['pct_authors_mca_in_country'],
        'top_5_partners': partners if partners else 'N/A',
    })

country_stats_df = pd.DataFrame(country_stats).sort_values('authors_mca', ascending=False)

print("\nTop 30 countries by MCA authors:")
print(country_stats_df[['country_code', 'total_authors', 'authors_mca', 'pct_mca', 'top_5_partners']].head(30).to_string())

# ── Export results ──────────────────────────────────────────────────────────
OUT = Path('output/coaffiliation')

country_pairs_sorted.to_csv(
    OUT / 'country_pairs_with_null_model.csv',
    index=False
)
print(f"\n✓ Exported: country_pairs_with_null_model.csv ({len(country_pairs_sorted)} rows)")

country_stats_df.to_csv(
    OUT / 'country_summary_with_partners.csv',
    index=False
)
print(f"✓ Exported: country_summary_with_partners.csv ({len(country_stats_df)} rows)")

country_summary.to_csv(
    OUT / 'country_summary_basic.csv',
    index=False
)
print(f"✓ Exported: country_summary_basic.csv ({len(country_summary)} rows)")

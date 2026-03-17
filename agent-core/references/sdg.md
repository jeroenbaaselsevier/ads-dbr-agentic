# SDG Table Reference — `snapshot_functions.sdg`

This document describes the SDG article-level mapping accessible via
`snapshot_functions.sdg`. It is referenced by the **analyst** agent.

The SDG dataset classifies Scopus publications against the 17 UN Sustainable
Development Goals using Elsevier's machine-learning classifier
(`scopus-art-sdg2023-flat-edc`). A paper can be assigned to multiple SDGs.

---

## API overview

```python
import snapshot_functions

# List available snapshot dates
snapshot_functions.sdg.list_snapshots()
# → ['2025-01-17', ..., '2026-03-09']  (63 snapshots as of 2026-03-12)

# Load latest snapshot (closest to today)
df_sdg = snapshot_functions.sdg.get_table()

# Load a specific date (falls back to closest)
df_sdg = snapshot_functions.sdg.get_table(snapshot='20260301')

# Add human-readable SDG labels
df_sdg = snapshot_functions.sdg.get_table(with_labels=True)

# SDG label lookup DataFrame independently
df_labels = snapshot_functions.sdg.get_labels()   # sdg_id (int), label (string)
```

---

## Table schema

| Column | Type | Description |
|---|---|---|
| `eid` | long | Numeric EID — matches `Eid` in ANI directly |
| `sdg` | int | SDG code (1–17) |
| `confidence` | float | Classifier confidence score |

> **Note:** The Python docstring says `confidence: double` but the actual Spark
> type is **float**. Use `.cast("double")` if downstream code requires it.

---

## Snapshot cadence

- **63 snapshots** available (as of 2026-03-12)
- Cadence: **~weekly** since 2025-01-17
- Latest snapshot: **2026-03-09**
- `get_table()` without a snapshot argument uses today's date and picks the
  closest available snapshot (printed to stdout).

---

## Data statistics (snapshot 2026-03-09)

| Metric | Value |
|---|---|
| Total rows | 31,052,072 |
| Distinct EIDs | 24,777,496 |
| Rows per EID | 1–17 (multiple SDGs per paper allowed) |

**Confidence score distribution:**

| Stat | Value |
|---|---|
| Min | 0.9501 |
| Max | 1.0 |
| Mean | 0.9989 |
| Median | 1.0 |

> All records have `confidence >= 0.9501`. This is a **threshold-filtered**
> dataset — low-confidence assignments are excluded before storage.
> No further confidence filtering is needed in most analyses.

**SDG distribution:**

| SDG | Label | Row count |
|---|---|---|
| 1 | No Poverty | 355,181 |
| 2 | Zero Hunger | 868,557 |
| 3 | Good Health and Well-Being | **12,810,511** |
| 4 | Quality Education | 923,469 |
| 5 | Gender Equality | 607,092 |
| 6 | Clean Water and Sanitation | 1,242,305 |
| 7 | Affordable and Clean Energy | 2,890,207 |
| 8 | Decent Work and Economic Growth | 1,154,508 |
| 9 | Industry, Innovation and Infrastructure | 2,422,797 |
| 10 | Reduced Inequalities | 995,270 |
| 11 | Sustainable Cities and Communities | 1,472,117 |
| 12 | Responsible Consumption and Production | 1,030,716 |
| 13 | Climate Action | 1,069,424 |
| 14 | Life Below Water | 665,494 |
| 15 | Life On Land | 860,126 |
| 16 | Peace, Justice and Strong Institutions | 966,658 |
| 17 | Partnerships for the Goals | 717,640 |

> SDG 3 (Good Health) dominates with 41% of all rows. SDG 1 (No Poverty) has
> the fewest citations.

**Multi-SDG distribution per paper:**

| SDGs per paper | Papers |
|---|---|
| 1 | 20,652,809 (83.5%) |
| 2 | 2,741,602 (11.1%) |
| 3 | 890,185 (3.6%) |
| 4 | 313,627 (1.3%) |
| 5+ | ~168K |

> 83.5% of SDG-classified papers are assigned to exactly one SDG.

---

## SDG labels

```python
df_labels = snapshot_functions.sdg.get_labels()
# Schema: sdg_id (int), label (string)
```

| sdg_id | label |
|---|---|
| 1 | No Poverty |
| 2 | Zero Hunger |
| 3 | Good Health and Well-Being |
| 4 | Quality Education |
| 5 | Gender Equality |
| 6 | Clean Water and Sanitation |
| 7 | Affordable and Clean Energy |
| 8 | Decent Work and Economic Growth |
| 9 | Industry, Innovation and Infrastructure |
| 10 | Reduced Inequalities |
| 11 | Sustainable Cities and Communities |
| 12 | Responsible Consumption and Production |
| 13 | Climate Action |
| 14 | Life Below Water |
| 15 | Life On Land |
| 16 | Peace, Justice and Strong Institutions |
| 17 | Partnerships for the Goals |

---

## Common patterns

### Basic: join SDG data onto ANI

```python
import snapshot_functions, column_functions
from pyspark.sql import functions as F

ani_stamp = '20260301'
df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())

# Load SDG table with labels embedded
df_sdg = snapshot_functions.sdg.get_table(with_labels=True)
# Columns: eid (long), sdg (int), confidence (float), label (string)

# Join — LEFT because not all papers are SDG-classified
df = df_ani.join(df_sdg, F.col('Eid') == F.col('eid'), 'left')
```

### Count SDG-classified publications per year

```python
df_sdg_raw = snapshot_functions.sdg.get_table()
df_labels  = snapshot_functions.sdg.get_labels()

df_result = (
    df_ani
    .join(df_sdg_raw, F.col('Eid') == F.col('eid'), 'inner')    # inner = SDG-only
    .join(df_labels, df_sdg_raw['sdg'] == df_labels['sdg_id'], 'left')
    .groupBy('sort_year', 'sdg', 'label')
    .count()
    .orderBy('sort_year', 'sdg')
)
```

### One-row-per-paper SDG view (pivot to array)

```python
df_sdg_array = (
    df_sdg_raw
    .groupBy('eid')
    .agg(F.collect_set('sdg').alias('sdg_codes'))
)

df_ani_sdg = df_ani.join(df_sdg_array, F.col('Eid') == F.col('eid'), 'left')
```

---

## Join key summary

| SDG column | ANI column | Cast needed? |
|---|---|---|
| `eid` (long) | `Eid` (long) | No — direct join |

---

## Coverage note

Not all ANI papers (even post-`nopp()`) have an SDG assignment. Only papers
that the classifier assigned with confidence ≥ 0.95 are present. Expect
roughly **22–25% of ANI papers** to have at least one SDG.

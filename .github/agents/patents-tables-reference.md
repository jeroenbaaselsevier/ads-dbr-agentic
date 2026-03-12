# Patents Tables Reference — `snapshot_functions.patents`

This document describes the two patent data sources accessible via
`snapshot_functions.patents`:

1. **`patents.metadata`** — full patent records (174M rows), parquet
2. **`patents.npl_citations_scopus`** — CSV mapping: patent → non-patent literature (NPL) → Scopus EID

It is referenced by the **analyst** agent.

---

## API overview

```python
import snapshot_functions

# ── metadata ─────────────────────────────────────────────────────────────────

snapshot_functions.patents.metadata.list_snapshots()
# → ['20230615', ..., '20260312']  (158 numbered snapshots + a 'Sample' folder)

df_meta = snapshot_functions.patents.metadata.get_table()          # latest
df_meta = snapshot_functions.patents.metadata.get_table(snapshot='20260301')

# ── NPL-Scopus CSV mapping ───────────────────────────────────────────────────

snapshot_functions.patents.npl_citations_scopus.list_snapshots()
# → ['20230721', ..., '20251024']  (15 snapshots — lags behind metadata)

df_npl = snapshot_functions.patents.npl_citations_scopus.get_table()

# ── convenience methods on the parent patents helper ────────────────────────

# Explode metadata.citation_nplcit → one row per (patent_id, sequence, citation_struct)
df_exploded = snapshot_functions.patents.explode_npl_from_metadata(snapshot='20260301')

# Full join: exploded NPL citations LEFT JOIN npl_citations_scopus on (patent_id, sequence)
df_joined = snapshot_functions.patents.join_npl_citations(snapshot='20260301')
# → patent_id, sequence, citation_nplcit (struct), eid (long | null)
```

---

## Table 1: `patents.metadata`

### Snapshot cadence

- **158 snapshots + `Sample` folder** (ignore `Sample`)
- Cadence: **~weekly** since 2023-06-15; bi-monthly since early 2024
- Latest: **2026-03-12**

> The `list_snapshots()` return includes `'Sample'` as it sorts lexicographically
> after numeric names. Pass explicit dates to `get_table()` to avoid it.

### Statistics (snapshot 2026-03-12)

| Metric | Value |
|---|---|
| Total rows | 174,810,506 |
| Distinct `patent_id` | 174,810,506 (one row per patent) |
| Patents with ≥1 NPL citation | 22,115,723 (12.7%) |
| Total NPL citation entries | 108,203,377 |

**Top patent offices by `country_ar`:**

| country_ar | Count |
|---|---|
| CN | 54,341,128 |
| JP | 28,985,512 |
| US | 22,094,330 |
| EP | 9,087,717 |
| KR | 8,508,786 |
| DE | 7,644,955 |
| WO | 6,132,315 |
| GB | 4,104,704 |
| FR | 3,328,873 |
| CA | 3,256,329 |

### Key columns

| Column | Type | Description |
|---|---|---|
| `patent_id` | string | Unique patent identifier (e.g. `"US6121195A"`, `"CN113234767A"`) |
| `country_ar` | string | Granting office / country code (ISO 2-letter, e.g. `"US"`, `"WO"`) |
| `country_pr` | string | Priority application country code |
| `patent_office` | string | Patent office name |
| `date_ar` | string | Grant / assignment date |
| `date_pr` | string | Priority application date |
| `date_changed` | string | Record change date |
| `appl_type` | string | Application type |
| `abstracts` | array\<struct\> | Multilingual abstracts: `{format, lang, abstract}` |
| `assignees` | array\<struct\> | Who owns the patent: nested `{date_changed, assignee[{seq, addressbook[{lang, orgname, orgname_standardized, type, role, address[{address_1, city, postcode, country}]}]}]}` |
| `applicant` | array\<struct\> | Who applied: `{addressbook[{name, orgname, country, city, ...}], app_type, country_nationality, country_residence, seq, n_addressbook}` |
| `agent` | array\<struct\> | Legal representative: same shape as `applicant` |
| `citation_nplcit` | array\<struct\> | Non-patent literature (NPL) citations — **see below** |
| `citation_nplcit_desc` | array\<struct\> | Alternative NPL description form: `{id, npl_type, text}` |
| `citation_nplcit_desc_list` | array\<struct\> | Another NPL form: `{p_num, nplcit[{num, text}]}` |
| `citation_fwdcit` | array\<struct\> | Forward patent citations: `{country, date, doc_number, kind, name}` |
| `citation_patcit` | array\<struct\> | Backward patent-to-patent citations |

### `citation_nplcit` struct fields

This is the array used by `explode_npl_from_metadata()`:

| Field | Type | Description |
|---|---|---|
| `nplcit_num` | string | Reference sequence number within the patent |
| `srep_phase` | string | Search report phase |
| `category` | string | Citation category code |
| `corresponding_docs_text` | string | Free text of corresponding docs |
| `scopus_url` | string | Scopus URL (if pre-linked by source data) |
| `text` | string | Raw bibliographic text of the NPL reference |

> For linking NPL citations to Scopus EIDs programmatically, use the
> `npl_citations_scopus` table + `join_npl_citations()` rather than parsing
> the `scopus_url` or `text` fields manually.

---

## Table 2: `patents.npl_citations_scopus`

### Snapshot cadence

- **15 snapshots** available (as of 2026-03-12)
- Cadence: **~bi-monthly** since 2023-07-21
- Latest: **2025-10-24** — lags behind `metadata` by ~5 months

> Always check `list_snapshots()` for the latest available NPL mapping.
> When calling `join_npl_citations()` without explicit snapshot args,
> each sub-table independently picks its closest snapshot.

### Statistics (snapshot 2025-10-24)

| Metric | Value |
|---|---|
| Total rows | 39,242,632 |
| Distinct `patent_id` in mapping | 6,666,552 |
| Distinct Scopus EIDs cited by patents | 6,316,106 |

### Schema

| Column | Type | Description |
|---|---|---|
| `eid` | long | Scopus EID of the cited paper |
| `patent_id` | string | Patent identifier (same format as `metadata.patent_id`) |
| `sequence` | int | Reference sequence number within the patent |

---

## Convenience methods

### `explode_npl_from_metadata(snapshot=None)`

Explodes `metadata.citation_nplcit` to one row per `(patent_id, sequence)`.

```python
df_npl_exploded = snapshot_functions.patents.explode_npl_from_metadata()
# Columns: patent_id (string), sequence (int), citation (struct)
```

The `sequence` is `coalesce(int(citation.nplcit_num), pos+1)` — i.e., uses
the embedded reference number if available, otherwise the array position.

### `join_npl_citations(snapshot=None, metadata_snapshot=None, npl_snapshot=None, how='left')`

Joins the exploded NPL entries to the Scopus EID mapping.

```python
df_joined = snapshot_functions.patents.join_npl_citations()
# Columns: patent_id, sequence, citation_nplcit (struct), eid (long | null)
```

**Statistics on latest snapshots:**

| Metric | Value |
|---|---|
| Total joined rows | 260,898,160 |
| Rows with Scopus EID match | 39,240,798 (~15%) |

> The total joined rows (260M) exceeds the raw NPL count (108M) because
> `metadata.patent_id` values can map to multiple rows in `npl_citations_scopus`
> (e.g. the same sequence in a document family). Expect fan-out.

---

## Common patterns

### Find Scopus papers cited by patents in a set

```python
import snapshot_functions, column_functions, dataframe_functions
from pyspark.sql import functions as F
import os

ani_stamp = '20260301'
cache = '/mnt/els/rads-projects/temporary_to_be_deleted/1d/my_project/cache'

# Patent papers citing a set of Scopus EIDs
df_target_eids = ...   # DataFrame with column 'Eid' (long)

df_npl = dataframe_functions.df_cached(
    snapshot_functions.patents.join_npl_citations(),
    os.path.join(cache, 'patent_npl_joined'),
    partitions=20,
)

df_cited_by_patent = df_target_eids.join(
    df_npl.select('eid', 'patent_id').dropna(subset=['eid']),
    F.col('Eid') == F.col('eid'),
    'inner',
)
```

### Count NPL citations per Scopus paper

```python
df_npl_map = snapshot_functions.patents.npl_citations_scopus.get_table()

df_patent_citation_counts = (
    df_npl_map
    .groupBy('eid')
    .agg(
        F.countDistinct('patent_id').alias('n_citing_patents'),
        F.count('*').alias('n_patent_citations'),
    )
)

df_ani = spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())
df_enriched = df_ani.join(df_patent_citation_counts, F.col('Eid') == F.col('eid'), 'left')
```

### Get patent metadata for a list of patent_ids

```python
df_meta = snapshot_functions.patents.metadata.get_table()

# Extract assignee org names from top-level
df_assignee = (
    df_meta
    .select('patent_id', 'country_ar', 'date_ar',
            F.explode('assignees').alias('ag'))
    .select('patent_id', 'country_ar', 'date_ar',
            F.explode('ag.assignee').alias('a'))
    .select('patent_id', 'country_ar', 'date_ar',
            F.explode('a.addressbook').alias('ab'))
    .select('patent_id', 'country_ar', 'date_ar',
            F.col('ab.orgname_standardized').alias('assignee_name'))
)
```

---

## Join key summary

| patents column | ANI column | Cast needed? |
|---|---|---|
| `npl_citations_scopus.eid` (long) | `Eid` (long) | No |
| `join_npl_citations().eid` (long) | `Eid` (long) | No |

---

## Caveats

- **`npl_citations_scopus` lags by ~5 months** behind `metadata`. Papers published
  after Oct 2025 will not yet have Scopus EID linkage in the mapping.
- **Only ~12.7% of patents reference non-patent literature.** Most patents cite
  only other patents.
- **Only ~15% of NPL citations match a Scopus EID.** Many NPL citations point
  to non-Scopus sources (books, web pages, conference proceedings not in Scopus).
- Patent metadata is **very** large (174M rows). Cache intermediate results with
  `dataframe_functions.df_cached()` and use `partitions=20` or higher.
- `date_ar`, `date_pr`, `date_changed` are stored as **strings** (no fixed format
  guaranteed). Cast explicitly when filtering by year.

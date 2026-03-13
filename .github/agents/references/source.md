# Source Profiles Reference — `snapshot_functions.source`

This document describes the Scopus Source Profiles dataset and the
`snapshot_functions.source` API. It is referenced by the **analyst** agent.

Source Profiles are the records behind the **Scopus Source Browse** page
(`scopus.com/sources`). They contain journal/book series metadata, CiteScore,
SNIP, SJR, ISSN lists, ASJC classifications, and publisher information for the
sources that Scopus formally lists and evaluates.

---

## Critical coverage caveat

**Not every `source.srcid` in ANI has an entry in the source profiles table.**

ANI contains documents from ~725,000 distinct source IDs. The source profiles
table contains only ~49,400 entries — roughly **6.8%** of ANI source IDs.

The 93.2% gap consists of:
- Small or recently-added journals not yet in the source browse
- Most individual book series titles (only curated series appear)
- Conference proceedings series that are indexed but not individually evaluated
- Datasets and other non-standard source types

This means **always use LEFT JOIN** when enriching ANI data with source profile
metadata, and do not assume a missing source means a data problem.

```python
df_enriched = (
    df_ani
    .filter(column_functions.nopp())
    .join(
        df_src.select('id', 'sourcetitle', 'sourceType', 'isActive', ...),
        F.col('source.srcid') == F.col('id'),
        'left'   # LEFT JOIN — most ANI srcids won't have a source profile
    )
)
```

---

## Snapshots

Source profiles are published **roughly monthly** (not daily like OrgDB).
Available snapshots go back to **2024-03-14**; as of March 2026 the latest real
snapshot is **20260214**.

The base path is `/mnt/els/edc/source-profiles-parsed-edc/` (YYYYMMDD folders).
The snapshot selector automatically picks the **closest available date** — if
you request today's date it will fall back to the most recent available snapshot.

Some non-date folders exist in the path (`SciVal`, `typecorrected`) — these are
ignored by the snapshot picker.

```python
import snapshot_functions

# List all available snapshots
snapshots = snapshot_functions.source.list_snapshots()  # sorted list of YYYYMMDD strings

# Load latest available (falls back to closest before today)
df_src = snapshot_functions.source.get_table()

# Load a specific snapshot date (falls back to closest before)
df_src = snapshot_functions.source.get_table(snapshot='20260214')
```

---

## API

### `snapshot_functions.source.get_table(snapshot=None, format='edc')`

| Parameter | Default | Description |
|---|---|---|
| `snapshot` | `None` (today) | Requested date as `'YYYYMMDD'` string or int |
| `format` | `'edc'` | `'edc'` = raw EDC schema; `'ops_etl'` = transformed/renamed schema |

The `'edc'` format returns the raw parquet exactly as stored.
The `'ops_etl'` format renames `id` → `srcid`, restructures metrics into a
nested per-year map, and reshapes publisher/classification fields.

For most analytical work, **use `'edc'` format** (raw) to avoid the overhead of
the ops_etl transformation.

### `snapshot_functions.source.list_snapshots()`

Returns a sorted list of all available snapshot folder names.

---

## Schema (EDC / raw format)

**49,361 rows** as of snapshot `20260214`.

### Identity

| Column | Type | Description |
|---|---|---|
| `id` | long | **Source ID — matches `source.srcid` in ANI** |
| `sourcetitle` | string | Full source title |
| `normalizedName` | string | Normalised/lowercase title for matching |
| `variantnames` | array\<string\> | Alternative titles / abbreviations |
| `sortKey` | string | Sort key for display ordering |
| `sourceType` | string | Source type code (see below) |
| `status` | string | Record status (e.g. `"new"`, `"changed"`) |
| `isActive` | string | `"true"` / `"false"` — whether the source is currently active |
| `nodisplay` | string | If set, source is hidden from display |

#### `sourceType` codes

| Code | Meaning | Count |
|---|---|---|
| `j` | Journal | 44,898 |
| `k` | Book series | 2,450 |
| `p` | Conference proceedings | 1,210 |
| `d` | Dataset / data repository | 803 |

> **Note:** `isActive` is a string (`"true"` / `"false"`), not a boolean.
> Filter active sources with `F.col('isActive') == 'true'`.

### ISSN / EISSN

| Column | Type | Description |
|---|---|---|
| `issn` | array\<struct\> | All ISSN values: `{type: string, value: string}` |

Common `type` values: `"print"`, `"electronic"`, `"linking"`.

```python
# Get print ISSN
F.filter(F.col('issn'), lambda x: x['type'] == 'print')[0]['value']
```

### Open access

| Column | Type | Description |
|---|---|---|
| `openaccessstatus` | string | `"YES"` / `"NO"` (9,032 YES out of 49,361) |

### Classification (ASJC)

| Column | Type | Description |
|---|---|---|
| `classification` | array\<struct\> | ASJC codes: `{code: string}`. The code is a string here (cast to int/long if joining with `static_data.asjc`) |

```python
# Get ASJC codes as array of strings
F.col('classification.code')

# Explode to rows
df_src.select('id', F.explode('classification.code').alias('asjc_code'))
```

### Publisher

| Column | Type | Description |
|---|---|---|
| `publisherName` | string | Publisher name |
| `mainPublisherName` | string | Parent/main publisher name |
| `publisherId` | string | Publisher identifier |
| `publisherAddress` | struct | `{city: string, country: string, country_code: string}` |

### Coverage periods

| Column | Type | Description |
|---|---|---|
| `coverage` | array\<struct\> | Coverage windows: `{covStart: int, covEnd: int, xfabAdded: boolean}` |

`covStart` / `covEnd` are integer years (e.g. `1996`, `2026`).
`xfabAdded = true` means the coverage was added by cross-database linking.

```python
# Get earliest and latest coverage year
df_src.select(
    'id',
    F.array_min(F.col('coverage.covStart')).alias('first_year'),
    F.array_max(F.col('coverage.covEnd')).alias('last_year'),
)
```

### Metrics (SNIP and SJR time series)

| Column | Type | Description |
|---|---|---|
| `metrics` | array\<struct\> | Per-year metric values: `{name: string, value: double, year: int}` |

`name` values: `"SNIP"`, `"SJR"`.

```python
# Get latest SJR
df_src.select(
    'id',
    F.filter(F.col('metrics'), lambda m: m['name'] == 'SJR').alias('sjr_metrics')
).select(
    'id',
    F.array_sort('sjr_metrics')[-1]['value'].alias('sjr_latest'),
    F.array_sort('sjr_metrics')[-1]['year'].alias('sjr_year'),
)
```

### CiteScore calculations (per year)

| Column | Type | Description |
|---|---|---|
| `calculations` | array\<struct\> | Per-year CiteScore data (one element per year) |

Each `calculations` element:

| Field | Type | Description |
|---|---|---|
| `year` | int | Calendar year |
| `publications` | long | Scholarly output in that year |
| `citations` | long | Total citations received |
| `citationsSce` | long | Citations from Scopus core content |
| `zeroCites` | long | Papers with zero citations |
| `zeroCitesSce` | long | Papers with zero citations (Scopus core) |
| `revPercent` | double | Percentage of review articles |
| `status` | string | Calculation status (e.g. `"Complete"`) |
| `csMetric` | struct | CiteScore metric (only present when calculated) |

`csMetric` struct fields:

| Field | Type | Description |
|---|---|---|
| `csCiteScore` | double | CiteScore value |
| `csCitationCount` | long | Citation count used for CiteScore |
| `csScholarlyOutput` | long | Scholarly output used for CiteScore |
| `csPercentCited` | int | Percentage of documents cited |
| `csSubjectCategory` | array\<struct\> | Per-ASJC ranking: `{asjc, csRank, csRankOutOf, csPercentile, csQuartile}` |

```python
# Get most recent CiteScore for all sources
df_cs = (
    df_src
    .select('id', 'sourcetitle', F.explode('calculations').alias('calc'))
    .filter(F.col('calc.csMetric').isNotNull())
    .select(
        'id', 'sourcetitle',
        F.col('calc.year').alias('year'),
        F.col('calc.csMetric.csCiteScore').alias('citescore'),
        F.col('calc.csMetric.csScholarlyOutput').alias('output'),
    )
    .orderBy(F.desc('year'))
    .groupBy('id', 'sourcetitle')
    .agg(F.first('citescore').alias('citescore'), F.first('year').alias('year'))
)
```

### Related sources

| Column | Type | Description |
|---|---|---|
| `relation` | array\<struct\> | Related source records: `{reltype: string, relstatus: string, sourceid: int}` |

### Database membership

| Column | Type | Description |
|---|---|---|
| `databases` | array\<struct\> | Index databases: `{databasecode: string, databaseurl: string, type: string}` |

All source profile records have `databasecode = "Scopusbase"` — this reflects
that the source profiles table represents the **Scopus source browse**, not
individual index collections.

---

## Common access patterns

### Load active journals only

```python
df_journals = (
    snapshot_functions.source.get_table()
    .filter(F.col('sourceType') == 'j')
    .filter(F.col('isActive') == 'true')
)
```

### Look up a journal by title

```python
(
    df_src
    .filter(F.lower(F.col('sourcetitle')).contains('nature'))
    .select('id', 'sourcetitle', 'sourceType', 'isActive',
            F.col('publisherName'), F.col('openaccessstatus'))
    .show(10, truncate=False)
)
```

### Join source title to ANI (LEFT JOIN — most will not match)

```python
df_src_slim = df_src.select(
    F.col('id').alias('srcid'),
    F.col('sourcetitle').alias('source_profile_title'),
    'sourceType', 'isActive', 'openaccessstatus',
    F.col('publisherName').alias('publisher'),
)

df_enriched = (
    df_ani
    .filter(column_functions.nopp())
    .select('Eid', 'sort_year', F.col('source.srcid').alias('srcid'), F.col('source.sourcetitle').alias('ani_sourcetitle'))
    .join(df_src_slim, 'srcid', 'left')
)
```

### Get per-source CiteScore and SNIP for a given year

```python
target_year = 2024

df_metrics = (
    df_src
    .select('id', 'sourcetitle',
            F.explode('calculations').alias('calc'),
            F.explode('metrics').alias('m'))
    ...
)

# Simpler approach — filter metrics array directly:
df_metrics = (
    df_src
    .select(
        'id', 'sourcetitle',
        F.filter('calculations', lambda c: c['year'] == target_year)[0].alias('calc'),
        F.filter('metrics', lambda m: (m['name'] == 'SNIP') & (m['year'] == target_year))[0]['value'].alias('snip'),
        F.filter('metrics', lambda m: (m['name'] == 'SJR') & (m['year'] == target_year))[0]['value'].alias('sjr'),
    )
    .select(
        'id', 'sourcetitle', 'snip', 'sjr',
        F.col('calc.csMetric.csCiteScore').alias('citescore'),
    )
)
```

---

## Joining to ANI

| Source field | ANI field | Notes |
|---|---|---|
| `id` (long) | `source.srcid` (long) | Direct match — no cast required |
| `sourcetitle` | `source.sourcetitle` | Both exist, but ANI title may differ from profile title |
| `classification.code` | `ASJC` | Same ASJC codes; source profiles classify the journal, ANI classifies the article |

> `source.srcid` in ANI can also be accessed as `source['srcid']` since `source`
> is a struct column. Use `F.col('source.srcid')` (dot notation) directly.

---

## Related tables and datasets

| Dataset | Notes |
|---|---|
| `scopus.ani_YYYYMMDD` | Links via `source.srcid` ↔ `id`; LEFT JOIN required |
| `static_data.asjc` | Decode `classification.code` (cast to int first) |
| `static_data.doctype` | Decode ANI `citation_type` codes |
| `snapshot_functions.scival.get_table('topic_prominence')` | SciVal topic scores; sources ↔ topics via ANI EID mapping |

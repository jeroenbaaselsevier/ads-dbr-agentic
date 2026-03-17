# APR Table Reference — Scopus Author Profile Records

This document describes the `scopus.apr_YYYYMMDD` table.
It is referenced by the **analyst** agent.

---

## Table naming and snapshots

```
scopus.apr_YYYYMMDD
```

Same snapshot cadence as ANI:
- Use the **1st-of-month** snapshot for reproducible analyses.
- As of March 2026 the table contains **~59.4M rows** (one row per author profile).

```python
apr_stamp = '20260301'
df_apr = spark.table(f'scopus.apr_{apr_stamp}')
```

---

## Primary key

| Column | Spark type | Description |
|---|---|---|
| `auid` | `long` (bigint) | Scopus Author ID — the primary key |

The `auid` in the APR table is the same identifier that appears in
`Au[*].auid` in the ANI table.

---

## Coverage warning: not all ANI `auid`s exist in APR

**Not every author occurrence in ANI has a profile in APR.** Reasons include:

- Very early publications (pre-profile era)
- Machine-generated or unresolved author records
- Authors whose profiles were merged/aliased elsewhere

**Always use LEFT JOIN** when enriching ANI-derived author records with APR data:

```python
df_ani_authors = (
    df_ani
    .filter(column_functions.nopp())
    .select(F.col('Eid'), F.explode('Au').alias('au'))
    .select('Eid', F.col('au.auid').alias('auid'), F.col('au.surname_pn'), F.col('au.given_name_pn'))
    .filter(F.col('auid').isNotNull())
)

df_enriched = df_ani_authors.join(df_apr, 'auid', 'left')
# authors without APR profiles will have null in all df_apr columns
```

### Backfill strategies when APR data is missing

| What you need | Backfill source |
|---|---|
| Name (preferred) | `Au.surname_pn`, `Au.given_name_pn`, `Au.indexed_name_pn` from ANI |
| ORCID | `Au.orcid` from ANI (same field, sometimes populated there too) |
| Current country | `Af.affiliation_country` from ANI (affiliation at time of publication) |
| Current affiliation name | `Af.affiliation_organization` from ANI |
| Subject area | `ASJC` from ANI (document level) |

The `coalesce()` pattern is useful for filling from APR first, falling back to ANI:

```python
F.coalesce(F.col('apr.orcid'), F.col('au.orcid')).alias('orcid')
```

---

## Column reference (30 columns)

### Identity

| Column | Type | Description |
|---|---|---|
| `auid` | long | Scopus Author ID (PK) |
| `type` | string | Record type, typically `"author"` |
| `alias_status` | string | If non-null, this profile is an alias pointing to another |
| `suppress` | string | `"true"` / `"false"` as a **string** (not boolean) — suppressed profiles should be excluded |
| `history` | array\<long\> | Previous `auid` values that were merged into this profile |

> **Note:** `suppress` is stored as a string, not a boolean. Filter with
> `F.col('suppress') != 'true'` or `F.col('suppress') == 'false'`.

### Preferred names

| Column | Type | Description |
|---|---|---|
| `surname_pn` | string | Preferred surname |
| `given_name_pn` | string | Preferred given name |
| `initials_pn` | string | Preferred initials |
| `indexed_name_pn` | string | Preferred indexed name (e.g. `"Kawamata R."`) |
| `name_variants` | array\<struct\> | All observed name variants: `{initals, indexed_name, surname, given_name}` |

> **Note:** The `name_variants` struct has a typo in the field name: `initals` (single 'i'), not `initials`.

### Contact

| Column | Type | Description |
|---|---|---|
| `e_address` | string | Email address |
| `e_address_type` | string | Type of e-address (e.g. `"email"`) |
| `orcid` | string | ORCID identifier |
| `orcid_matching_type` | string | How ORCID was matched (e.g. `"self-asserted"`) |

### Current affiliations

| Column | Type | Description |
|---|---|---|
| `current_affiliations` | array\<long\> | List of current `afid`s (institution / dept level) |
| `current_affiliations_parent` | array\<long\> | Parent `afid`s of current affiliations |
| `n_affiliation_current` | smallint | Count of current affiliations |
| `affiliation_current_full` | array\<struct\> | Full details for each current affiliation (see below) |

#### `affiliation_current_full` struct fields

| Field | Type | Description |
|---|---|---|
| `afid` | long | Scopus Affiliation ID |
| `affiliation_id` | long | May be department-level (child of `afid`) |
| `affiliation_id_parent` | long | Parent institution `afid` |
| `afdispname` | string | Display name (e.g. `"The University of Tokyo, Department of Astronomy"`) |
| `preferred_name` | string | Preferred short name |
| `sort_name` | string | Sortable name |
| `address_part` | string | Street address |
| `city` | string | City |
| `state` | string | State / province |
| `postal_code` | string | Postal code |
| `country` | string | Country name |
| `country_tag` | string | ISO 3-letter country code (e.g. `"jpn"`) |
| `relationship` | string | `"author"` (direct) or `"derived"` (parent institution inferred) |
| `type_afid` | string | `"dept"` for department-level, `"parent"` for institution-level |

> The `relationship` field distinguishes directly-assigned affiliations (`"author"`)
> from parent institutions that were derived from a department assignment (`"derived"`).
> Filter to `relationship == "author"` if you want only the directly-stated affiliations.

### Subject areas

| Column | Type | Description |
|---|---|---|
| `ASJC` | array\<string\> | ASJC codes the author has published in |
| `ASJC_frequency_I` | array\<int\> | Publication count per ASJC code (parallel array to `ASJC`) |
| `SUBJABBR` | array\<string\> | Subject area abbreviations (e.g. `"PHYS"`, `"EART"`) |
| `SUBJABBR_frequency_I` | array\<int\> | Publication count per subject abbreviation (parallel to `SUBJABBR`) |

The `ASJC` and `ASJC_frequency_I` arrays are parallel — element `i` of `ASJC_frequency_I`
is the count for `ASJC[i]`. Use `arrays_zip` to work with them together:

```python
df_apr.select(
    'auid',
    F.arrays_zip('ASJC', 'ASJC_frequency_I').alias('asjc_freq')
).select(
    'auid',
    F.explode('asjc_freq').alias('af')
).select(
    'auid',
    F.col('af.ASJC').alias('asjc_code'),
    F.col('af.ASJC_frequency_I').alias('pub_count'),
)
```

### Manual curation

| Column | Type | Description |
|---|---|---|
| `manual_curation` | struct | `{curated: boolean, curtype: string, source: string, timestamp: timestamp}` |

### Timestamps & metadata

| Column | Type | Description |
|---|---|---|
| `datetime_max` | string | Date of most recent publication (`"YYYYMMDD"`) |
| `timestamp_date` | string | Profile last-updated date (`"YYYYMMDD"`) |
| `fname` | string | Internal filename reference |
| `corrupt_xml_file_B` | boolean | True if source XML was corrupt |
| `xmlsize` | long | Size of source XML in bytes |

---

## Common access patterns

### Load and filter suppressed profiles

```python
df_apr = (
    spark.table(f'scopus.apr_{apr_stamp}')
    .filter(F.col('suppress') == 'false')
    .filter(F.col('alias_status').isNull())  # exclude aliases
)
```

### Get author's current country (from APR, fall back to ANI)

```python
# From APR: take the first "author"-relationship country
apr_country = F.array_join(
    F.array_distinct(
        F.filter(
            F.col('affiliation_current_full.country'),
            lambda x: x.isNotNull()
        )
    ),
    '; '
)
```

### Enrich paper-level author list with APR profiles

```python
df_authors = (
    df_ani
    .filter(column_functions.nopp())
    .select('Eid', 'sort_year', F.explode('Au').alias('au'))
    .select(
        'Eid', 'sort_year',
        F.col('au.auid').alias('auid'),
        F.col('au.Authorseq').alias('authorseq'),
        F.col('au.orcid').alias('ani_orcid'),
    )
    .filter(F.col('auid').isNotNull())
    .join(
        df_apr.select('auid', 'surname_pn', 'given_name_pn', 'orcid',
                      'current_affiliations_parent', 'affiliation_current_full'),
        'auid',
        'left'  # LEFT JOIN — some auids won't have APR profiles
    )
    .withColumn('orcid', F.coalesce(F.col('orcid'), F.col('ani_orcid')))
)
```

### Find the primary subject area of each author

```python
df_main_asjc = (
    df_apr
    .select('auid', F.arrays_zip('ASJC', 'ASJC_frequency_I').alias('af'))
    .select('auid', F.explode('af').alias('p'))
    .select('auid', F.col('p.ASJC').alias('asjc'), F.col('p.ASJC_frequency_I').alias('freq'))
    .orderBy(F.desc('freq'))
    .groupBy('auid')
    .agg(F.first('asjc').alias('primary_asjc'))
)
```

---

## Relationship to other tables

| Table | Join key | Notes |
|---|---|---|
| `scopus.ani_*` | `auid` ↔ `Au[*].auid` | Always LEFT JOIN — not all ANI auids exist in APR |
| OrgDB / orgdb_functions | `afid` ↔ `current_affiliations` | Map APR current affiliation to institution hierarchy |
| `static_data.asjc` | `ASJC[*]` ↔ `code` | Decode ASJC codes to subject names |

# OrgDB Reference — Organisation Database (`orgdb_support`)

This document describes the OrgDB tables in the `orgdb_support` database and the
`orgdb_functions` library module. It is referenced by the **analyst** agent.

OrgDB is Scopus's curated organisation hierarchy. It maps raw affiliation strings
(via `afid`) to named institutions and their parent organisations, enabling
institution-level aggregation of publication data.

---

## Snapshots

OrgDB is updated **daily**. Unlike ANI, there is no special 1st-of-month rule —
snapshots are available for nearly every day going back to 2022.

```python
import orgdb_functions

orgdb_date = orgdb_functions.get_last_orgdb_date()   # e.g. '20260312'
all_dates  = orgdb_functions.get_all_orgdb_dates()   # sorted list of all available dates
```

`get_last_orgdb_date()` returns the lexicographically maximum date that has
**both** a `hierarchy_*` and a `documentcount_*` snapshot — i.e. a fully
consistent pair.

---

## Three core tables

| Table | Rows (2026-03-12) | Description |
|---|---|---|
| `orgdb_support.orgdb_YYYYMMDD` | 262,377 | Master org metadata (names, types, countries, external IDs) |
| `orgdb_support.hierarchy_YYYYMMDD` | 597,805 | Parent–child relationships and hierarchy paths |
| `orgdb_support.documentcount_YYYYMMDD` | 103,759 | Pre-computed publication counts per org per year |

All three share `org_id` (string) as the join key.

---

## Table 1: `orgdb_support.orgdb_YYYYMMDD` — Organisation metadata

51 columns, all strings unless noted.

### Identity & status

| Column | Description |
|---|---|
| `org_id` | **Primary key** — Scopus Affiliation ID as a string (matches `afid` in ANI/APR) |
| `orgname` | Raw organisation name (may contain HTML entities — use `udf_unescape()`) |
| `orgvisibility` | `"true"` / `"false"` string — only `"true"` orgs are shown to end users |
| `orglevel` | See level taxonomy below. Filter out `"Skeletal"` in almost all use cases |
| `orgtype` | Organisation type code (see types below) |
| `status` | Typically `"orgdb"` |
| `val` | Validity flag — typically `"valid"` |
| `orgprimary` | `"true"` if this is a primary record |
| `standalone` | `"true"` if this org has no parent relationship |
| `generic` | `"true"` if this is a generic/catch-all entry |

### Contact & location

| Column | Description |
|---|---|
| `city`, `state`, `country` | Human-readable location |
| `twolettercountry` | ISO 2-letter country code (e.g. `"us"`) |
| `threelettercountry` | ISO 3-letter country code (e.g. `"USA"`) — **use this for country matching** |
| `postalcode` | Postal code |
| `street`, `subcity`, `substate` | Address detail |
| `orgurl` | Official website URL |
| `orgdomain` | Web domain (e.g. `"marinescience.ucdavis.edu"`) |
| `orgemail` | Contact email |
| `orglocation` | URL to location page |
| `orghistory` | URL to history page |

### External identifiers

| Column | Description |
|---|---|
| `ROR` | Research Organization Registry ID |
| `fundingBodyID` | Funder identifier |
| `IPEDS` | US IPEDS institution code |
| `QS_Core_Id` | QS World Rankings ID |
| `SciVal` | SciVal institution ID |
| `THE` | Times Higher Education institution ID |
| `ECR` | ECR (Elsevier) institution ID |
| `ECRConsortium` | ECR consortium ID |
| `ParityInternal` | Internal parity/dedup ID |
| `OC_domain`, `OC_edition`, `OC_source`, `OC_tier`, `OC_value` | Open Citations metadata |

### Hierarchy shortcuts

| Column | Description |
|---|---|
| `superorgid` | Direct parent `org_id` (shortcut — also encoded in hierarchy table) |
| `reltype_superorgid` | Relationship type to superorg (e.g. `"po-60014439"`) |
| `superorgid_datacombine` | Full parent description string |

### OrgDB taxonomy

**`orglevel`** — determines how skeletal/stub entries are treated:

| Level | Count | Meaning |
|---|---|---|
| `Skeletal` | 122,131 | Placeholder/stub — **always filter out** with `orglevel != 'Skeletal'` |
| `Primary` | 98,186 | Known institution with full metadata |
| `Standard` | 36,222 | Recognised institution |
| `Subprimary` | 5,272 | Sub-unit with its own identity |
| `System` | 565 | Multi-campus university system |

**`orgtype`** — most common codes:

| Code | Meaning | Count |
|---|---|---|
| `comp` | Company / industry | 59,030 |
| `resi` | Research institute | 27,837 |
| `univ` | University | 21,441 |
| `hosp` | Hospital | 15,189 |
| `coll` | College | 8,210 |
| `ngov` | Non-governmental org | 4,751 |
| `govt` | Government body | 4,004 |
| `meds` | Medical school | 2,872 |
| `ddep` | Department (within a larger org) | 1,038 |
| `milo` | Military organisation | 270 |
| `museum`, `library`, `lawf`, `poli` | Specialist types | \<200 each |

Multiple types can be pipe-concatenated (e.g. `"hosp|meds"`).

---

## Table 2: `orgdb_support.hierarchy_YYYYMMDD` — Parent–child relationships

12 columns, all strings.

| Column | Description |
|---|---|
| `org_id` | Child organisation ID |
| `reltype` | Relationship type (see below). `NULL` = standalone root org |
| `parent_orgid` | Immediate parent `org_id` |
| `toplevel_orgid` | Top of the attribution chain (e.g. the university for its departments) |
| `toplevel_orgvisibility` | Whether the top-level org is visible |
| `final_attribution` | `"include"` or `"exclude"` — whether to attribute publications here |
| `level_count` | Depth of the hierarchy path (as string) |
| `level_orgids` | Pipe-separated list of `org_id`s from root to leaf |
| `isTopLevelAttributable` | `"yes"` if publications at this org attribute to `toplevel_orgid` |
| `isTopLevelNonAttributable` | `"yes"` if this cascade also holds for non-attributable relationships |
| `source`, `target` | Scope of the record (typically `"ani"`, `"all"`) |

### Relationship types

| Code | Label | Count | In default set? |
|---|---|---|---|
| `NULL` | standalone root | 262,377 | yes (treated as root) |
| `po` | part_of | 299,508 | **yes** |
| `jt` | jointlyOwned_by | 16,013 | **yes** |
| `rn` | renamed_as | 7,246 | **yes** |
| `mg` | merged_with | 6,389 | **yes** |
| `at` | affiliated_to | 2,227 | no |
| `so` | spinoff_from | 1,871 | no |
| `aw` | affiliated_with | 1,063 | no |
| `aa` | affiliated_attributable | 498 | **yes** |
| `sp` | sponsored_by | 448 | **yes** |
| `op` | operated_by | 88 | no |
| `mb` | member_of | 67 | no |
| `st` | splitup_from | 10 | no |

**Default attributable set** = `['po', 'jt', 'sp', 'aa', 'mg', 'rn']`.
Use `orgdb_functions.get_default_attributable_relationships()`.
This is the standard set for institutional publication attribution.

---

## Table 3: `orgdb_support.documentcount_YYYYMMDD` — Publication counts

| Column | Description |
|---|---|
| `org_id` | Organisation ID |
| `2016` … `2025` | Published documents attributed to this org per calendar year |
| `before_2016` | Cumulative count before 2016 |
| `after_2025` | Count after 2025 |
| `whole_institution` | Total attributed via the full hierarchy (including children) |
| `affiliation_only` | Count where this org is directly mentioned in the affiliation — used as a proxy for output size in `get_df_hierarchy_selected()` |

---

## `orgdb_functions` API

```python
import sys
sys.path.append('/Workspace/rads/library/')
import orgdb_functions
```

### Snapshot discovery

```python
orgdb_date = orgdb_functions.get_last_orgdb_date()
all_dates  = orgdb_functions.get_all_orgdb_dates()
```

### Relationship lists

```python
orgdb_functions.get_default_attributable_relationships()
# → ['po', 'jt', 'sp', 'aa', 'mg', 'rn']

orgdb_functions.get_all_relationships()
# → ['aa','at','aw','jt','mb','mg','op','po','rn','so','sp','st']

orgdb_functions.get_relationship_labels()
# → {'po': 'part_of', 'mg': 'merged_with', ...}
```

### `get_df_hierarchy_selected(orgdb_date, relationships)`

Returns a denormalised hierarchy DataFrame ready for recursive joining.
Combines `orgdb_*`, `hierarchy_*`, and `documentcount_*` for the given date.
Filters out `Skeletal` orgs and non-attributable relationship types.

**Output columns:** `org_id`, `orgtype`, `relationship`, `parent_org_id`,
`visible`, `org_name`, `output` (= `affiliation_only` doc count)

```python
df_hierarchy = orgdb_functions.get_df_hierarchy_selected(
    orgdb_date,
    orgdb_functions.get_default_attributable_relationships()
)
```

### `get_df_generated_mapping_cached(file, orgdb_date, df_institutions, relationships)`

The primary function for mapping a list of institutions (with known `afid`s) to
their complete OrgDB hierarchy. **Cached** — writes a parquet file on first run
and reads it back on subsequent calls.

**`df_institutions` must have these columns:**
- `root_org_id` — the starting `org_id` (typically from ANI `afid` or APR `current_affiliations`)
- `institution_id` — a user-supplied integer key for the institution
- `institution_name` — a label for the institution

**Output columns:** `institution_id`, `institution_name`, `org_id`, `org_name`,
`orgtype`, `output`, `relationship`, `parent_org_id`, `recursive_pass`

```python
import os

df_institutions = spark.createDataFrame([
    (1, 'MIT', '60009982'),
    (2, 'Harvard', '60021226'),
], ['institution_id', 'institution_name', 'root_org_id'])

df_mapping = orgdb_functions.get_df_generated_mapping_cached(
    file_mapping=os.path.join(cache_folder, 'orgdb_mapping'),
    orgdb_date=orgdb_date,
    df_institutions=df_institutions,
)
```

The mapping walks the hierarchy **recursively** (up to 10 levels) — departments
are resolved back to their parent institutions so publication counts roll up
correctly.

### `get_df_hierarchy_formatted(df_orgdb, df_hierarchy)`

Produces a clean export-ready table: `afid`, `name`, `country`, `main_afid`,
`main_name`. Filters to `isTopLevelAttributable = "yes"` and `final_attribution = "include"`.
Useful when delivering an institution list to clients.

### `get_mapping_file_hash(array_institutions, orgdb_date, relationships)`

Returns a 32-char MD5 hex hash of the input parameters. Use as a stable cache
filename suffix when the institution list changes between runs.

```python
hash_str = orgdb_functions.get_mapping_file_hash(
    [[1, 'MIT', '60009982']],
    orgdb_date
)
cache_path = os.path.join(cache_folder, f'orgdb_mapping_{hash_str}')
```

---

## Common patterns

### Map ANI affiliations to top-level institutions

```python
from pyspark.sql import functions as F
import orgdb_functions, column_functions

orgdb_date = orgdb_functions.get_last_orgdb_date()

# Step 1: get all distinct afids from core ANI content
df_afids = (
    df_ani
    .filter(column_functions.nopp())
    .select(F.explode('Af.affiliation_ids').alias('afid_array'))
    .select(F.explode('afid_array').alias('afid'))
    .filter(F.col('afid').isNotNull())
    .distinct()
    .withColumn('afid_str', F.col('afid').cast('string'))
)

# Step 2: join to hierarchy to find top-level org
df_hierarchy = orgdb_functions.get_df_hierarchy_selected(
    orgdb_date,
    orgdb_functions.get_default_attributable_relationships(),
)

df_afid_to_toplevel = (
    df_afids
    .join(
        spark.table(f'orgdb_support.hierarchy_{orgdb_date}')
        .filter(F.col('isTopLevelAttributable') == 'yes')
        .filter(F.col('final_attribution') == 'include')
        .select(F.col('org_id').alias('afid_str'), 'toplevel_orgid'),
        'afid_str',
        'left'
    )
)
```

### Look up an institution by name

```python
(
    spark.table(f'orgdb_support.orgdb_{orgdb_date}')
    .filter(F.lower(F.col('orgname')).contains('leiden'))
    .filter(F.col('orglevel') != 'Skeletal')
    .filter(F.col('orgvisibility') == 'true')
    .select('org_id', 'orgname', 'orgtype', 'country', 'ROR')
    .show(10, truncate=False)
)
```

### Get top institutions by output size

```python
(
    spark.table(f'orgdb_support.documentcount_{orgdb_date}')
    .join(
        spark.table(f'orgdb_support.orgdb_{orgdb_date}')
        .filter(F.col('orgvisibility') == 'true')
        .filter(F.col('orglevel') != 'Skeletal')
        .select('org_id', 'orgname', 'orgtype', 'threelettercountry'),
        'org_id'
    )
    .orderBy(F.desc('whole_institution'))
    .select('org_id', 'orgname', 'orgtype', 'threelettercountry', 'whole_institution')
    .show(20, truncate=False)
)
```

---

## Joining OrgDB to ANI and APR

| ANI/APR field | OrgDB join key | Notes |
|---|---|---|
| `Af[*].afid` | `orgdb.org_id` | Direct match; afid in ANI is a long, org_id in OrgDB is a string — cast with `.cast('string')` |
| `Af[*].affiliation_ids[*]` | `orgdb.org_id` | Array of all IDs for an affiliation entry (dept + parent) |
| `APR.current_affiliations[*]` | `orgdb.org_id` | Same cast required |
| `APR.current_affiliations_parent[*]` | `orgdb.org_id` | Top-level parent afids directly from APR |

> **Type mismatch:** `afid` in ANI/APR is a `long`; `org_id` in OrgDB tables is a
> `string`. Always cast before joining: `F.col('afid').cast('string')`.

---

## Related tables and datasets

| Dataset | Notes |
|---|---|
| `scopus.ani_YYYYMMDD` | Links via `Af[*].afid` → `org_id` |
| `scopus.apr_YYYYMMDD` | Links via `current_affiliations[*]` → `org_id` |
| `snapshot_functions.scival.get_table('topic_eid')` | SciVal topics; institutions ↔ topics via ANI |

# ANI Table Reference — Scopus Article-level November Index

This document describes the `scopus.ani_YYYYMMDD` table, the primary data source
for bibliometric analyses. It is referenced by the **analyst** agent.

---

## Table naming and snapshots

```
scopus.ani_YYYYMMDD
```

- A new snapshot is generated **daily** (e.g. `ani_20260311`, `ani_20260312`).
- Only the **1st-of-month** snapshots have long retention (~1 year, then moves
  to S3 Glacier). Daily snapshots are deleted after ~2 weeks.
- **Always use the 1st-of-month snapshot** for reproducible analyses:
  ```python
  ani_stamp = '20260301'
  df_ani = spark.table(f'scopus.ani_{ani_stamp}')
  ```
- As of March 2026 the table contains **~109M rows**.

---

## Primary key

| Column | Spark type | Description |
|---|---|---|
| `Eid` | `long` (bigint) | Numeric Scopus document identifier. This is the **primary key**. |

The full EID string has the format `2-s2.0-NNNNNNNNNN` where the numeric part is
zero-padded to 10 digits. Use `column_functions` helpers to convert:

```python
column_functions.long_eid_to_eidstr(F.col('Eid'))   # long → "2-s2.0-0034250131469"
column_functions.eid_to_long(F.col('eid_string'))     # "2-s2.0-..." → long
column_functions.long_eid_to_keystr(F.col('Eid'))    # long → "0034250131"  (10-char padded)
```

> **Note:** The column is named `Eid` (capital E) in the raw table, but many
> cached DataFrames and library functions use the lowercase alias `eid` via
> `.select(F.col('Eid').alias('eid'))` or `.withColumnRenamed('Eid', 'eid')`.

---

## Column reference (74 columns)

### Identifiers

| Column | Type | Description |
|---|---|---|
| `Eid` | long | Scopus document ID (PK) |
| `PUI` | long | Publishing Unit Identifier |
| `PII` | string | Publisher Item Identifier (Elsevier) |
| `doi` | string | DOI (not normalized — use `column_functions.normalize_doi()`) |
| `pmid` | string | PubMed ID |
| `sgr` | string | Scopus Group ID |
| `issn` | string | ISSN of the source |
| `puilist` | array\<long\> | List of related PUIs |
| `fname` | string | Internal filename reference |

### Dates & sorting

| Column | Type | Description |
|---|---|---|
| `sort_year` | int | **Primary year** for analysis (publication/cover year) |
| `pub_year` | int | Publication year (may differ from sort_year) |
| `sort_yyyymm` | string | Year-month for sorting (e.g. `"202301"`) |
| `datesort` | string | Full sort date `"YYYYMMDD"` |
| `date_sort_day` | string | Day component of sort date |
| `date_sort_month` | string | Month component of sort date |
| `date_month` | string | Publication month |
| `date_text` | string | Free-text date |
| `date_created_history` | string | Record creation date |
| `item_online` | string | Online publication date |
| `orig_load_date` | string | Original load date into Scopus |
| `timestamp` | timestamp | Record last-modified timestamp |
| `timestamp_str` | string | String version of timestamp |
| `timestamp_syndication` | timestamp | Syndication timestamp |

### Title & abstract

| Column | Type | Description |
|---|---|---|
| `citation_title` | array\<struct\> | Titles with language tags. Each element: `{title, lang, original}`. Use `column_functions.get_ani_title_col()` to extract preferred title. |
| `abstract` | string | Abstract text |
| `abstract_lang` | string | Language of abstract |
| `abstract_original` | string | `"y"` if abstract is in original language |

### Document classification

| Column | Type | Description |
|---|---|---|
| `citation_type` | string | Document type code (e.g. `"ar"` = article, `"re"` = review). See `static_data.doctype`. |
| `ASJC` | array\<string\> | All-Science Journal Classification codes |
| `subjareas` | array\<string\> | Subject area abbreviations |
| `n_subjareas` | short | Count of subject areas |
| `dbcollections` | array\<string\> | Index collections: `SCOPUS`, `MEDL`, `ARCSPR`, `Scopusbase`, etc. **Critical for `nopp()` filter.** |
| `ItemStage` | string | Processing stage (e.g. `"S300"`) |
| `item_match_type` | string | Match type (e.g. `"core"`) |
| `online_status` | string | Online publication status |
| `openaccess` | string | Open access flag |
| `SciencedirectScore` | string | ScienceDirect relevance score |

### Citations

| Column | Type | Description |
|---|---|---|
| `citations` | array\<long\> | **EIDs of documents cited by this paper** (may contain duplicates). These are numeric EIDs (long), not string EIDs. |
| `n_citations` | long | Count of citation references |
| `n_citations_corrupt_eid` | short | Count of corrupt/unparseable citation EIDs |

> **Important:** `citations` contains the references **made by** this document
> (outgoing citations), not the count of **times cited**. To find papers that
> cite a given EID, you must explode the `citations` array across all documents
> and filter for the target.

### Authors (`Au`, `Au_cors`, `Au_Af`)

| Column | Type | Description |
|---|---|---|
| `Au` | array\<struct\> | Author list. Each element: `{Authorseq, auid, author_type, collaboration, degrees, e_address, e_address_type, given_name, given_name_pn, indexed_name, indexed_name_pn, initials, initials_pn, instance_id, members, nametext, orcid, suffix, surname, surname_pn}` |
| `Au_cors` | array\<struct\> | Corresponding authors (same struct as `Au`) |
| `Au_Af` | array\<struct\> | Author↔Affiliation mapping: `{Authorseq, affiliation_seq, validity_B}` |
| `Au_unique_IN` | array\<string\> | Unique indexed names of authors |
| `n_Au` | short | Author count |
| `n_Au_unique_IN` | short | Unique author name count |

Key author fields:
- `auid` — Scopus Author ID (long)
- `orcid` — ORCID identifier
- `given_name`, `surname` — name parts
- `indexed_name` — display name (e.g. `"Fanardzhyan V.V."`)
- `Authorseq` — position in author list (1-based)
- `collaboration` — true if this is a group/consortium author
- `members` — array of group member structs (for collaborations)

### Affiliations (`Af`)

| Column | Type | Description |
|---|---|---|
| `Af` | array\<struct\> | Affiliation list. Each element: `{affiliation_address_part, affiliation_city, affiliation_city_group, affiliation_country, affiliation_ids, affiliation_organization, affiliation_organization_count, affiliation_postal_code, affiliation_state, affiliation_tag_country, affiliation_text, afid, dptid}` |
| `Af_valid_B` | array\<boolean\> | Validity flags per affiliation |
| `n_Af` | short | Affiliation count |

Key affiliation fields:
- `afid` — Scopus Affiliation ID (long)
- `dptid` — Department ID (long)
- `affiliation_ids` — array of all affiliation IDs for this entry
- `affiliation_country` / `affiliation_tag_country` — country (may differ)
- `affiliation_organization` — array of organization name parts
- `affiliation_city`, `affiliation_state`, `affiliation_postal_code` — location

### Source (journal/conference)

| Column | Type | Description |
|---|---|---|
| `source` | struct | Source metadata. Fields: `{article_number, conferenceinfo, country, date_day, date_month, date_text, date_year, e_address, e_address_type, firstpage, isbn_electronic, isbn_other, isbn_print, issn_electronic, issn_other, issn_print, lastpage, pagecnt, pages, publishername, sourcetitle, sourcetitle_abbrev, srcid, type, website}` |

Key source fields:
- `source.srcid` — Source ID (long). Use this to filter by journal.
- `source.sourcetitle` — Full journal/conference name
- `source.sourcetitle_abbrev` — Abbreviated title
- `source.publishername` — Publisher name
- `source.type` — Source type (e.g. `"j"` = journal, `"b"` = book, `"p"` = conference proceeding)
- `source.conferenceinfo` — struct with `{enddate, location, name, startdate}`
- `source.issn_print`, `source.issn_electronic` — arrays of ISSNs
- `source.isbn_print`, `source.isbn_electronic` — arrays of ISBNs

### Pages & volume

| Column | Type | Description |
|---|---|---|
| `firstpage` | string | First page |
| `lastpage` | string | Last page |
| `volume` | string | Volume |
| `issue` | string | Issue |

### Keywords & terms

| Column | Type | Description |
|---|---|---|
| `keywords` | array\<string\> | Author keywords |
| `keywords_details` | array\<struct\> | Keywords with language: `{keyword, lang}` |
| `n_keywords` | short | Keyword count |
| `terms` | array\<struct\> | Index terms: `{candidate, controlled, mainterm, type, weight}` |
| `n_terms` | short | Term count |
| `n_term_types` | short | Distinct term type count |

### Funding

| Column | Type | Description |
|---|---|---|
| `funding_list` | array\<struct\> | Funding sources: `{agency, agency_acronym, agency_country, agency_id, agency_matched_string, ids}` |
| `funding_text` | array\<string\> | Raw funding acknowledgement text |
| `funding_count` | short | Number of funding entries |
| `funding_addon_type` | string | Addon type |
| `funding_content` | string | Funding content metadata |
| `grant` | array\<struct\> | Grant info: `{grant_acronym, grant_agency, grant_agency_id, grant_id}` |

### Correspondence

| Column | Type | Description |
|---|---|---|
| `correspondence` | array\<struct\> | Corresponding author addresses: `{country, txt}` |

### Copyright & access

| Column | Type | Description |
|---|---|---|
| `copyright_types` | array\<string\> | Copyright type labels |
| `free_to_read_status_list` | array\<string\> | Free-to-read status codes |
| `meta_language` | array\<string\> | Document languages |
| `meta_source_country` | string | Country of the source |

### Internal / quality fields

| Column | Type | Description |
|---|---|---|
| `corrupt_xml_file_B` | boolean | True if source XML was corrupt |
| `suppressdummy` | long | Suppression flag |
| `xmlsize` | long | Size of source XML in bytes |

---

## Common access patterns

### Filter out preprints — always apply `nopp()` first

Scopus indexes not only peer-reviewed literature but also **preprints** (e.g. arXiv, bioRxiv,
SSRN). Preprints are distinguished by their `dbcollections` value: they appear as `ARCSPR`
(and similar) instead of `SCOPUS` or `MEDL`.

`nopp()` keeps only documents that are in the `SCOPUS` or `MEDL` collections:
```python
import column_functions
df = df_ani.filter(column_functions.nopp())
# nopp() = arrays_overlap(dbcollections, ['SCOPUS', 'MEDL'])
```

**This filter must be applied to every analysis.** Preprints do have reference lists
(populated `citations` arrays), so they will silently inflate citation counts if not
removed. This matters in two ways:

1. **Citing side** — a preprint can appear in `citations` of another document. When
   building a citing-document set or counting citations received, filter the source to
   `nopp()` so preprints are not counted as legitimate citing papers.
2. **Cited side** — a preprint's EID can appear inside the `citations` array of other
   papers. When computing times-cited for a target document, restrict the universe of
   citing papers to `nopp()` so only core Scopus/MEDL records are counted.

```python
# Correct: count times-cited using only core content on BOTH sides
df_core = df_ani.filter(column_functions.nopp())

df_times_cited = (
    df_core
    .select(F.col('Eid').alias('citing_eid'), F.explode('citations').alias('cited_eid'))
    .groupBy('cited_eid')
    .agg(F.count('*').alias('times_cited'))
)
```

> Always double-check `nopp()` is applied whenever citation counts or citing-document
> sets are computed. Missing this filter is a common source of inflated citation numbers.

### Get article title
```python
df.select(
    'Eid',
    column_functions.get_ani_title_col(F.col('citation_title')),
)
```

### Filter by journal (source ID)
```python
# Heliyon example
df.filter(F.col('source.srcid') == 21100411756)
```

### Look up a journal's srcid
```python
df_ani.filter(
    F.lower(F.col('source.sourcetitle')).contains('lancet')
).select('source.srcid', 'source.sourcetitle').distinct().show(truncate=False)
```

### Linking authors to affiliations (`Au` → `Au_Af` → `Af`)

Authorship and affiliation are stored in three parallel arrays that must be joined by
sequence number:

| Array | Key field | Role |
|---|---|---|
| `Au` | `Authorseq` (1-based int) | One element per author; contains name fields, `auid`, `orcid` |
| `Au_Af` | `Authorseq` + `affiliation_seq` | Junction table — maps each author to zero or more affiliations |
| `Af` | positional (1-based index) | One element per affiliation; contains `afid`, country, org name, etc. |

`Au_Af.affiliation_seq` is a **1-based position** into the `Af` array for the same
document. Only links where `validity_B == True` are valid.

The join pattern used by `scd_functions._authors_with_affiliations_explode()` is:

```python
# 1. Explode authors → (Eid, Authorseq, au_struct)
df_authors = (
    df.select('Eid', F.explode_outer('Au').alias('au'))
      .select('Eid', F.col('au.Authorseq').cast('int').alias('Authorseq'), 'au')
)

# 2. Explode Au_Af and keep only valid links
df_links = (
    df.select('Eid', F.explode_outer('Au_Af').alias('link'))
      .where(F.col('link.validity_B') == True)
      .select(
          'Eid',
          F.col('link.Authorseq').cast('int').alias('Authorseq'),
          F.col('link.affiliation_seq').alias('affiliation_seq'),
      )
)

# 3. Give Af elements their 1-based sequence numbers
df_af = (
    df.select(
        'Eid',
        F.sequence(F.lit(1), F.expr('size(Af)')).alias('seq'),
        'Af',
    )
    .withColumn('kv', F.arrays_zip('seq', 'Af'))
    .select('Eid', F.explode_outer('kv').alias('p'))
    .select('Eid', F.col('p.seq').alias('affiliation_seq'), F.col('p.Af').alias('af'))
)

# 4. Join to get author + their affiliations
df_author_af = (
    df_authors
    .join(df_links, ['Eid', 'Authorseq'], 'left')
    .join(df_af,    ['Eid', 'affiliation_seq'], 'left')
)
```

To get all unique countries for each author across their affiliations:
```python
(
    df_author_af
    .groupBy('Eid', 'Authorseq', F.col('au.auid'))
    .agg(F.array_distinct(F.collect_list('af.affiliation_country')).alias('countries'))
)
```

> `scd_functions` exposes `orig_formatted_af(col)` and `orig_formatted_af_with_id(col)`
> to format a single `Af` element into a human-readable string (org + city + country +
> optional affiliation IDs). Use `author_full_name(col_au_element)` to format an `Au`
> element into `"Surname, Givenname (auid)"` format.

### Build citation edge list
```python
df_edges = (
    df_ani
    .filter(column_functions.nopp())
    .select(
        F.col('Eid').alias('citing_eid'),
        F.explode('citations').alias('cited_eid'),
    )
    .distinct()
)
```

### Count by year
```python
df_ani.filter(column_functions.nopp()).groupBy('sort_year').count().orderBy('sort_year').show()
```

---

## Related tables and datasets

| Dataset | Access | Description |
|---|---|---|
| SciVal topic mapping | `snapshot_functions.scival.get_table('topic_eid')` | Maps EIDs to SciVal topic clusters |
| SciVal prominence | `snapshot_functions.scival.get_table('topic_prominence')` | Topic prominence/rank scores |
| ADS publication metrics | `snapshot_functions.ads.publication.get_table(name)` | Publication-level bibliometric snapshots |
| ADS author metrics | `snapshot_functions.ads.author.get_table(name)` | Author-level bibliometric snapshots |
| Retraction Watch | `rwdb_functions.get_clean_rw_df(...)` | Retracted papers matched to ANI EIDs |
| ASJC codes | `static_data.asjc` | Subject classification code → description |
| Document types | `static_data.doctype` | Citation type code → description |
| SDG mapping | `static_data.sdg` | UN Sustainable Development Goals |

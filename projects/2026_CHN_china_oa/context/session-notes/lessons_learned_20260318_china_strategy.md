# Lessons Learned — China Strategy OA Journal Aggregation (2026-03-18)

**Session:** China university Elsevier journal aggregation for APC revenue proxy analysis  
**Deliverables:** Two CSVs (journal-level and institution-year summary aggregates, 465K + 2.9K rows)  
**Technology:** PySpark on Databricks + direct local S3 sharing  

---

## 1. OA Status Classification via SCD Functions

### What Worked
Pulled OA status mappings **directly from `scd_functions.csv_formatted_from_selected_ani()`** instead of inventing custom logic:
- `repositoryam`, `repositoryvor`, `repository` → **Green**
- `publisherfullgold`, `publisherhybridgold` → **Gold**
- `publisherfree2read` → **Bronze**
- No status or unmapped → **Closed/Unknown**

### Recommendation for Agent Knowledge
**Pattern:** For any future OA classification task, reference the SCD library's Open Access builder at `rads_library/scd_functions.py:550–560`. This ensures consistency with the internal ADS metrics pipeline and avoids duplication.

**Caveat:** The `free_to_read_status_list` field in ANI can be `null`, so always wrap with `F.coalesce(..., F.array().cast('array<string>'))` to avoid breaking on null arrays.

---

## 2. Spark-First Architecture for Institution-Scale Joins

### What Worked
Performed all heavy lifting on Databricks cluster:
- Loaded ANI (13.3M rows for 2023–2025)
- Joined SciVal institution → AFID mapping (2,522 links across 1,001 target universities)
- Exploded affiliation arrays per paper
- Joined Rosetta for publisher/title enrichment
- Applied OA tags
- Aggregated and exported

**No local post-processing required.** Files were production-ready from Spark.

### Recommendation for Agent Knowledge
**Pattern:** For institution-level analysis at scale (especially with Array columns like `Af` and multi-row aggregations), resist the temptation to export raw data and process locally. Keep the transformation pipeline **entirely in Spark** and export only the final aggregated output. Local should only be used for:
- QA spot checks on sample rows
- Charting or formatting for presentations
- Downstream reshaping if needed by downstream system

---

## 3. Rosetta Consolidation: Single Load + Filter

### What Worked (Original)
```python
df_rosetta_pub = snapshot_functions.rosetta.get_publisher_view(...)
df_rosetta_title = snapshot_functions.rosetta.get_table(...)
df_papers = df_papers.join(df_rosetta_pub, ...).join(df_rosetta_title, ...)
```

### What's Better
```python
df_rosetta = snapshot_functions.rosetta.get_table(current_only=True).filter(
    F.lower(F.coalesce(F.col('publisher'), F.lit(''))).contains('elsevier')
)
df_papers = df_papers.join(df_rosetta.select('srcid', 'publisher', 'title'), ...)
```

### Recommendation for Agent Knowledge
**Pattern:** Don't use specialized Rosetta views (e.g., `get_publisher_view()`) unless you specifically need flattened publisher-only fields or BM/imprint joins. For general source enrichment, use `get_table(current_only=True)` and apply domain filters **before** the join. This reduces intermediate DataFrame materialization and makes the filter pushdown visible.

---

## 4. Institution Mapping via SciVal IDs

### What Worked
User-provided institution list contained SciVal `institution_id` values. This allowed a **clean lookup** into:
1. `snapshot_functions.scival.get_table('institution')` → AFID mapping
2. `snapshot_functions.scival.get_table('institution_metadata')` → clean institution names

Result: 1,001 input institutions → 2,522 institution-AFID links (many universities have multiple AFIDs due to department/campus splits).

### Recommendation for Agent Knowledge
**Pattern:** For institution-based analyses, always ask for SciVal institution IDs in the input list. This avoids fuzzy name matching and gives direct access to SciVal's authoritative institution hierarchy. If users only provide names, document the fallback: use `OrgDB` for Scopus affiliations and manually verify coverage.

**Caveats:**
- Some universities may have incomplete AFID coverage in SciVal (coverage gaps exist; use LEFT JOIN)
- Department-level AFIDs will inflate the count but are usually desired for granular analyses

---

## 5. Publisher Normalization: Rosetta + Fallback

### What Worked
```python
.withColumn('publisher_norm', F.coalesce('publisher_rosetta', 'publisher_ani'))
```

Rosetta publisher field is **curated and standardized**; ANI's raw `source.publishername` has junk values (e.g., `NULL`, encoding artifacts). Fallback to ANI only when Rosetta is missing.

### Recommendation for Agent Knowledge
**Pattern:** For any publisher-level filtering or grouping, always prefer Rosetta curated values with ANI as fallback. Document the fallback percentage in outputs so Finance/Strategy teams know data quality.

---

## 6. Direct Local S3 Sharing (vs. Library Share Function)

### What Worked (Preferred)
```bash
aws s3 cp s3://rads-projects/.../output/file.csv.gz \
         s3://rads-custom-data/download/<random-id>/file.csv.gz
# Generate metadata JSON with public URLs manually
```

Advantages:
- **Immediate:** No waiting for cron job to pick up metadata and copy
- **Transparent:** Exact URLs printed right away for distribution
- **Simple:** Metadata is a small JSON file, easy to version-control in `/tmp/`

### What We Avoided
```python
dataframe_functions.share_dataframe(df, recipient='...')  # writes to temporary_to_be_deleted/custom_data/
# Waits for external cron to copy to rads-custom-data
```

### Recommendation for Agent Knowledge
**Pattern:** Once Spark outputs are **validated and final**, use **direct local S3 copy** to `rads-custom-data/download/<session-id>/` and generate metadata locally. Reserve the library `share_dataframe()` function only for ad-hoc exports that need to be picked up by the rads-custom-data pipeline (e.g., one-off research requests or when you don't control the recipient list).

**SOP for delivery:**
1. Spark writes to project output folder
2. Validate row counts and sample records
3. `aws s3 cp` → rads-custom-data
4. Print metadata JSON with public URLs
5. Share URLs directly with recipients

---

## 7. Time Window Filtering on ANI

### What Worked
```python
.filter(F.col('sort_year').between(2023, 2025))
```

Used `sort_year` (not publication year) to align with Scopus indexing conventions. This ensures consistent year boundaries across all downstream analyses.

### Recommendation for Agent Knowledge
**Pattern:** Always use `sort_year` for temporal filters on ANI unless explicitly asked for publication year. Document the choice so downstream users know whether the data is indexed-year or published-year.

---

## 8. Mandatory `nopp()` Filter

### What Worked
Applied `column_functions.nopp()` as the **first filter** on ANI:
```python
spark.table(f'scopus.ani_{ani_stamp}').filter(column_functions.nopp())
```

This removes preprints (arXiv, bioRxiv, SSRN) automatically, ensuring only peer-reviewed records are counted.

### Why It Matters
Without `nopp()`, preprints can silently inflate citation counts and publication counts because they have `citations` arrays but shouldn't be counted as legitimate publications in revenue/impact analyses.

### Recommendation for Agent Knowledge
**Rule (already documented):** Always apply `nopp()` as the first filter. No exceptions. If a user specifically requests preprints, document that deviation loudly in outputs.

---

## 9. Cache Invalidation Strategy

### What Happened
Used `cache_v1` folder for all intermediate Spark outputs. If code logic changes (e.g., OA mapping), the cache becomes stale.

### Recommendation for Agent Knowledge
**Pattern:** Adopt explicit versioning for cache folders: `cache_v1`, `cache_v2`, etc. When re-running after code changes, increment the version number. This leaves old caches as fallback and makes the change intent clear in code review.

**Alternative (if cleanup is needed):** Before deletion, always run `aws s3 ls s3://rads-projects/.../cache_folder/` to verify only data files (`.parquet`, `_SUCCESS`, `_committed_*`) are present. If you see subdirectories (`PRE` lines), you're at the wrong level.

---

## Summary of Patterns to Codify

| Pattern | Applies To | Recommendation |
|---------|-----------|-----------------|
| OA status via SCD | Any open-access classification | Use `scd_functions` mappings; document null-arm handling |
| Spark-first joins | Large institution/source analysis | Never move raw joins to local; final output only |
| Rosetta consolidation | Publisher/journal enrichment | Use `get_table()` + filter; avoid specialized views |
| SciVal institution IDs | Institution-level workflows | Require as input; document AFID coverage gaps |
| Publisher normalization | Filtering by publisher | Rosetta + ANI fallback; track fallback % |
| Direct S3 sharing | Validated outputs | Copy to rads-custom-data; generate metadata locally |
| `sort_year` filtering | Temporal queries | Always use; document if using pub_year instead |
| `nopp()` mandatory | All ANI analyses | Apply first; no exceptions without user sign-off |
| Cache versioning | Intermediate Spark outputs | Use `cache_vN`; increment on logic change |

---

## Questions for Agent-Core Refinement

1. **Should SCD OA mappings be exposed as a standalone function** in `column_functions.py` for easier reuse across notebooks?
2. **Should we codify a preferred project structure** for institution-level analyses (e.g., always use SciVal IDs if available, LEFT JOIN all secondary tables)?
3. **Should the direct S3 sharing pattern be documented in the local-python runbook** as the preferred approach over `share_dataframe()` for final outputs?
4. **Should cache versioning be a formal convention** in core-rules.md (currently rule 8 mentions it but could be clearer)?


# Article_PlumX_Metrics

**Level:** Publication  
**Pipeline notebook:** `Article_PlumX_Data_Normalized.py`  
**Hive table:** `fca_ds.Article_PlumX_Metrics_{YYYYMMDD}`

---

## Description

Per-article PlumX altmetrics counts, with a normalized score for each
`(source, category)` combination relative to the article's year × ASJC334 ×
document-type cohort. Articles without a DOI get null for count-based columns.

PlumX sources include: captures, citations, mentions, social_media, usage.
Each source can have one or more categories (e.g., "Reads", "Exports-Saves",
"Blog Mentions", "Tweets", "Abstract Views", etc.).

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_PlumX_Metrics')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `Sort_Year` | int | Publication year |
| `asjc334` | string | 4-digit ASJC subfield (one row per ASJC if multi-disciplinary) |
| `citation_type` | string | Document type |
| `doi` | string | DOI (null if not available) |
| `source` | string | PlumX source category (e.g. `captures`, `mentions`) |
| `category` | string | PlumX interaction type (e.g. `Reads`, `Tweets`) |
| `Total_Interactions` | long | Total interaction count; null if no DOI |
| `At_least_one_interaction` | int | 1 if ≥ 1 interaction, 0 if none, null if no DOI |
| `Average_Publications_with_Interactions_Year_Subject_Citation_Type` | double | Average `At_least_one_interaction` for same year×asjc334×citation_type×source×category cohort |
| `Normalized_Cited_Publications_Count_by_Year_Subject_Citation_Type` | double | `At_least_one_interaction / Average_…` (relative engagement) |

---

## Notes

- Multi-ASJC articles produce multiple rows per (source, category).
- The cross-join of all source×category combinations means every article ×  
  ASJC has a row for every (source, category) pair, even if count is 0.
- Source data: `scopus_public_dataset_prod` parquet on the SciVal tech data mount.

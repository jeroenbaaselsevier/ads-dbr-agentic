# Multidisciplinarity (p_multidisciplinarity)

**Level:** Publication  
**Pipeline notebook:** `Multidisciplinarity_Master_Scopus_ani.py`  
**Hive table:** `fca_ds.multidisciplinarity_scores_{YYYYMMDD}`  
**Path subfolder:** `MultDisc/p_multidisciplinarity_{YYYYMMDD}/`  
**Depends on:** `sm_classifier_tfidf_logreg_v1` (for SM subfield classification)

---

## Description

Multidisciplinarity (MD) score for each article using the Rao-Stirling
diversity index applied to the Science-Metrix (SM) subfield profile of the
article's reference list. A cosine-similarity matrix across subfields (built
from co-citation patterns) provides the distance weights. MD is null for
articles with fewer than 5 cross-subfield references.

Produces normalized scores and decile/top-percentile flags for 4 strata:

| Suffix | Stratum |
|---|---|
| `_Subfield_DocType` | Normalized within SM subfield × document type |
| `_Subfield_Year_DocType` | Normalized within SM subfield × year × document type |
| `_NoSingleAuthor` | Same as above, excluding single-author papers |
| `_Year_NoSingleAuthor` | Same as above, excluding single-author papers |

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('MultDisc/p_multidisciplinarity_20250301')
# Or use list() to find the exact path name:
snapshot_functions.ads.publication.list()
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `eid` | long | Scopus article ID |
| `year` | int | Publication year |
| `subfield_article` | string | SM subfield code of the article |
| `document_type` | string | Document type |
| `md_raw` | double | Raw Rao-Stirling diversity score (null if < 5 cross-subfield refs) |
| `n_auid_effective` | int | Number of effective authors (excluding single-author flag) |

### Per-stratum columns (4 strata × 4 metrics each)

For each `{stratum}` in `Subfield_DocType`, `Subfield_Year_DocType`,
`Subfield_DocType_NoSingleAuthor`, `Subfield_Year_DocType_NoSingleAuthor`:

| Column | Description |
|---|---|
| `MD_Norm_{stratum}` | Normalized MD score (z-score or fraction) within stratum |
| `Decile_1_{stratum}` … `Decile_10_{stratum}` | Decile flags (1/0) indicating which decile within stratum |
| `Top_1_pct_Norm_{stratum}` | 1 if in top 1% of stratum, else 0 |
| `Top_5_pct_Norm_{stratum}` | 1 if in top 5% of stratum, else 0 |

---

## Notes

- Based on SM subfield classification from `sm_classifier_tfidf_logreg_v1`.
- The cosine similarity matrix uses co-citation co-occurrence across all ANI records.

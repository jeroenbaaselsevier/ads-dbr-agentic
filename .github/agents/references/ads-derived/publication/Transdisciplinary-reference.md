# Transdisciplinary (scopus_ani_transdisciplinarity)

**Level:** Publication  
**Pipeline notebook:** `Transdisciplinary_Master_all_scopus_ani.py`  
**Hive table:** `fca_ds.transdisciplinarity_scores_{YYYYMMDD}`  
**Path subfolder:** `transdisciplinarity/scopus_ani_transdisciplinarity_{YYYYMMDD}/`  
**Depends on:** `sm_classifier_tfidf_logreg_v1`

---

## Description

Transdisciplinarity (TD) score for each article using the Rao-Stirling diversity
index applied to the SM subfield profile of the article's own reference list.
Higher scores indicate the article draws from more diverse research fields.

TD is null for articles with fewer than 5 cross-subfield references.

Normalized scores and decile/top-percentile ranks are produced for 2 strata:
- `_Subfield_DocType` — normalized within SM subfield × document type
- `_Subfield_Year_DocType` — normalized within SM subfield × year × document type

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('transdisciplinarity/scopus_ani_transdisciplinarity_20250301')
# Check exact path name with:
snapshot_functions.ads.publication.list()
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `eid` | long | Scopus article ID |
| `year` | int | Publication year |
| `subfield_article` | string | SM subfield of the article |
| `document_type` | string | Document type |
| `total_cits` | long | Total citations in reference list (denominator) |
| `n_references` | int | Number of cross-subfield references |
| `transdisciplinarity_raw` | double | Raw Rao-Stirling diversity score (null if < 5 refs) |

### Per-stratum columns (2 strata × 4 metrics each)

For `{stratum}` in `Subfield_DocType`, `Subfield_Year_DocType`:

| Column | Description |
|---|---|
| `transdisciplinarity_Norm_{stratum}` | Normalized TD score within stratum |
| `Decile_1_{stratum}` … `Decile_10_{stratum}` | Decile flags (1/0) |
| `Top_1_pct_Norm_{stratum}` | 1 if top 1%, else 0 |
| `Top_5_pct_Norm_{stratum}` | 1 if top 5%, else 0 |

---

## Notes

- Distinguish from Multidisciplinarity: TD is based on the **reference list**
  (outgoing citations), while MD is also based on the reference list but
  uses a different co-citation similarity matrix.
- Both use the SM classification for subfield assignment.

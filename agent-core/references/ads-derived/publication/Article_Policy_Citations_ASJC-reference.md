# Article_Policy_Citations_ASJC

**Level:** Publication  
**Pipeline notebook:** `Article_Policy_Citations_ASJC.py`  
**Hive table:** `fca_ds.Article_Policy_Citations_ASJC_{YYYYMMDD}`  
**Depends on:** `Article_Policy_Citations_EID`

---

## Description

Normalizes per-article policy citation counts against the article's year ×
ASJC334 × document-type cohort. For each article × ASJC subfield, produces
a normalized score = individual `Cited_at_least_once` divided by the cohort
average.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Policy_Citations_ASJC')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `Sort_Year` | int | Publication year |
| `asjc334` | string | 4-digit ASJC subfield |
| `citation_type` | string | Document type |
| `Total_Policy_Citations` | long | Raw policy citation count (null if no DOI) |
| `Cited_at_least_once` | int | 1/0/null binary flag |
| `Average_Cited_Publications_by_Year_Subject_Citation_Type` | double | Mean `Cited_at_least_once` for year×asjc334×citation_type cohort |
| `Normalized_Cited_Publications_Count_by_Year_Subject_Citation_Type` | double | `Cited_at_least_once / Average_…` |

---

## Notes

- Multi-ASJC articles produce multiple rows.
- For SM broad-field normalization see
  [Article_Policy_Citations_SM_BID-reference.md](Article_Policy_Citations_SM_BID-reference.md).

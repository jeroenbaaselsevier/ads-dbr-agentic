# Article_Policy_Citations_SM_BID

**Level:** Publication  
**Pipeline notebook:** `Article_Policy_Citations_SM_BID.py`  
**Hive table:** `fca_ds.Article_Policy_Citations_SM_BID_{YYYYMMDD}`  
**Depends on:** `Article_Policy_Citations_EID`, `sm_classifier_tfidf_logreg_v1`

---

## Description

Same as `Article_Policy_Citations_ASJC` but uses the Science-Metrix (SM)
broad interdisciplinary discipline (`bid`) instead of ASJC codes for
normalization. The SM classification gives 5 broad disciplines (Natural
Sciences, Life Sciences, Health Sciences, Social Sciences, Arts & Humanities).

Unlike the ASJC variant, normalization here uses **subtraction** (not division):
`Normalized = Cited_at_least_once - Average`.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Policy_Citations_SM_BID')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `Sort_Year` | int | Publication year |
| `citation_type` | string | Document type |
| `bid` | string | SM broad interdisciplinary discipline code |
| `Total_Policy_Citations` | long | Raw policy citation count (null if no DOI) |
| `Cited_at_least_once` | int | 1/0/null binary flag |
| `Average_Cited_Publications_by_Year_Subject_Citation_Type` | double | Mean `Cited_at_least_once` for year×bid×citation_type cohort |
| `Normalized_Cited_Publications_Count_by_Year_Subject_Citation_Type` | double | `Cited_at_least_once - Average_…` (subtraction, not division) |

---

## Notes

- Uses the latest `smcs_mapping_model_tfidf_logreg_2021_ani_up_to_*` Hive table.
- `bid` codes: see `sm.classification` Hive table for code → discipline mapping.
- Normalization formula differs from ASJC variant (subtraction vs division).

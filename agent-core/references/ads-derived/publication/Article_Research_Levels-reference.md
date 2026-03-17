# Article_Research_Levels

**Level:** Publication  
**Pipeline notebook:** `Article_Research_Levels.py` (delegates to `basic_applied_classifier_prod_v031_table_creator.py`)  
**Hive table:** `fca_ds.BAC_v031_{YYYYMMDD}` (check exact name in pipeline)

---

## Description

ML-based classification of each Scopus article as basic research, applied
research, or other. Uses a TF-IDF + Logistic Regression model (BAC v0.31)
trained on article text features (title, abstract, keywords).

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Research_Levels')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `Eid` | long | Scopus article ID |
| `research_level` | string | `basic`, `applied`, or `other` |

---

## Notes

- `Eid` column name is capitalized (not `EID` or `eid`).
- Only articles with title or abstract are classified; others may be absent.
- The BAC v0.31 model is stored at:  
  `/mnt/els/rads-main/mappings_and_metrics/mappings/EID_classifications/BAC/`

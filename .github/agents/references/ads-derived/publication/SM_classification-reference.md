# SM_classification (smcs_mapping_model_tfidf_logreg_2021)

**Level:** Publication  
**Pipeline notebook:** `sm_classifier_tfidf_logreg_v1.py`  
**Hive table:** `fca_ds.smcs_mapping_model_tfidf_logreg_2021_ani_up_to_{YYYYMMDD}`  
**Also produces:** `fca_ds.sm_journal_classification_srcid_{YYYYMMDD}`

---

## Description

Science-Metrix (SM) subfield classification for each Scopus article, using a
TF-IDF + Logistic Regression classifier (model v2021). Produces both article-level
and journal-level classification.

The SM classifier assigns each article to one of ~175 SM subfields (the
`bid` code is a broader grouping into 5 disciplines). This classification is
used as input for Multidisciplinarity, Transdisciplinarity, and
Policy Citations SM-BID tables.

---

## Load

```python
# Article-level: find latest table name
import re
from pyspark.sql.functions import col
sm_tables = [t for t in sqlContext.tableNames('fca_ds') 
             if re.match(r'smcs_mapping_model_tfidf_logreg_2021_ani_up_to_\d{8}', t)]
latest = sorted(sm_tables)[-1]
df = spark.table(f'fca_ds.{latest}')
```

---

## Article-level columns

| Column | Type | Description |
|---|---|---|
| `Eid` | long | Scopus article ID |
| `pred_probs` | array\<double\> | Model prediction probabilities per subfield |
| `bid` | string | SM broad interdisciplinary discipline code |

---

## Journal-level columns (`sm_journal_classification_srcid_{YYYYMMDD}`)

| Column | Type | Description |
|---|---|---|
| `srcid` | long | Scopus source ID |
| SM subfield columns | string | Dominant SM subfield assigned to the journal |

---

## Notes

- Model: sklearn Pipeline (TF-IDF + LogReg), stored at  
  `/mnt/els/rads-main/mappings_and_metrics/mappings/EID_classifications/sm_ml_subfield_clf_v1/models/`
- SM classification taxonomy: `sm.classification` Hive table with  
  columns `SubField`, `bid`, `Field`, `Domain`.
- Journals need ≥ 50 documents to receive a journal-level classification.
- Articles classified as "General Science & Technology" or  
  "General Arts, Humanities & Social Sciences" are reclassified based on
  journal-level majority vote where possible.

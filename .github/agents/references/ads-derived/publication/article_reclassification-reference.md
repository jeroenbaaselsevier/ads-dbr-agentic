# article_reclassified

**Level:** Publication (supporting/mapping)  
**Pipeline notebook:** `article_reclassification.py`  
**Hive table:** `fca_ds.article_reclassified_{YYYYMMDD}`  
**Used by:** `FWCI_recl_normal_and_no_self_cit_cleaned.py`

---

## Description

Reclassified ASJC codes for articles that have only the general/multidisciplinary
code `1000` as their sole ASJC. For these articles, the pipeline finds the most
frequent non-1000 ASJC among:
1. Papers that **cite** this article (citing ASJC)
2. Papers **cited by** this article (cited ASJC)

Cited ASJC takes priority; citing ASJC is a fallback.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('article_reclassified')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `Eid` | long | Scopus article ID |
| `ASJC_reclassified` | array\<string\> | New ASJC code(s) assigned after reclassification |

---

## Notes

- Only articles with exactly one ASJC code equal to `1000` are present (sparse).
- `ASJC_reclassified` may be null if no suitable ASJC could be inferred from
  citing or cited papers.
- This table is an intermediate/support table; analysts typically consume
  [FWCI_recl_All-reference.md](FWCI_recl_All-reference.md) instead.

# Article_Citations_Perc_ASJC334

**Level:** Publication  
**Pipeline notebook:** `Citation_Percentiles.py`  
**Hive table:** `fca_ds.Article_Citations_Perc_ASJC334_{YYYYMMDD}`

---

## Description

Citation percentile rank for each article × narrow ASJC subfield combination.
An article with multiple ASJC codes will have one row per 4-digit ASJC.
Percentile is computed within the cohort of articles sharing the same
`sort_year` and 4-digit `asjc`.

This is the most fine-grained citation percentile table.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Citations_Perc_ASJC334')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `eid` | long | Scopus article ID |
| `asjc` | string | 4-digit ASJC subfield code (e.g. `"1311"` = Genetics) |
| `sort_year` | int | Publication year |
| `Total_Citations` | long | Total citations for this article |
| `Citation_Percentile` | double | Percentile rank within `sort_year` × `asjc` cohort (0–100) |

---

## Notes

- Multi-disciplinary articles produce multiple rows.
- For broader discipline-level percentiles see [Article_Citations_Perc_ASJC27-reference.md](Article_Citations_Perc_ASJC27-reference.md).

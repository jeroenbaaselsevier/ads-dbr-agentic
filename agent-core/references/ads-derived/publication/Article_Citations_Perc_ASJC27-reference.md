# Article_Citations_Perc_ASJC27

**Level:** Publication  
**Pipeline notebook:** `Citation_Percentiles.py`  
**Hive table:** `fca_ds.Article_Citations_Perc_ASJC27_{YYYYMMDD}`

---

## Description

Citation percentile rank for each article × broad ASJC subject area
combination. An article with multiple ASJC codes will have one row per
2-digit ASJC. Percentile is computed within the cohort of articles sharing
the same `sort_year` and 2-digit `asjc`.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Citations_Perc_ASJC27')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `eid` | long | Scopus article ID |
| `sort_year` | int | Publication year |
| `asjc` | string | 2-digit ASJC code (e.g. `"14"` = Business, Management & Accounting) |
| `Total_Citations` | long | Total citations for this article |
| `Citation_Percentile` | double | Percentile rank within `sort_year` × `asjc` cohort (0–100) |

---

## Notes

- Multi-disciplinary articles produce multiple rows (one per ASJC27 code).
- To get the list of 2-digit ASJC codes and names use `static_data.asjc`.
- For a single global percentile see [Article_Citations_Perc_Total-reference.md](Article_Citations_Perc_Total-reference.md).

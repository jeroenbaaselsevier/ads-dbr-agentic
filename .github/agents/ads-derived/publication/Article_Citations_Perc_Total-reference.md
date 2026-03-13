# Article_Citations_Perc_Total

**Level:** Publication  
**Pipeline notebook:** `Citation_Percentiles.py`  
**Hive table:** `fca_ds.Article_Citations_Perc_Total_{YYYYMMDD}`

---

## Description

Citation percentile rank for each article, computed against the global (all
disciplines combined) cohort of articles in the same publication year. The most
discipline-agnostic of the three citation percentile tables.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Citations_Perc_Total')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `eid` | long | Scopus article ID |
| `sort_year` | int | Publication year |
| `Total_Citations` | long | Total citations received |
| `Citation_Percentile` | double | Percentile rank within the `sort_year` cohort (0–100) |
| `asjc` | string | Literal `"0"` (no subject filter — all disciplines) |

---

## Related tables

- [Article_Citations_Perc_ASJC27-reference.md](Article_Citations_Perc_ASJC27-reference.md) — percentile within broad subject area (27 categories)  
- [Article_Citations_Perc_ASJC334-reference.md](Article_Citations_Perc_ASJC334-reference.md) — percentile within narrow subfield (334 categories)

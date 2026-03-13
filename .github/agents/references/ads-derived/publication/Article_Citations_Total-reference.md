# Article_Citations_Total_All_and_NonSelf_Citations

**Level:** Publication  
**Pipeline notebook:** `Article_Citation_Year_One_Table.py`  
**Hive table:** `fca_ds.Article_Citations_Total_All_and_NonSelf_Citations_{YYYYMMDD}`

---

## Description

Per-article summary of total citation counts (all citations + excluding author
self-citations) together with citation percentile ranks within the article's
publication year. A self-citation is defined as any cited/citing pair that
shares at least one author (`auid`).

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Citations_Total_All_and_NonSelf_Citations')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID (numeric, without `2-s2.0-` prefix) |
| `Sort_Year` | int | Publication year |
| `Total_Citations` | long | Total incoming citations (all) |
| `Citation_Percentile_All` | double | Percentile rank (0–100) within sort_year, all citations |
| `Total_Citations_Excluding_Self_Cits` | long | Citations excluding author self-citations |
| `Citation_Percentile_Excluding_Self_Cits` | double | Percentile rank (0–100) within sort_year, no self-cits |

---

## Notes

- Join key is `EID` (long). To convert from string EID (`2-s2.0-NNNN...`) use `column_functions.eid_to_long()`.  
- Percentile = 0 means least-cited, 100 means most-cited in the year cohort.
- The intermediate per-year table `Article_Citations_by_Year_All_and_NonSelf_Citations`
  (see [Article_Citations_by_Year-reference.md](Article_Citations_by_Year-reference.md)) is
  also written by the same notebook.

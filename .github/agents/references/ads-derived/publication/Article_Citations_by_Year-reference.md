# Article_Citations_by_Year_All_and_NonSelf_Citations

**Level:** Publication  
**Pipeline notebook:** `Article_Citation_Year_One_Table.py`  
**Hive table:** `fca_ds.Article_Citations_by_Year_All_and_NonSelf_Citations_{YYYYMMDD}`

---

## Description

Time-series citation table: for each article, contains one row per
(publication year, citation year) pair showing how many citations were received
in that citation year, with and without author self-citations.

Use this table for year-by-year citation analysis or citation trajectory work.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Citations_by_Year_All_and_NonSelf_Citations')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `sort_year` | int | Publication year of the article |
| `cite_year` | int | Year in which the citations were received |
| `citations` | long | Citations received in `cite_year` (all) |
| `citations_excluding_self_cits` | long | Citations received in `cite_year`, excluding self-citations |

---

## Notes

- Self-citation = citing and cited paper share at least one `auid`.
- Rows only exist for years where citations > 0 (sparse format).
- For totals, see [Article_Citations_Total-reference.md](Article_Citations_Total-reference.md).

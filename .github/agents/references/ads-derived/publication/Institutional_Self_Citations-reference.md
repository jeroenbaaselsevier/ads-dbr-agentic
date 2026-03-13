# Institutional_Self_Citations

**Level:** Publication  
**Pipeline notebook:** `Institutional_Self_Citations.py`  
**Hive table:** `fca_ds.Institutional_Self_Citations_{YYYYMMDD}`  
**Depends on:** `Institutions` (Institution + Institution_Afid tables)

---

## Description

For each (cited article, citing institution) pair: the total number of
citations that article received from that institution, and the count excluding
cases where the citing paper is from the same institution as the cited paper
(i.e. excluding institutional self-citations).

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Institutional_Self_Citations')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID (the cited article) |
| `Cited_Institution` | long | SciVal Institution ID of the citing institution |
| `Total_Citations_check` | long | Total citations by this institution to this article |
| `Citiations_Excluding_Institutional_Self_Citations_check` | long | Citations excluding those where citing and cited share the same institution |

---

## Notes

- Note the typo in the column name: `Citiations_Excluding_…` (not `Citations`).
- An article appears multiple times — once per citing institution.
- To get total institutional self-citation count per article, aggregate:
  ```python
  df.withColumn('inst_self_cits', F.col('Total_Citations_check') - F.col('Citiations_Excluding_Institutional_Self_Citations_check'))
  ```
- Institution mapping from [Institution_Afid-reference.md](../mappings/Institution_Afid-reference.md).

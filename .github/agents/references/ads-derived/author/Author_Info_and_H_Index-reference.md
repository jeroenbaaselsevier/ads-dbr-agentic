# Author_Info_and_H_Index

**Level:** Author  
**Pipeline notebook:** `H_Index_One_Table_From_Ani.py`  
**Hive table:** `fca_ds.Author_Info_and_H_Index_{YYYYMMDD}`

---

## Description

Per-author summary table containing H-index, citation counts, publication
counts, fractional metrics, and average FWCI values. Derived from the full
ANI corpus — the most comprehensive author-level summary available.

---

## Load

```python
df = snapshot_functions.ads.author.get_table('Author_Info_and_H_Index')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `auid` | long | Scopus author ID |
| `HIDX` | int | H-index (all citations) |
| `HIDX_NS` | int | H-index excluding self-citations |
| `Total_Citations` | long | Total incoming citations (all) |
| `Total_Non_Self_Citations` | long | Total citations excluding author self-citations |
| `Fractional_Citations` | double | Fractional citation count (1/n_authors per paper) |
| `Fractional_Non_Self_Citations` | double | Fractional non-self-citation count |
| `Pubs` | int | Total publications |
| `Fractional_Pubs` | double | Fractional publication count (1/n_authors per paper) |
| `Average_5Y_FWCI` | double | Mean FWCI_5y (all citations) across author's papers |
| `average_5y_fwci_noselfcits` | double | Mean FWCI_5y (no self-cits) across author's papers |
| `Average_FWCI_5Y_Fractional` | double | Fractionally weighted mean FWCI_5y |
| `Average_FWCI_5Y_NoSelfCits_Fractional` | double | Fractionally weighted mean FWCI_5y (no self-cits) |
| `First_year_in_scopus` | int | Earliest publication year in Scopus |
| `Most_recent_year_in_scopus` | int | Most recent publication year in Scopus |

---

## Notes

- FWCI values are sourced from `fca_ds.fwci_all_cits_and_nonself_cits_perc_{YYYYMMDD}`.
- Fractional counts allocate 1/n_authors weight per paper.
- Authors present in ANI but absent from FWCI table contribute 0 to FWCI averages.

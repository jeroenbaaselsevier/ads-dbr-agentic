# Author_Afid_Info

**Level:** Mapping (institutional_info)  
**Pipeline notebook:** `Auid_afid_by_eid.py`  
**Hive table:** `fca_ds.Author_Afid_Info_{YYYYMMDD}`  
**Path:** `/mnt/els/rads-main/mappings_and_metrics/mappings/institutional_info/snapshot_metrics/{YYYYMMDD}/Author_Afid_Info/`  
**Depends on:** `Institution`, `Institution_Afid`

---

## Description

Links each author (`auid`) on each article (`eid`) to their affiliation IDs
(`afid`) and the corresponding SciVal institution. One row per
(auid, eid, afid) combination.

---

## Load

```python
ani_stamp = '20250301'
path_loc = f'/mnt/els/rads-main/mappings_and_metrics/mappings/institutional_info/snapshot_metrics/{ani_stamp}/'
df = spark.read.parquet(path_loc + 'Author_Afid_Info')

# Or via Hive:
df = spark.table(f'fca_ds.Author_Afid_Info_{ani_stamp}')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `auid` | long | Scopus author ID |
| `eid` | long | Scopus article ID |
| `afid` | long | Scopus affiliation ID |
| `SV_Institution_ID` | long | SciVal Institution ID (null if afid not in mapping) |
| `SV_Institution_Name` | string | Institution name (null if not mapped) |

---

## Notes

- One row per (auid, eid, afid). An author on a paper with multiple afids gets
  multiple rows.
- `SV_Institution_ID` is null for afids not present in `Institution_Afid`.
- Useful for finding which institution an author was affiliated with on a specific
  paper without exploding the ANI `af` / `au` arrays manually.

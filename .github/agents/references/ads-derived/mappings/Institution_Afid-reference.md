# Institution_Afid

**Level:** Mapping (institutional_info)  
**Pipeline notebook:** `Institutions.py`  
**Hive table:** `fca_ds.Institution_Afid_{YYYYMMDD}`  
**Path:** `/mnt/els/rads-main/mappings_and_metrics/mappings/institutional_info/snapshot_metrics/{YYYYMMDD}/Institution_Afid/`

---

## Description

Mapping from Scopus affiliation IDs (`afid`) to SciVal Institution IDs.
Each `afid` maps to one institution. Used by Collaboration, Institutional
Self-Citations, and Author-Afid notebooks to resolve affiliation → institution.

---

## Load

```python
ani_stamp = '20250301'
path_loc = f'/mnt/els/rads-main/mappings_and_metrics/mappings/institutional_info/snapshot_metrics/{ani_stamp}/'
df_afid = spark.read.parquet(path_loc + 'Institution_Afid')

# Or via Hive:
df_afid = spark.table(f'fca_ds.Institution_Afid_{ani_stamp}')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `afid` | long | Scopus affiliation ID |
| `Institution_ID` | long | SciVal Institution ID (foreign key to `Institution.Institution_Id`) |
| `afid_name` | string | Affiliation name as stored in SciVal |

---

## Notes

- Source: SciVal `institutions.csv` (monthly snapshot from  
  `s3://com-elsevier-scival-tech-data/institution/`).
- Join pattern:
  ```python
  df_afid.join(df_inst, df_afid.Institution_ID == df_inst.Institution_Id)
  ```
- Not all Scopus `afid` values appear here — only those mapped by SciVal.

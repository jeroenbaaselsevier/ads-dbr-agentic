# Institution

**Level:** Mapping (institutional_info)  
**Pipeline notebook:** `Institutions.py`  
**Hive table:** `fca_ds.Institution_{YYYYMMDD}`  
**Path:** `/mnt/els/rads-main/mappings_and_metrics/mappings/institutional_info/snapshot_metrics/{YYYYMMDD}/Institution/`

---

## Description

SciVal institution metadata: ID, country, name, type, region and sector.
This is the master institution reference table used by the Collaboration,
Institutional Self-Citations, and Author-Afid notebooks.

---

## Load

```python
ani_stamp = '20250301'
path_loc = f'/mnt/els/rads-main/mappings_and_metrics/mappings/institutional_info/snapshot_metrics/{ani_stamp}/'
df_inst = spark.read.parquet(path_loc + 'Institution')

# Or via Hive:
df_inst = spark.table(f'fca_ds.Institution_{ani_stamp}')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `Institution_Id` | long | SciVal Institution ID |
| `Country` | string | ISO-3 country code (uppercase, e.g. `"GBR"`) |
| `Name` | string | Institution name |
| `instType` | string | Raw institution type from SciVal |
| `Region` | string | Geographic region |
| `Sector` | string | Bucketed sector: `Academic`, `Corporate`, `Government`, `Medical`, `Other` |

---

## Notes

- Source: SciVal `institution-metadata.json` (monthly snapshot from  
  `s3://com-elsevier-scival-tech-data/institution-metadata/`).
- `Sector` is derived from `instType`: values not in (academic, corporate,
  government, medical) map to `Other`.
- Country code is ISO-3 (3-letter), unlike ANI which often uses ISO-2.
- Paired with [Institution_Afid-reference.md](Institution_Afid-reference.md) to
  link Scopus affiliation IDs to institutions.

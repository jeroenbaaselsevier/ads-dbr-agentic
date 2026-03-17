# FWVI_All_cits (Field-Weighted View Impact)

**Level:** Publication  
**Pipeline notebook:** `FWVI_computation.py`  
**Hive table:** none (parquet only)  
**Path subfolder:** `FWVI_All_cits/`  
**Depends on:** `Usage_Data`

---

## Description

Field-Weighted View Impact (FWVI): analogous to FWCI but using article view
counts (abstract views + outward links) instead of citation counts. Computed
over three windows (4-year, 5-year, no window) using the same ASJC
fractionalization framework as FWCI.

FWVI > 1 means more views than the field/year average.

> **Note:** This table is written to parquet only — no Hive table is created.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('FWVI_All_cits')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `ASJC_count_4y` / `_5y` / `_NoWindow` | int | Number of ASJC subfields used for normalization, per window |
| `views_4y` / `_5y` / `_NoWindow` | double | Total views within the window |
| `Expected_4y` / `_5y` / `_NoWindow` | double | Expected views based on field/year average |
| `FWVI_4y` / `FWVI_5y` / `FWVI_NoWindow` | double | Field-Weighted View Impact |
| `FWVI_Type` | string | Literal `"Total_views"` |

---

## Notes

- View window: 4-year = views from ANI year up to ANI year + 3.
- Uses same ASJC fractionalization inverse-CPP framework as FWCI.
- Only articles/years/ASJC combinations with > 0 total views contribute to the
  expected-view calculation.
- `FWVI_4y` is the recommended variant for comparability.

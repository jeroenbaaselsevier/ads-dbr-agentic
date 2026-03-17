# genderized_auid (Genderize_Authors)

**Level:** Author (restricted)  
**Pipeline notebook:** `Genderize_Authors_New_Method.py`  
**Hive table:** `fca_ds.genderized_auid_{YYYYMMDD}`  
**Restricted path:** `/mnt/els/rads-restricted/namsor/genderized_auids/{YYYYMMDD}/genderized_auid/`  
**Depends on:** `Author_First_Last_Country_No_Ties`

---

## Description

Author gender inference using the NamSor name-country dictionary with a 0.85
probability threshold. A two-pass approach:
1. **First pass**: match by (first name, last name, country) then (first name, last name).
2. **Second pass**: for remaining unknowns, use first-name-only frequency among NamSor entries
   (≥ 90% genderized AND ≥ 99% consistent → assign that gender).

---

## Load

```python
# Via Hive (preferred)
df = spark.table(f'fca_ds.genderized_auid_{ani_stamp}')

# Access via snapshot_functions if available as author-level table
df = snapshot_functions.ads.author.get_table('genderized_auid')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `auid` | long | Scopus author ID |
| `first` | string | Cleaned first name (initials stripped, lowercase) |
| `last` | string | Cleaned last name (lowercase) |
| `country` | string | Country (uppercase ISO 2-letter) |
| `Gender_Name_Country` | string | Gender from name+country NamSor lookup |
| `Probability_Name_Country` | double | Probability for `Gender_Name_Country` |
| `Gender_Name_Only` | string | Gender from name-only NamSor lookup |
| `Probability_Name_Only` | double | Probability for `Gender_Name_Only` |
| `Gender` | string | Final gender: `male`, `female`, or `unknown` |

---

## Notes

- This table is stored in the **restricted** path and requires appropriate permissions.
- Default gender when no name available: `unknown`.
- The NamSor dictionary is an annual snapshot from `/mnt/els/rads-restricted/namsor/{year}/dictionary/`.
- Authors with null first names after cleaning are excluded.

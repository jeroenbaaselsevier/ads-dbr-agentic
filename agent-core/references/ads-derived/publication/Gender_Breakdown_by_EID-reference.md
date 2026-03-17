# Gender_breakdown_multiauthor (Gender_Breakdown_by_EID)

**Level:** Publication  
**Pipeline notebook:** `Gender_Breakdown_by_EID_excluding_single_author.py`  
**Hive table:** `fca_ds.gender_breakdown_multiauthored_eids_{YYYYMMDD}`  
**Table path:** `Gender_breakdown_multiauthor/`  
**Depends on:** `genderized_auid` (restricted NamSor path)

---

## Description

Per-article gender composition of the author list, for multi-author papers only
(single-author papers are excluded). Uses the pre-computed `genderized_auid`
table to resolve gender for each author.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Gender_breakdown_multiauthor')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `Total_authors` | int | Number of distinct authors (excludes nulls) |
| `Inferred_unknown_count` | int | Authors with unknown gender |
| `Inferred_male_count` | int | Authors inferred as male |
| `inferred_female_count` | int | Authors inferred as female (lowercase `i`) |
| `Total_authors_with_inferred_gender` | int | `Inferred_male_count + inferred_female_count` |
| `Fraction_with_inferred_gender` | double | `Total_authors_with_inferred_gender / Total_authors` |

---

## Notes

- Only articles with `Total_authors > 1` are present.
- Note the inconsistent capitalisation: `inferred_female_count` (lowercase) vs
  `Inferred_male_count` (uppercase).
- For journal-level gender statistics see
  [../author/gender_breakdown_by_source-reference.md](../author/gender_breakdown_by_source-reference.md).
- Gender assignment methodology: NamSor with 0.85 probability threshold — see
  [../author/Genderize_Authors-reference.md](../author/Genderize_Authors-reference.md).

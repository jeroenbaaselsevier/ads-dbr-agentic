# Author_First_Last_Country_No_Ties

**Level:** Author  
**Pipeline notebook:** `Pyspark_First_Last_Country_Exclude_Ties.py`  
**Hive table:** `fca_ds.Author_First_Last_Country_No_Ties_{YYYYMMDD}`

---

## Description

For each author, their preferred first and last name (longest non-initial name
from their publication history) and their inferred country of origin from their
first publication year. Country ties (multiple equally frequent countries) are
marked as `"Indeterminant"`.

Used as input for the NamSor gender genderization pipeline.

---

## Load

```python
df = snapshot_functions.ads.author.get_table('Author_First_Last_Country_No_Ties')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `auid` | long | Scopus author ID |
| `first_year` | int | First year the author appears in Scopus |
| `first` | string | Cleaned first name (initials stripped, lowercase) |
| `last` | string | Cleaned last name (lowercase) |
| `Country_of_Origin` | string | Lowercase ISO 2-letter country code, or `"Indeterminant"` if tied |

---

## Notes

- `"Indeterminant"` (note spelling) = two or more countries tied for most frequent
  in the author's first publication year.
- Name cleaning: a UDF selects the longest non-initial token from each author's
  list of first/last names across all papers.
- Used as input by [Genderize_Authors-reference.md](Genderize_Authors-reference.md).

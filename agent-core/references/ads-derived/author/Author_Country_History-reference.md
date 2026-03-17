# Author_Country_History

**Level:** Author  
**Pipeline notebook:** `Author_Country_History_NO_NULLS.py`  
**Hive table:** `fca_ds.Author_Country_History_{YYYYMMDD}`

---

## Description

Full history of each author's country of publication, aggregated by year. For
each author × year × country combination, stores publication count and earliest/
latest publication date. Useful for studying author mobility over time.

Country resolution priority:
1. IPR `country_tag` (most accurate)
2. ANI affiliation tag country
3. SciVal institution country (from `Institution_Afid` mapping)

---

## Load

```python
df = snapshot_functions.ads.author.get_table('Author_Country_History')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `auid` | long | Scopus author ID |
| `first_year_in_scopus` | int | Earliest publication year |
| `country_history` | array\<struct\> | Sorted array of country-year records (see below) |

### `country_history` struct fields

| Field | Type | Description |
|---|---|---|
| `sort_year` | int | Publication year |
| `earliest_datesort` | string | Earliest `datesort` value in this year |
| `latest_datesort` | string | Latest `datesort` value in this year |
| `country` | string | Country code (ISO 2-letter, lowercase) |
| `country_pubs` | long | Number of publications in this country × year |

---

## Notes

- Array is sorted by `sort_year` then `country`.
- Authors with no country information may have null entries omitted.
- One row per author (nested format); explode `country_history` for flat analysis:
  ```python
  from pyspark.sql import functions as F
  df_flat = df.select('auid', 'first_year_in_scopus', F.explode('country_history').alias('ch')) \
              .select('auid', 'first_year_in_scopus', 'ch.*')
  ```

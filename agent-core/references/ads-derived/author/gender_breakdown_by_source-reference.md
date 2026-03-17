# gender_breakdown_by_source

**Level:** Author / Source  
**Pipeline notebook:** `gender_breakdown_by_source.py`  
**Hive table:** none (parquet written to S3 Collibra bucket)  
**Output path:** `s3://com-elsevier-rads-collibra-prod/gender_breakdown_by_source/{YYYYMMDD}/`  
**Depends on:** `genderized_auid` (restricted)

---

## Description

Gender statistics per journal source for a rolling 6-year window plus the
full aggregate period. For each source, counts distinct authors and authorships
(author-paper appearances) broken down by inferred gender. Filters to journals
present in the current EDC source-profiles snapshot.

---

## Load

```python
df = spark.read.parquet(f's3://com-elsevier-rads-collibra-prod/gender_breakdown_by_source/{ani_stamp}/')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `srcid` | long | Scopus source ID |
| `sort_year_period` | string | Year (e.g. `"2024"`) or period (e.g. `"2018_2024"`) |
| `Total_publications` | long | Distinct article count |
| `Distinct_authors` | long | Distinct author count |
| `all_authors` | long | Same as `Distinct_authors` |
| `female_authors` | long | Distinct female authors |
| `male_authors` | long | Distinct male authors |
| `unknown_authors` | long | Distinct authors with unknown gender |
| `Total_authorships_by_auid_count` | long | Total (non-distinct) author-paper appearances |
| `all_authorships` | long | Same as `Total_authorships_by_auid_count` |
| `female_authorships` | long | Author-paper appearances for female authors |
| `male_authorships` | long | Author-paper appearances for male authors |
| `unknown_authorships` | long | Author-paper appearances for unknown gender |
| `Check_authorship_counts_sum_up` | boolean | `female + male + unknown = all` (quality check) |
| `Check_author_count_sums_up` | boolean | `female + male + unknown = all` (quality check) |
| `Percentage_male_authors` | double | % male of distinct authors |
| `Percentage_female_authors` | double | % female of distinct authors |
| `Percentage_male_authorships` | double | % male of all authorships |
| `Percentage_female_authorships` | double | % female of all authorships |

---

## Notes

- Each source appears twice per year: once as a single year and once as the
  rolling 6-year aggregate period.
- Filtered to sources with ≥ 1 article in the period.
- Restricted data dependency: `genderized_auid` uses NamSor (restricted access).

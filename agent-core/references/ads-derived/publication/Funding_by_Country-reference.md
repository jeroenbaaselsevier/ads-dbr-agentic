# Funding_by_Country (Funding_per_EID)

**Level:** Publication  
**Pipeline notebook:** `Funding_by_Country.py`  
**Hive table:** `fca_ds.funding_per_eid_{YYYYMMDD}`  
**Path subfolder:** `Funding_per_EID/data/`

---

## Description

Per-article funding body information extracted from the ANI `funding_list`
column, enriched with funder name and country from the Elsevier funders
taxonomy. Each article can have multiple rows (one per funding body).

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Funding_per_EID/data')
# Or via Hive:
df = spark.table(f'fca_ds.funding_per_eid_{ani_stamp}')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `fundingbodyid` | string | Funder ID (extracted from ANI `agency_id` URI) |
| `Funding_body_name` | string | English preferred name of the funding body |
| `fundingbody_country` | string | Country of the funding body |
| `financetype` | string | Finance type (e.g. `"contract"`, `"grant"`) |

---

## Notes

- One row per (EID, fundingbodyid). Articles with multiple funders have multiple rows.
- Articles with no funding information are absent.
- Funder taxonomy source: `/mnt/els/edc/funders-taxonomy-parquet` (latest monthly snapshot).
- `Funding_body_name` is the English `preferredName`; null if not in taxonomy.

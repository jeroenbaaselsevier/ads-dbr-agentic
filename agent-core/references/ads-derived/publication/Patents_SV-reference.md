# Patents_SV (patent_data_sv_aligned_updated)

**Level:** Publication  
**Pipeline notebook:** `Patents_SV_Method_New_method_production.py`  
**Hive table:** `fca_ds.patent_data_sv_aligned_updated_{YYYYMMDD}`  
**Path subfolder:** `patent_data_sv_aligned_updated/data/`

---

## Description

Links Scopus article EIDs to patent records using SciVal's patent-to-Scopus
mapping. Enriched with patent metadata (publication date, patent office, family
IDs). Aligns with the SciVal patent citation methodology.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('patent_data_sv_aligned_updated/data')
# Or via Hive:
df = spark.table(f'fca_ds.patent_data_sv_aligned_updated_{ani_stamp}')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `sort_year` | int | Article publication year |
| `date_pr` | string | Patent publication date (string, format varies) |
| `patent_publication_year` | long | Year extracted from `date_pr` |
| `patent_id` | string | Patent identifier (FileID from SciVal mapping) |
| `patent_office` | string | Patent office code (e.g. `"US"`, `"EP"`) |
| `family_id_domestic` | long | Domestic patent family ID |
| `family_id_complete` | long | Complete international patent family ID |
| `family_id_main` | long | Main patent family ID |

---

## Notes

- One row per (EID, patent). Articles cited by multiple patents have multiple rows.
- Source mapping: `s3://com-elsevier-scival-tech-data/xfab_patent_scopus_mapping` (CSV).
- Patent metadata: `sccontent-patent-ura-parsed-parquet-prod` S3 bucket.
- For counting patents per article, use `countDistinct('patent_id')` or aggregate by
  family ID for patent-family-level counts.

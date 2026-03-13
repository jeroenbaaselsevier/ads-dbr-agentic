# Usage_Data

**Level:** Publication  
**Pipeline notebook:** `Usage_Data.py`  
**Hive table:** `fca_ds.Usage_Data_{YYYYMMDD}`

---

## Description

Article-level view and outward-link counts sourced from SciVal's monthly usage
data files (`document-view-counts` S3 bucket). The pipeline picks the most
recent monthly file available. Raw data — no transformations beyond schema
enforcement.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Usage_Data')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `usage_year` | int | Year the views/clicks were recorded |
| `abstract_views` | double | Number of abstract page views |
| `outward_links` | double | Number of outward link clicks (full-text clicks) |

---

## Notes

- Source data: `s3://com-elsevier-scival-tech-data/document-view-counts/`.
- One row per (EID, usage_year). Years with zero usage are absent (sparse).
- Used as input for FWVI computation — see [FWVI-reference.md](FWVI-reference.md).
- `abstract_views + outward_links` approximates total usage.

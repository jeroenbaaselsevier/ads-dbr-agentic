# FWCI_All_cits_and_non_self_cits_perc

**Level:** Publication  
**Pipeline notebook:** `FWCI_normal_and_no_self_cit_cleaned.py`  
**Hive table:** `fca_ds.fwci_all_cits_and_nonself_cits_perc_{YYYYMMDD}`

---

## Description

Field-Weighted Citation Impact (FWCI) for each article, computed over three
citation windows (4-year, 5-year, no window) for both all citations and
citations excluding author self-citations. Also includes FWCI percentile ranks.

FWCI = actual citations / expected citations, where expected is the harmonic
mean of fractional citation counts across the article's ASJC subfields.
FWCI > 1 means above-average for its field/year.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('FWCI_All_cits_and_non_self_cits_perc')
```

---

## Columns

### Key

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `sort_year` | int | Publication year |
| `count_docs` | long | Articles in the same year/field cohort |

### All-citations columns (3 windows)

| Column | Description |
|---|---|
| `Citations_4y` | Raw citations within 4-year window |
| `Citations_5y` | Raw citations within 5-year window |
| `Citations_NoWindow` | Total citations (no window) |
| `ASJC_count_4y` / `_5y` / `_NoWindow` | Number of ASJC subfields used for normalization |
| `Expected_4y` / `_5y` / `_NoWindow` | Expected citations based on field/year average |
| `FWCI_4y` / `FWCI_5y` / `FWCI_NoWindow` | Field-Weighted Citation Impact |
| `count_FWCI_4y` / `_5y` / `_NoWindow` | Number of eligible docs in percentile cohort |
| `perc_Citations_4y` / `_5y` / `_NoWindow` | Citations percentile (0–100) |
| `perc_FWCI_4y` / `_5y` / `_NoWindow` | FWCI percentile (0–100) |

### No-self-citations columns (suffix `_NoSelfCits`)

Same structure as above for each of the 3 windows:  
`Citations_4y_NoSelfCits`, `ASJC_count_4y_NoSelfCits`, `Expected_4y_NoSelfCits`,
`FWCI_4y_NoSelfCits`, `count_FWCI_4y_NoSelfCits`, `perc_Citations_4y_NoSelfCits`,
`perc_FWCI_4y_NoSelfCits` … (likewise for `_5y_` and `_NoWindow_`)

---

## Notes

- Uses standard ASJC classification. For reclassified ASJC (ASJC=1000 override) see
  [FWCI_recl_All-reference.md](FWCI_recl_All-reference.md).
- All source tables include preprints (SCOPUS+MEDL filter, not `nopp()`).
- `FWCI_4y` is the most-used variant for SciVal comparability.

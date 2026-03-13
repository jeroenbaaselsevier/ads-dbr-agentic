# FWCI_recl_All_cits_and_non_self_cits_perc

**Level:** Publication  
**Pipeline notebook:** `FWCI_recl_normal_and_no_self_cit_cleaned.py`  
**Hive table:** `fca_ds.FWCI_recl_All_cits_and_non_self_cits_perc_{YYYYMMDD}`  
**Depends on:** `article_reclassified`

---

## Description

Same as `FWCI_All_cits_and_non_self_cits_perc` but uses **reclassified ASJC
codes** for articles that have only the general/multidisciplinary code
`1000`. Articles with ASJC=`[1000]` alone are reassigned to the most common
ASJC among their citing and cited papers (see
[article_reclassification-reference.md](article_reclassification-reference.md)).

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('FWCI_recl_All_cits_and_non_self_cits_perc')
```

---

## Columns

Same structure as [FWCI_All-reference.md](FWCI_All-reference.md), with two
differences:

1. **No-self-citation suffix is `_ns`** (not `_NoSelfCits`)  
   e.g. `Citations_4y_ns`, `FWCI_4y_ns`, `perc_FWCI_4y_ns`

2. All citation/FWCI/percentile columns exist for the same 3 windows:  
   `_4y`, `_5y`, `_NoWindow` (all citations) and `_4y_ns`, `_5y_ns`, `_NoWindow_ns` (no self-cits)

---

## When to use this vs FWCI_All

- Use `FWCI_recl_*` when comparing against SciVal FWCI metrics — SciVal uses
  reclassification for general-science journals.
- Use `FWCI_All_*` for full-corpus FWCI without reclassification.

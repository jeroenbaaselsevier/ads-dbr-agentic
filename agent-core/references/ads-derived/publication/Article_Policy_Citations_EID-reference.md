# Article_Policy_Citations_EID

**Level:** Publication  
**Pipeline notebook:** `Article_Policy_Citations_EID.py`  
**Hive table:** `fca_ds.Article_Policy_Citations_EID_{YYYYMMDD}`  
**Depends on:** PlumX data (same source as `Article_PlumX_Metrics`)

---

## Description

Per-article count of policy citations from PlumX data
(source = `"policy citation"`, category = `"citation"`). A policy citation is
a reference to a Scopus article by a policy document. Articles without a DOI
get null values.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Policy_Citations_EID')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `Total_Policy_Citations` | long | Number of policy citations received; null if no DOI |
| `Cited_at_least_once` | int | 1 if ≥ 1 policy citation, 0 if none, null if no DOI |

---

## Notes

- Input: PlumX `metrics` where `source = 'policy citation'` and `category = 'citation'`.
- For ASJC-normalized policy citation score see
  [Article_Policy_Citations_ASJC-reference.md](Article_Policy_Citations_ASJC-reference.md).
- For SM-BID-normalized score see
  [Article_Policy_Citations_SM_BID-reference.md](Article_Policy_Citations_SM_BID-reference.md).

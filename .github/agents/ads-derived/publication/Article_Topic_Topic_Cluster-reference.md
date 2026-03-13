# Article_Topic_Topic_Cluster

**Level:** Publication  
**Pipeline notebook:** `EID_Topic_Topic_Cluster.py`  
**Hive table:** `fca_ds.Article_Topic_Topic_Cluster_{YYYYMMDD}`

---

## Description

Maps each Scopus article to a SciVal Topic and Topic Cluster using the direct
SciVal topic-to-EID mapping files. Topics are fine-grained research clusters
(~96 000; ~1 500 topic clusters as of 2025). Each article is assigned to a
single topic.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Topic_Topic_Cluster')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `TopicID` | long | SciVal Topic ID |
| `Topic_Keywords` | array\<string\> | Representative keywords for the topic |
| `Cluster` | long | Topic Cluster ID (grouping of related topics) |
| `Cluster_Keywords` | array\<string\> | Representative keywords for the cluster |

---

## Notes

- This table reads SciVal source files directly (not `snapshot_functions.scival`).
  It may differ slightly from the `topic_eid` SciVal table in snapshot timing.  
- Not all articles have a topic assignment (null rows excluded).
- For topic prominence scores see the SciVal `topic_prominence` table via  
  `snapshot_functions.scival.get_table('topic_prominence')`.

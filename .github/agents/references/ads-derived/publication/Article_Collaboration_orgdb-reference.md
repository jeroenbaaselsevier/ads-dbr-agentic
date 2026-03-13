# Article_Collaboration_orgdb

**Level:** Publication  
**Pipeline notebook:** `Article_Collaboration_orgdb.py`  
**Hive table:** `fca_ds.ani_eid_orgdb_collab_{YYYYMMDD}`

---

## Description

Same as `Article_Collaboration` but uses **OrgDB** (not SciVal institution
metadata) for afid → organisation mapping. Organisations are resolved via
affiliation IDs to OrgDB `org_id`, with country and sector from OrgDB.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Collaboration_orgdb')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `eid` | long | Scopus article ID |
| `docUniqueAuidCount` | int | Number of distinct authors |
| `org` | array\<struct\> | One element per OrgDB organisation on the paper (see below) |
| `docAfCount` | int | Number of distinct affiliations |
| `DocCountryCount` | int | Number of distinct countries |
| `CollaborationLevel` | string | `SINGLE_AUTHOR`, `INSTITUTIONAL`, `NATIONAL`, `INTERNATIONAL`, `INDETERMINATE` |
| `Acad` | boolean | Has at least one Academic organisation |
| `Corp` | boolean | Has at least one Corporate organisation |
| `Govt` | boolean | Has at least one Government organisation |
| `Med` | boolean | Has at least one Medical organisation |
| `Other` | boolean | Has at least one Other organisation |

### `org` struct fields

| Field | Type | Description |
|---|---|---|
| `org_id` | long | OrgDB organisation ID |
| `name` | string | Organisation name |
| `country` | string | Country |
| `sector` | string | Sector: Academic / Corporate / Government / Medical / Other |
| `auids` | array\<long\> | Author IDs affiliated with this org on this paper |
| `docAfAuidcount` | int | Number of those authors |

---

## Notes

- OrgDB-based: use this when you need to join to OrgDB hierarchy or when
  comparing to other OrgDB-based datasets.
- For SciVal institution-based collaboration, use [Article_Collaboration-reference.md](Article_Collaboration-reference.md).

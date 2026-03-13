# Article_Collaboration

**Level:** Publication  
**Pipeline notebook:** `Article_Collaboration.py`  
**Hive table:** `fca_ds.ani_eid_afinst_collab_{YYYYMMDD}`

---

## Description

Collaboration type and institutional breakdown per article, resolved via
SciVal institution metadata (not OrgDB). Each article gets a `CollaborationLevel`
label and sector flags based on the distinct institutions and countries that
appear in the author affiliation list.

---

## Load

```python
df = snapshot_functions.ads.publication.get_table('Article_Collaboration')
```

---

## Columns

| Column | Type | Description |
|---|---|---|
| `EID` | long | Scopus article ID |
| `docUniqueAuidCount` | int | Number of distinct authors |
| `m_af` | array\<struct\> | One element per institution on the paper (see below) |
| `docAfCount` | int | Number of distinct affiliations |
| `DocCountryCount` | int | Number of distinct countries |
| `CollaborationLevel` | string | `SINGLE_AUTHOR`, `INSTITUTIONAL`, `NATIONAL`, `INTERNATIONAL`, `INDETERMINATE` |
| `Acad` | boolean | Has at least one Academic institution |
| `Corp` | boolean | Has at least one Corporate institution |
| `Govt` | boolean | Has at least one Government institution |
| `Med` | boolean | Has at least one Medical institution |
| `Other` | boolean | Has at least one Other institution |

### `m_af` struct fields

| Field | Type | Description |
|---|---|---|
| `m_af_id` | long | SciVal Institution ID |
| `m_af_id_is_scival` | boolean | Whether the ID is a SciVal institution |
| `m_af_name` | string | Institution name |
| `m_af_country` | string | Country (ISO-3 uppercase) |
| `m_af_sector` | string | Sector: Academic / Corporate / Government / Medical / Other |
| `auids` | array\<long\> | Author IDs affiliated with this institution on this paper |
| `docAfAuidcount` | int | Number of those authors |

---

## Notes

- Uses SciVal institution mapping. For OrgDB-based collaboration see
  [Article_Collaboration_orgdb-reference.md](Article_Collaboration_orgdb-reference.md).
- `INDETERMINATE` means afids present but could not be resolved to institutions.
- Sector flags are non-exclusive — a paper can be both `Acad=true` and `Corp=true`.

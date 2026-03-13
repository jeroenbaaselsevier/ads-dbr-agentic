# ADS Derived Metrics — Reference Overview

This folder contains per-table reference docs for every table produced by the
**ADS derived metrics pipeline** (`rads-derived-metrics-code`). The pipeline
runs on a Databricks monthly job and writes Parquet + Hive tables from each
Scopus ANI snapshot.

---

## Accessing tables

```python
import snapshot_functions

# List available publication-level tables
snapshot_functions.ads.publication.list()

# Load a table (latest snapshot)
df = snapshot_functions.ads.publication.get_table('Article_Citations_Total_All_and_NonSelf_Citations')

# Author-level
df = snapshot_functions.ads.author.get_table('Author_Info_and_H_Index')
```

Hive tables are also accessible directly:
```python
df = spark.table(f'fca_ds.{table_name}_{ani_stamp}')   # e.g. fca_ds.fwci_all_cits_and_nonself_cits_perc_20250301
```

---

## Output paths

| Level | Path |
|---|---|
| Publication-level | `/mnt/els/rads-main/mappings_and_metrics/bibliometrics/publication_level/snapshot_metrics/<YYYYMMDD>/<TableName>/` |
| Author-level | `/mnt/els/rads-main/mappings_and_metrics/bibliometrics/author_level/snapshot_metrics/<YYYYMMDD>/<TableName>/` |
| Institutional mappings | `/mnt/els/rads-main/mappings_and_metrics/mappings/institutional_info/snapshot_metrics/<YYYYMMDD>/` |
| Restricted (gender) | `/mnt/els/rads-restricted/namsor/genderized_auids/<YYYYMMDD>/` |
| Gender breakdown by source | `s3://com-elsevier-rads-collibra-prod/gender_breakdown_by_source/<YYYYMMDD>/` |
| SM classification | `fca_ds.smcs_mapping_model_tfidf_logreg_2021_ani_up_to_<YYYYMMDD>` (Hive only) |

---

## ANI filter note

The ADS pipeline uses a slightly different ANI filter than analyst notebooks:

```python
# ADS pipeline (includes preprints — all SCOPUS+MEDL)
func.arrays_overlap(func.col('dbcollections'), func.array(func.lit("SCOPUS"), func.lit("MEDL")))

# Analyst notebooks (excludes preprints)
column_functions.nopp()
```

When joining ADS outputs to ANI subsets filtered with `nopp()`, you may see
articles in ADS tables that are excluded from your ANI subset.

---

## Pipeline task graph (abbreviated)

```
Define_Snapshot
├── Article_PlumX_Data_Normalized
│   └── Article_Policy_Citations_EID
│       └── Article_Policy_Citations_ASJC
├── Article_Research_Levels
├── Article_Collaboration
├── Article_Collaboration_orgdb
├── Funding_by_Country
├── Institutions
│   ├── Auid_afid_by_eid
│   ├── Author_Country_History_NO_NULLS
│   └── Pyspark_First_Last_Country_Exclude_Ties
│       └── Genderize_Authors_New_Method
│           └── gender_breakdown_by_source
├── Institutional_Self_Citations
├── Patents_SV_Method_New_method_production
├── sm_classifier_tfidf_logreg_v1
│   ├── Article_Policy_Citations_SM_BID
│   ├── Multidisciplinarity_Master_Scopus_ani
│   └── Transdisciplinary_Master_all_scopus_ani
├── Usage_Data
│   └── FWVI_computation
├── Article_Citation_Year_One_Table
│   └── Citation_Percentiles
├── article_reclassification
│   └── FWCI_recl_normal_and_no_self_cit_cleaned
├── EID_Topic_Topic_Cluster
└── FWCI_normal_and_no_self_cit_cleaned
    └── H_Index_One_Table_From_Ani
```

Source code: `rads_metrics_code/` (run `./sync_metrics_code.sh` to update).

---

## Table index

### Publication-level

| Table name | Reference doc |
|---|---|
| `Article_Citations_Total_All_and_NonSelf_Citations` | [publication/Article_Citations_Total-reference.md](publication/Article_Citations_Total-reference.md) |
| `Article_Citations_by_Year_All_and_NonSelf_Citations` | [publication/Article_Citations_by_Year-reference.md](publication/Article_Citations_by_Year-reference.md) |
| `Article_Citations_Perc_Total` | [publication/Article_Citations_Perc_Total-reference.md](publication/Article_Citations_Perc_Total-reference.md) |
| `Article_Citations_Perc_ASJC27` | [publication/Article_Citations_Perc_ASJC27-reference.md](publication/Article_Citations_Perc_ASJC27-reference.md) |
| `Article_Citations_Perc_ASJC334` | [publication/Article_Citations_Perc_ASJC334-reference.md](publication/Article_Citations_Perc_ASJC334-reference.md) |
| `FWCI_All_cits_and_non_self_cits_perc` | [publication/FWCI_All-reference.md](publication/FWCI_All-reference.md) |
| `FWCI_recl_All_cits_and_non_self_cits_perc` | [publication/FWCI_recl_All-reference.md](publication/FWCI_recl_All-reference.md) |
| `Article_Collaboration` | [publication/Article_Collaboration-reference.md](publication/Article_Collaboration-reference.md) |
| `Article_Collaboration_orgdb` | [publication/Article_Collaboration_orgdb-reference.md](publication/Article_Collaboration_orgdb-reference.md) |
| `Article_Research_Levels` | [publication/Article_Research_Levels-reference.md](publication/Article_Research_Levels-reference.md) |
| `Article_Topic_Topic_Cluster` | [publication/Article_Topic_Topic_Cluster-reference.md](publication/Article_Topic_Topic_Cluster-reference.md) |
| `MultDisc/p_multidisciplinarity_*` | [publication/Multidisciplinarity-reference.md](publication/Multidisciplinarity-reference.md) |
| `Institutional_Self_Citations` | [publication/Institutional_Self_Citations-reference.md](publication/Institutional_Self_Citations-reference.md) |
| `Usage_Data` | [publication/Usage_Data-reference.md](publication/Usage_Data-reference.md) |
| `Article_PlumX_Metrics` | [publication/Article_PlumX_Metrics-reference.md](publication/Article_PlumX_Metrics-reference.md) |
| `Article_Policy_Citations_EID` | [publication/Article_Policy_Citations_EID-reference.md](publication/Article_Policy_Citations_EID-reference.md) |
| `Article_Policy_Citations_ASJC` | [publication/Article_Policy_Citations_ASJC-reference.md](publication/Article_Policy_Citations_ASJC-reference.md) |
| `Article_Policy_Citations_SM_BID` | [publication/Article_Policy_Citations_SM_BID-reference.md](publication/Article_Policy_Citations_SM_BID-reference.md) |
| `Funding_per_EID` | [publication/Funding_by_Country-reference.md](publication/Funding_by_Country-reference.md) |
| `transdisciplinarity/scopus_ani_transdisciplinarity_*` | [publication/Transdisciplinary-reference.md](publication/Transdisciplinary-reference.md) |
| `patent_data_sv_aligned_updated/data` | [publication/Patents_SV-reference.md](publication/Patents_SV-reference.md) |
| `FWVI_All_cits` | [publication/FWVI-reference.md](publication/FWVI-reference.md) |
| `article_reclassified` | [publication/article_reclassification-reference.md](publication/article_reclassification-reference.md) |
| `smcs_mapping_model_tfidf_logreg_2021_ani_up_to_*` | [publication/SM_classification-reference.md](publication/SM_classification-reference.md) |
| `Gender_breakdown_multiauthor` | [publication/Gender_Breakdown_by_EID-reference.md](publication/Gender_Breakdown_by_EID-reference.md) |

### Author-level

| Table name | Reference doc |
|---|---|
| `Author_Info_and_H_Index` | [author/Author_Info_and_H_Index-reference.md](author/Author_Info_and_H_Index-reference.md) |
| `Author_First_Last_Country_No_Ties` | [author/Author_First_Last_Country_No_Ties-reference.md](author/Author_First_Last_Country_No_Ties-reference.md) |
| `Author_Country_History` | [author/Author_Country_History-reference.md](author/Author_Country_History-reference.md) |
| `genderized_auid` | [author/Genderize_Authors-reference.md](author/Genderize_Authors-reference.md) |
| `gender_breakdown_by_source` | [author/gender_breakdown_by_source-reference.md](author/gender_breakdown_by_source-reference.md) |

### Mappings (institutional_info path)

| Table name | Reference doc |
|---|---|
| `Institution` | [mappings/Institution-reference.md](mappings/Institution-reference.md) |
| `Institution_Afid` | [mappings/Institution_Afid-reference.md](mappings/Institution_Afid-reference.md) |
| `Author_Afid_Info` | [mappings/Author_Afid_Info-reference.md](mappings/Author_Afid_Info-reference.md) |

---

## Keeping docs in sync

The source notebooks live at `rads_metrics_code/` (local mirror, gitignored).
Run `./sync_metrics_code.sh` to pull the latest version.

If a column described here is missing from actual data, or a metric behaves
unexpectedly, check the corresponding notebook in `rads_metrics_code/` — the
docs may be out of date.

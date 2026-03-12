# Databricks notebook source

# COMMAND ----------
import sys
sys.path.append('/Workspace/rads/library/')
import snapshot_functions
from pyspark.sql import functions as F

# COMMAND ----------
# ── METADATA ─────────────────────────────────────────────────────────────────
meta_snapshots = snapshot_functions.patents.metadata.list_snapshots()
print(f"Patents metadata snapshot count: {len(meta_snapshots)}")
print(f"  Earliest: {meta_snapshots[0]}")
print(f"  Latest:   {meta_snapshots[-1]}")
print(f"  All: {meta_snapshots}")

# COMMAND ----------
# Load latest metadata snapshot
df_meta = snapshot_functions.patents.metadata.get_table()
print(f"Row count: {df_meta.count()}")
df_meta.printSchema()
df_meta.show(2)

# COMMAND ----------
# Sample top-level columns (some may be structs/arrays — show types)
print("Top-level columns:")
for f in df_meta.schema.fields:
    print(f"  {f.name}: {f.dataType}")

# COMMAND ----------
# Key stats
# Count non-null patent_id (or equivalent patent key)
key_col = [c for c in df_meta.columns if 'patent' in c.lower() and 'id' in c.lower()]
print(f"Potential patent key columns: {key_col}")
if key_col:
    print(f"Distinct {key_col[0]}: {df_meta.select(key_col[0]).distinct().count()}")

# COMMAND ----------
# Country / office distribution if available
office_cols = [c for c in df_meta.columns if 'office' in c.lower() or 'country' in c.lower() or 'cc' == c.lower()]
print(f"Country/office columns: {office_cols}")
if office_cols:
    df_meta.groupBy(office_cols[0]).count().orderBy(F.desc('count')).show(20)

# COMMAND ----------
# Year distribution if available
year_cols = [c for c in df_meta.columns if 'year' in c.lower() or 'date' in c.lower() or 'appln' in c.lower()]
print(f"Year-like columns: {year_cols}")

# COMMAND ----------
# citation_nplcit presence
if 'citation_nplcit' in df_meta.columns:
    n_with_npl = df_meta.filter(F.size('citation_nplcit') > 0).count()
    n_total = df_meta.count()
    print(f"Patents with >=1 NPL citation: {n_with_npl} / {n_total} ({100*n_with_npl/n_total:.1f}%)")
    # explode and count total NPL citations
    df_npl_exploded = df_meta.select(F.explode('citation_nplcit').alias('npl'))
    print(f"Total NPL citation entries: {df_npl_exploded.count()}")
    print("NPL citation struct fields:")
    df_npl_exploded.printSchema()
else:
    print("No citation_nplcit column found")

# COMMAND ----------
# ── NPL CITATIONS SCOPUS CSV ─────────────────────────────────────────────────
npl_snapshots = snapshot_functions.patents.npl_citations_scopus.list_snapshots()
print(f"NPL-Scopus mapping snapshot count: {len(npl_snapshots)}")
print(f"  Earliest: {npl_snapshots[0]}")
print(f"  Latest:   {npl_snapshots[-1]}")
print(f"  All: {npl_snapshots}")

# COMMAND ----------
df_npl = snapshot_functions.patents.npl_citations_scopus.get_table()
print(f"NPL-Scopus row count: {df_npl.count()}")
df_npl.printSchema()
df_npl.show(5)

# COMMAND ----------
# How many distinct patents cite Scopus papers?
print(f"Distinct patent_id: {df_npl.select('patent_id').distinct().count()}")
print(f"Distinct Scopus EIDs cited: {df_npl.select('eid').dropna().distinct().count()}")

# COMMAND ----------
# join_npl_citations: count matched rows
df_joined = snapshot_functions.patents.join_npl_citations()
print(f"Joined NPL rows (with or without Scopus EID match): {df_joined.count()}")
matched = df_joined.filter(F.col('eid').isNotNull()).count()
print(f"  With Scopus EID match: {matched}")

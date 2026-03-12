# Databricks notebook source

# COMMAND ----------
import sys
sys.path.append('/Workspace/rads/library/')
import snapshot_functions
from pyspark.sql import functions as F

# COMMAND ----------
# Available snapshots
snapshots = snapshot_functions.sdg.list_snapshots()
print(f"SDG snapshot count: {len(snapshots)}")
print(f"  Earliest: {snapshots[0]}")
print(f"  Latest:   {snapshots[-1]}")
print(f"  All: {snapshots}")

# COMMAND ----------
# Load latest snapshot
df_sdg = snapshot_functions.sdg.get_table()
print(f"Row count: {df_sdg.count()}")
df_sdg.printSchema()
df_sdg.show(5)

# COMMAND ----------
# SDG distribution
df_sdg.groupBy('sdg').count().orderBy('sdg').show(20)

# COMMAND ----------
# Confidence distribution
df_sdg.select(
    F.min('confidence').alias('min'),
    F.max('confidence').alias('max'),
    F.avg('confidence').alias('mean'),
    F.percentile_approx('confidence', 0.5).alias('median'),
    F.percentile_approx('confidence', 0.25).alias('p25'),
    F.percentile_approx('confidence', 0.75).alias('p75'),
).show()

# COMMAND ----------
# Distinct EIDs assigned vs total rows (papers can have multiple SDGs)
print(f"Distinct EIDs: {df_sdg.select('eid').distinct().count()}")
print(f"Total rows:    {df_sdg.count()}")

# COMMAND ----------
# SDG labels from static_data
df_labels = snapshot_functions.sdg.get_labels()
print("SDG labels:")
df_labels.orderBy('sdg_id').show(20, truncate=False)

# COMMAND ----------
# Multi-SDG: how many papers have >1 SDG?
df_per_paper = df_sdg.groupBy('eid').agg(F.count('sdg').alias('n_sdg'))
df_per_paper.groupBy('n_sdg').count().orderBy('n_sdg').show(10)

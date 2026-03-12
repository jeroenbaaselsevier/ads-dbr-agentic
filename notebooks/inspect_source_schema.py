# Databricks notebook source

# COMMAND ----------
# Inspect snapshot_functions.source — Scopus Source Profiles

import sys
sys.path.append('/Workspace/rads/library/')
import snapshot_functions
from pyspark.sql import functions as F
import json

# List available snapshots
snapshots = snapshot_functions.source.list_snapshots()
print(f"Available source snapshots: {len(snapshots)}")
print(f"First: {snapshots[0]}, Last: {snapshots[-1]}")
print(f"Recent: {snapshots[-5:]}")

# COMMAND ----------
# Load latest snapshot (EDC / raw format)
df_src = snapshot_functions.source.get_table()
count = df_src.count()
print(f"\nRow count: {count:,}")
print()
df_src.printSchema()

# COMMAND ----------
# Sample one row as JSON to understand nested struct shapes
sample = df_src.limit(1).toJSON().collect()
if sample:
    print(json.dumps(json.loads(sample[0]), indent=2, default=str))

# COMMAND ----------
# Column-level type summary
def describe_schema(schema, prefix=""):
    lines = []
    for field in schema.fields:
        ftype = field.dataType
        name = f"{prefix}{field.name}"
        lines.append(f"{name}: {ftype.simpleString()}")
        if hasattr(ftype, 'fields'):
            lines.extend(describe_schema(ftype, prefix=name + "."))
    return lines

print("\n".join(describe_schema(df_src.schema)))

# COMMAND ----------
# sourceType distribution
print("=== sourceType distribution ===")
df_src.groupBy('sourceType').count().orderBy(F.desc('count')).show(20, truncate=False)

# COMMAND ----------
# isActive distribution
print("=== isActive distribution ===")
df_src.groupBy('isActive').count().orderBy(F.desc('count')).show(10, truncate=False)

# COMMAND ----------
# databases values (what index DB collections appear)
print("=== databases distribution ===")
df_src.select(F.explode('databases').alias('db')).groupBy('db').count().orderBy(F.desc('count')).show(20, truncate=False)

# COMMAND ----------
# openaccess_status / openaccessstatus distribution
oa_col = 'openaccessstatus' if 'openaccessstatus' in df_src.columns else 'openaccess_status'
print(f"=== {oa_col} distribution ===")
df_src.groupBy(oa_col).count().orderBy(F.desc('count')).show(20, truncate=False)

# COMMAND ----------
# Show sample rows: id, sourcetitle, sourceType, isActive
print("=== Sample sources ===")
df_src.select('id', 'sourcetitle', 'sourceType', 'isActive').show(20, truncate=False)

# COMMAND ----------
# Check coverage against ANI: how many distinct ANI srcids appear in source profiles?
ani_stamp = '20260301'
import column_functions

df_ani_srcids = (
    spark.table(f'scopus.ani_{ani_stamp}')
    .filter(column_functions.nopp())
    .select(F.col('source.srcid').alias('srcid'))
    .filter(F.col('srcid').isNotNull())
    .distinct()
)

df_source_ids = df_src.select(F.col('id').alias('srcid')).filter(F.col('srcid').isNotNull()).distinct()

ani_count = df_ani_srcids.count()
matched = df_ani_srcids.join(df_source_ids, 'srcid', 'inner').count()
unmatched = ani_count - matched

print(f"Distinct ANI source IDs (core content): {ani_count:,}")
print(f"Found in source profiles:               {matched:,}  ({100*matched/ani_count:.1f}%)")
print(f"Missing from source profiles:           {unmatched:,} ({100*unmatched/ani_count:.1f}%)")
print()
print("(Missing = small/new journals, book series, conference proceedings not yet in source browse)")

# COMMAND ----------
# Show top fields from a few sample sources with metrics
print("=== Sample with metrics ===")
df_src.select(
    'id', 'sourcetitle', 'sourceType', 'issn',
    F.col('publisher.name').alias('publisher'),
    F.size('metrics').alias('n_metrics'),
    F.size('classification').alias('n_classifications'),
    F.size('coverage').alias('n_coverage_entries'),
).orderBy(F.desc('n_metrics')).show(10, truncate=False)

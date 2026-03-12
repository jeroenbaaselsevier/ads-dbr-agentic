# Databricks notebook source

# COMMAND ----------
# Inspect scopus.apr_20260301 — Author Profile Records
# Outputs: printSchema, row count, column list with types

apr_stamp = '20260301'
df_apr = spark.table(f'scopus.apr_{apr_stamp}')

print(f"=== scopus.apr_{apr_stamp} ===")
print(f"Row count: {df_apr.count()}")
print()

# COMMAND ----------
df_apr.printSchema()

# COMMAND ----------
# Show first row to understand nested struct shapes
import json
from pyspark.sql import functions as F

# Collect one row as JSON for inspection
sample = df_apr.limit(1).toJSON().collect()
if sample:
    import json
    parsed = json.loads(sample[0])
    print(json.dumps(parsed, indent=2, default=str))

# COMMAND ----------
# Column summary: names + spark type strings
from pyspark.sql.types import StructType

def describe_schema(schema, prefix=""):
    lines = []
    for field in schema.fields:
        ftype = field.dataType
        name = f"{prefix}{field.name}"
        lines.append(f"{name}: {ftype.simpleString()}")
        if hasattr(ftype, 'fields'):
            lines.extend(describe_schema(ftype, prefix=name + "."))
    return lines

print("\n".join(describe_schema(df_apr.schema)))

# COMMAND ----------
# Check overlap between APR auids and a sample of ANI auids
# (how many ANI auids are missing from APR?)
ani_stamp = '20260301'
df_ani = spark.table(f'scopus.ani_{ani_stamp}')

import column_functions
df_ani_core = df_ani.filter(column_functions.nopp())

# Sample: get distinct auids from first 100k ANI rows
df_ani_auids = (
    df_ani_core
    .limit(100000)
    .select(F.explode('Au.auid').alias('auid'))
    .filter(F.col('auid').isNotNull())
    .distinct()
)

# Get all APR auids
df_apr_auids = df_apr.select(F.col('auid')).filter(F.col('auid').isNotNull()).distinct()

ani_count = df_ani_auids.count()
matched = df_ani_auids.join(df_apr_auids, 'auid', 'inner').count()
unmatched = ani_count - matched

print(f"Distinct ANI auids sampled: {ani_count}")
print(f"Found in APR:               {matched}  ({100*matched/ani_count:.1f}%)")
print(f"Missing from APR:           {unmatched} ({100*unmatched/ani_count:.1f}%)")

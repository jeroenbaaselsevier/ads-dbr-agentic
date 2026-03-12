# Databricks notebook source

# COMMAND ----------
# Inspect OrgDB tables: orgdb_support.hierarchy_*, orgdb_support.orgdb_*, orgdb_support.documentcount_*

import sys
sys.path.append('/Workspace/rads/library/')
import orgdb_functions

# Find latest snapshot
orgdb_date = orgdb_functions.get_last_orgdb_date()
print(f"Latest OrgDB date: {orgdb_date}")

# List all available dates
all_dates = sorted(orgdb_functions.get_all_orgdb_dates())
print(f"All available dates: {all_dates}")

# COMMAND ----------
# Schema and row counts for the three core tables

from pyspark.sql import functions as F

for tbl in ['orgdb', 'hierarchy', 'documentcount']:
    df = spark.table(f'orgdb_support.{tbl}_{orgdb_date}')
    count = df.count()
    print(f"\n=== orgdb_support.{tbl}_{orgdb_date} ({count:,} rows) ===")
    df.printSchema()

# COMMAND ----------
# Sample one row from each table as JSON

import json
for tbl in ['orgdb', 'hierarchy', 'documentcount']:
    df = spark.table(f'orgdb_support.{tbl}_{orgdb_date}')
    sample = df.limit(1).toJSON().collect()
    if sample:
        print(f"\n=== {tbl} sample ===")
        print(json.dumps(json.loads(sample[0]), indent=2, default=str))

# COMMAND ----------
# Relationship type distribution in hierarchy table
print("=== Relationship type counts ===")
(
    spark.table(f'orgdb_support.hierarchy_{orgdb_date}')
    .groupBy('reltype')
    .count()
    .orderBy(F.desc('count'))
    .show(20, truncate=False)
)

# COMMAND ----------
# orgtype distribution in orgdb table
print("=== orgtype counts ===")
(
    spark.table(f'orgdb_support.orgdb_{orgdb_date}')
    .groupBy('orgtype')
    .count()
    .orderBy(F.desc('count'))
    .show(30, truncate=False)
)

# COMMAND ----------
# orglevel distribution
print("=== orglevel counts ===")
(
    spark.table(f'orgdb_support.orgdb_{orgdb_date}')
    .groupBy('orglevel')
    .count()
    .orderBy(F.desc('count'))
    .show(20, truncate=False)
)

# COMMAND ----------
# orgvisibility distribution
print("=== orgvisibility counts ===")
(
    spark.table(f'orgdb_support.orgdb_{orgdb_date}')
    .groupBy('orgvisibility')
    .count()
    .orderBy(F.desc('count'))
    .show(20, truncate=False)
)

# COMMAND ----------
# Show a few visible non-skeletal orgs with their hierarchy context
print("=== Sample org with hierarchy ===")
df_h = orgdb_functions.get_df_hierarchy_selected(orgdb_date, orgdb_functions.get_default_attributable_relationships())
df_h.filter(F.col('output') > 100).orderBy(F.desc('output')).show(10, truncate=False)

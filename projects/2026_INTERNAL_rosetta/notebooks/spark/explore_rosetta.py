# Databricks notebook source

# COMMAND ----------
# Explore rosetta tables in mi_team database

import re
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# 1. List all rosetta tables (snapshots) in mi_team database

print("=" * 80)
print("ROSETTA TABLE EXPLORATION")
print("=" * 80)

# Get all tables in mi_team database
tables_list = spark.sql("SHOW TABLES IN mi_team").collect()
print(f"\nTotal tables in mi_team: {len(tables_list)}")

# Find all rosetta tables
rosetta_tables = []
for row in tables_list:
    table_name = row.tableName
    if table_name.startswith("rosetta_"):
        rosetta_tables.append(table_name)

print(f"\nRosetta snapshots found: {len(rosetta_tables)}")
print("\nAll rosetta snapshots (sorted):")
rosetta_tables_sorted = sorted(rosetta_tables, reverse=True)
for table_name in rosetta_tables_sorted:
    print(f"  - {table_name}")

# COMMAND ----------
# 2. Get info about the latest rosetta table

if rosetta_tables_sorted:
    latest_rosetta = rosetta_tables_sorted[0]
    print(f"\n\nLATEST ROSETTA TABLE: mi_team.{latest_rosetta}")
    print("=" * 80)
    
    # Load the table
    df = spark.table(f"mi_team.{latest_rosetta}")
    
    # Get row count
    row_count = df.count()
    print(f"\nRow count: {row_count:,}")
    
    # Get schema
    print(f"\nSchema (total columns: {len(df.columns)}):")
    print("-" * 80)
    df.printSchema()
    
    # COMMAND ----------
    # 3. Sample rows
    print("\n\nSample rows (first 5):")
    print("-" * 80)
    df.show(5, truncate=False)
    
    # COMMAND ----------
    # 4. Column statistics
    print("\n\nColumn statistics:")
    print("-" * 80)
    df.describe().show()
    
    # COMMAND ----------
    # 5. Data summary
    print("\n\nData summary:")
    print("-" * 80)
    print(f"Columns: {df.columns}")
    print(f"Memory estimate (approx): {df.rdd.map(lambda x: len(str(x))).sum() / (1024*1024):.2f} MB")
    
else:
    print("ERROR: No rosetta tables found in mi_team database!")

# COMMAND ----------
# 6. Extract snapshot dates from table names
print("\n\nSNAPSHOT DATES (in YYYYMMDD format):")
print("=" * 80)

snapshots = []
for table_name in rosetta_tables:
    # Pattern: rosetta_YYYYMMDD
    match = re.match(r"rosetta_(\d{8})", table_name)
    if match:
        snapshot_date = match.group(1)
        snapshots.append(snapshot_date)

snapshots_sorted = sorted(snapshots, reverse=True)
for snapshot in snapshots_sorted:
    print(f"  {snapshot}")

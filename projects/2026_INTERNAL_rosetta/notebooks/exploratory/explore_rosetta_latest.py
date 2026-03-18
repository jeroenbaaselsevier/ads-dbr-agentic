# Databricks notebook source

# Explore the latest rosetta table in detail

from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

latest_table = "mi_team.rosetta_20251022"

print("=" * 80)
print(f"ROSETTA TABLE DETAILS: {latest_table}")
print("=" * 80)

try:
    df = spark.table(latest_table)
    
    # Row count
    print(f"\n1. ROW COUNT:")
    row_count = df.count()
    print(f"   {row_count:,} rows")
    
    # Schema
    print(f"\n2. SCHEMA ({len(df.columns)} columns):")
    print("-" * 80)
    df.printSchema()
    
    # Column list
    print(f"\n3. COLUMN NAMES:")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i:2d}. {col}")
    
    # Sample data
    print(f"\n4. SAMPLE DATA (first 5 rows):")
    print("-" * 80)
    df.show(5, truncate=False)
    
    # Data types summary
    print(f"\n5. DATA TYPE SUMMARY:")
    schema = df.schema
    type_counts = {}
    for field in schema.fields:
        dtype = str(field.dataType)
        type_counts[dtype] = type_counts.get(dtype, 0) + 1
    
    for dtype, count in sorted(type_counts.items()):
        print(f"   {dtype}: {count} column(s)")
    
except Exception as e:
    print(f"\nERROR: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

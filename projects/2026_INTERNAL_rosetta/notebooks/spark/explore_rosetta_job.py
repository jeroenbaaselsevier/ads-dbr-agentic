#!/usr/bin/env python3
"""
Explore rosetta table structure and save results.
Run as: databricks jobs submit ... --python-file explore_rosetta_job.py
"""
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
dbutils = spark._jvm.com.databricks.dbutils.DBUtilsHolder.dbutils()

results = {}

print("=" * 80)
print("EXPLORING ROSETTA TABLE STRUCTURE")
print("=" * 80)

# 1. List all rosetta tables
print("\n[1/3] Listing all rosetta tables in mi_team...")
try:
    tables_result = spark.sql("SHOW TABLES IN mi_team").collect()
    all_tables = [row.tableName for row in tables_result]
    rosetta_tables = sorted([t for t in all_tables if 'rosetta' in t.lower()])
    
    print(f"Found {len(rosetta_tables)} rosetta tables:")
    for t in rosetta_tables:
        print(f"  - {t}")
    
    results['rosetta_tables'] = rosetta_tables
    
except Exception as e:
    print(f"ERROR: Could not list tables: {e}")
    results['error_listing'] = str(e)
    rosetta_tables = []

# 2. Get latest rosetta table and inspect schema
if rosetta_tables:
    latest = rosetta_tables[-1]
    print(f"\n[2/3] Inspecting latest table: {latest}")
    
    try:
        df = spark.table(f"mi_team.{latest}")
        
        # Get schema
        schema_str = df._jdf.schema().prettyJson()
        print(f"\nSchema of {latest}:")
        print(schema_str)
        
        # Get column info
        columns = df.dtypes
        print(f"\nColumns ({len(columns)} total):")
        for col_name, col_type in columns:
            print(f"  {col_name}: {col_type}")
        
        # Row count
        row_count = df.count()
        print(f"\nRow count: {row_count:,}")
        
        # Sample rows
        print(f"\nFirst 3 rows:")
        df.show(3, truncate=False)
        
        # Store results
        results['latest_table'] = latest
        results['schema'] = {name: dtype for name, dtype in columns}
        results['row_count'] = row_count
        
    except Exception as e:
        print(f"ERROR inspecting {latest}: {e}")
        results['error_inspect'] = str(e)

# 3. Write results to DBFS for retrieval
print("\n[3/3] Writing results to DBFS...")
results_json = json.dumps(results, indent=2, default=str)
print(results_json)

output_path = "/dbfs/tmp/rosetta_exploration_results.json"
with open(output_path, 'w') as f:
    f.write(results_json)

print(f"\nResults saved to: {output_path}")
print("=" * 80)

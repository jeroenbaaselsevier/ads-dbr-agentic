# Databricks notebook source

# Direct diagnostic test - no imports, just raw SQL

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# 1. Can we query the database?
print("1. Testing SHOW TABLES IN mi_team:")
try:
    result = spark.sql("SHOW TABLES IN mi_team").collect()
    print(f"   ✓ Found {len(result)} tables")
except Exception as e:
    print(f"   ✗ Error: {e}")

# COMMAND ----------

# 2. Can we iterate and find rosetta tables?
print("2. Finding rosetta tables:")
try:
    result = spark.sql("SHOW TABLES IN mi_team").collect()
    rosetta_count = 0
    for row in result:
        table_name = row.tableName
        if table_name.startswith("rosetta_"):
            rosetta_count += 1
            if rosetta_count <= 5:
                print(f"   - {table_name}")
    print(f"   ✓ Found {rosetta_count} rosetta tables total")
except Exception as e:
    print(f"   ✗ Error: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# 3. Can we import snapshot_functions?
print("3. Importing snapshot_functions:")
try:
    import sys
    sys.path.append('/Workspace/rads/library/')
    import snapshot_functions
    print("   ✓ Import successful")
except Exception as e:
    print(f"   ✗ Error: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# 4. Does rosetta exist in snapshot_functions?
print("4. Checking rosetta object:")
try:
    if hasattr(snapshot_functions, 'rosetta'):
        print("   ✓ rosetta object exists")
        print(f"   Type: {type(snapshot_functions.rosetta)}")
    else:
        print("   ✗ rosetta object not found in snapshot_functions")
except Exception as e:
    print(f"   ✗ Error: {e}")

# COMMAND ----------

# 5. Can we call list_snapshots?
print("5. Calling rosetta.list_snapshots():")
try:
    snapshots = snapshot_functions.rosetta.list_snapshots()
    print(f"   ✓ Success! Found {len(snapshots)} snapshots")
    if snapshots:
        print(f"     Latest: {snapshots[0]}")
        print(f"     Oldest: {snapshots[-1]}")
except Exception as e:
    print(f"   ✗ Error: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# 6. Can we call get_table?
print("6. Calling rosetta.get_table():")
try:
    df = snapshot_functions.rosetta.get_table()
    print(f"   ✓ Success! Loaded: {df.count()} rows, {len(df.columns)} columns")
except Exception as e:
    print(f"   ✗ Error: {e}")
    import traceback
    traceback.print_exc()

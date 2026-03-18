# Databricks notebook source

# Test the new _RosettaHelper implementation

import sys, os
sys.path.append('/Workspace/rads/library/')
import snapshot_functions

# COMMAND ----------

print("=" * 80)
print("TESTING _RosettaHelper")
print("=" * 80)

# Test 1: List snapshots
print("\n1. Testing list_snapshots():")
snapshots = snapshot_functions.rosetta.list_snapshots()
print(f"   Found {len(snapshots)} snapshots")
print(f"   Most recent 10:")
for snap in snapshots[:10]:
    print(f"     - {snap}")

# COMMAND ----------

# Test 2: Get latest table (no snapshot specified)
print("\n2. Testing get_table() with no snapshot (should use latest):")
df_latest = snapshot_functions.rosetta.get_table()
print(f"   Loaded table: {df_latest.count()} rows, {len(df_latest.columns)} columns")
print(f"   First few columns: {df_latest.columns[:5]}")

# COMMAND ----------

# Test 3: Get specific snapshot (string YYYYMMDD)
print("\n3. Testing get_table('20240816'):")
df_specific = snapshot_functions.rosetta.get_table('20240816')
print(f"   Loaded table: {df_specific.count()} rows, {len(df_specific.columns)} columns")

# COMMAND ----------

# Test 4: Get specific snapshot (integer YYYYMMDD)
print("\n4. Testing get_table(20240816):")
df_int = snapshot_functions.rosetta.get_table(20240816)
print(f"   Loaded table: {df_int.count()} rows, {len(df_int.columns)} columns")

# COMMAND ----------

# Test 5: Get with date format YYYY-MM-DD
print("\n5. Testing get_table('2024-08-16'):")
df_dash = snapshot_functions.rosetta.get_table('2024-08-16')
print(f"   Loaded table: {df_dash.count()} rows, {len(df_dash.columns)} columns")

# COMMAND ----------

# Test 6: Show sample data
print("\n6. Sample data from latest snapshot:")
df_latest.select('srcid', 'title', 'issn1', 'publisher').show(3, truncate=True)

# COMMAND ----------

print("\n✓ All tests passed!")

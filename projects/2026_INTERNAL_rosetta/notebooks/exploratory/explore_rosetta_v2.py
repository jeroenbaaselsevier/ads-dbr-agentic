"""
Simple exploration of rosetta tables - trying different access patterns
"""

print("\n" + "="*80)
print("EXPLORING ROSETTA TABLE - ATTEMPT 2")
print("="*80)

# Try approach 1: Using spark.sql with SHOW TABLES
print("\n[Approach 1] Using SHOW TABLES in mi_team...")
try:
    result = spark.sql("SHOW TABLES IN mi_team")
    tables = result.collect()
    print(f"success - found {len(tables)} tables")
    
    rosetta_tables = [row.tableName for row in tables if 'rosetta' in str(row.tableName).lower()]
    print(f"Rosetta tables: {rosetta_tables}")
    
    if rosetta_tables:
        latest = max(rosetta_tables)  # Sort by name (should be YYYYMMDD)
        print(f"\nLatest rosetta table: {latest}")
        
        # Try to load it
        df = spark.table(f"mi_team.{latest}")
        print(f"✓ Loaded successfully")
        
        # Print schema
        print("\nSCHEMA:")
        df.printSchema()
        
        # Row count
        print(f"\nROW COUNT: {df.count():,}")
        
        # Sample
        print("\nSAMPLE (first 2 rows):")
        df.show(2, truncate=False)
        
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)

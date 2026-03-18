# Explore rosetta table structure and snapshots
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

print("\n" + "="*80)
print("EXPLORING ROSETTA TABLE")
print("="*80)

# Try to list all tables matching rosetta pattern
try:
    print("\n1. Looking for rosetta tables using information_schema...")
    result = spark.sql("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '%rosetta%'
        ORDER BY table_schema, table_name
    """).collect()
    
    if result:
        print(f"\nFound {len(result)} rosetta tables:")
        for row in result:
            print(f"  {row.table_schema}.{row.table_name}")
        
        # Get the latest one
        latest = result[-1]
        full_name = f"{latest.table_schema}.{latest.table_name}"
        print(f"\nInspecting latest: {full_name}")
        
        df = spark.table(full_name)
        print(f"\nSchema:")
        df.printSchema()
        print(f"\nRow count: {df.count():,}")
        print(f"\nFirst 3 rows:")
        df.show(3, truncate=False)
    else:
        print("No rosetta tables found in information_schema")
        
except Exception as e:
    print(f"Error with information_schema: {e}")

# Alternative: try direct mi_team access
try:
    print("\n2. Trying direct mi_team database access...")
    tables = spark.sql("SHOW TABLES IN mi_team").collect()
    rosetta_tables = [t.tableName for t in tables if 'rosetta' in t.tableName.lower()]
    
    if rosetta_tables:
        print(f"Found rosetta tables via SHOW TABLES: {rosetta_tables}")
        latest = sorted(rosetta_tables)[-1]
        df = spark.table(f"mi_team.{latest}")
        print(f"\nSchema of {latest}:")
        df.printSchema()
        print(f"Row count: {df.count():,}")
    else:
        print(f"No rosetta tables in mi_team. Available tables: {[t.tableName for t in tables[:10]]}")
        
except Exception as e:
    print(f"Error with mi_team: {e}")

print("\n" + "="*80)

# Databricks notebook source

# COMMAND ----------
# Explore rosetta tables in mi_team database

# List all tables in mi_team database that match rosetta pattern
print("=" * 80)
print("LISTING ALL ROSETTA SNAPSHOTS IN mi_team DATABASE")
print("=" * 80)

all_tables = spark.sql("SHOW TABLES IN mi_team").collect()
rosetta_tables = [row['tableName'] for row in all_tables if 'rosetta' in row['tableName'].lower()]
rosetta_tables_sorted = sorted(rosetta_tables, reverse=True)

print(f"\nFound {len(rosetta_tables_sorted)} rosetta tables:")
for table_name in rosetta_tables_sorted:
    print(f"  - {table_name}")

# COMMAND ----------
if len(rosetta_tables_sorted) > 0:
    latest_rosetta = rosetta_tables_sorted[0]
    print(f"\n{'=' * 80}")
    print(f"ANALYZING LATEST ROSETTA TABLE: {latest_rosetta}")
    print(f"{'=' * 80}")
    
    # Get table schema
    print(f"\nSCHEMA OF mi_team.{latest_rosetta}:")
    print("-" * 80)
    schema = spark.table(f"mi_team.{latest_rosetta}").schema
    for field in schema.fields:
        # Print column name, type, and nullable status
        nullable_str = "nullable" if field.nullable else "NOT NULL"
        print(f"  {field.name:30s} {str(field.dataType):30s} [{nullable_str}]")
    
    # COMMAND ----------
    # Get row count
    print(f"\n{'=' * 80}")
    print("TABLE STATISTICS")
    print(f"{'=' * 80}")
    
    df = spark.table(f"mi_team.{latest_rosetta}")
    row_count = df.count()
    print(f"\nRow count: {row_count:,}")
    
    # Get memory footprint estimate (columns and sample data)
    print(f"\nColumn count: {len(schema.fields)}")
    print(f"Columns: {', '.join([f.name for f in schema.fields])}")
    
    # COMMAND ----------
    # Sample data - first 20 rows
    print(f"\n{'=' * 80}")
    print("SAMPLE DATA (first 20 rows)")
    print(f"{'=' * 80}\n")
    df.limit(20).display()
    
    # COMMAND ----------
    # Check for table properties / comments that might contain documentation
    print(f"\n{'=' * 80}")
    print("TABLE METADATA / DOCUMENTATION")
    print(f"{'=' * 80}")
    
    metadata = spark.sql(f"DESCRIBE TABLE EXTENDED mi_team.{latest_rosetta}").collect()
    print("\nFull table properties:")
    for row in metadata:
        col_name = row['col_name']
        data_type = row['data_type']
        comment = row['comment']
        if col_name and data_type:
            print(f"  {col_name:30s} {data_type}")
        if comment:
            print(f"    ^ Comment: {comment}")
    
    # COMMAND ----------
    # Check for any view dependencies or lineage info
    print(f"\n{'=' * 80}")
    print("CHECKING ALL ROSETTA TABLE NAMES WITH SNAPSHOTS")
    print(f"{'=' * 80}\n")
    print("Pattern: rosetta_YYYYMMDD (sorted chronologically)")
    for table_name in rosetta_tables_sorted[:10]:  # Show top 10
        print(f"  {table_name}")
else:
    print("\nNo rosetta tables found in mi_team database.")

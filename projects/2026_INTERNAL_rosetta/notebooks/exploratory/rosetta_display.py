# Databricks notebook source

# COMMAND ----------
# Simple rosetta exploration - display results directly using display()

# Get all tables
all_tables = spark.sql("SHOW TABLES IN mi_team").collect()
all_table_names = [row['tableName'] for row in all_tables]

# Filter rosetta
rosetta_tables = sorted([t for t in all_table_names if 'rosetta' in t.lower()], reverse=True)

print("=" * 80)
print(f"ROSETTA SNAPSHOTS IN mi_team DATABASE")
print("=" * 80)
print(f"\nTotal Rosetta tables: {len(rosetta_tables)}\n")

# Display as DataFrame
import pandas as pd
rosetta_df = pd.DataFrame({'Rosetta_Tables': rosetta_tables})
display(rosetta_df)

# COMMAND ----------
if rosetta_tables:
    latest = rosetta_tables[0]
    print(f"\n{'=' * 80}")
    print(f"LATEST ROSETTA TABLE: {latest}")
    print("=" * 80)
    
    df = spark.table(f"mi_team.{latest}")
    row_count = df.count()
    schema = df.schema
    
    print(f"\nBasic Stats:")
    print(f"  Row count: {row_count:,}")
    print(f"  Columns:   {len(schema.fields)}")
    
    # COMMAND ----------
    print(f"\nSchema Information:")
    print("-" * 80)
    
    schema_data = []
    for field in schema.fields:
        schema_data.append({
            'Column_Name': field.name,
            'Data_Type': str(field.dataType),
            'Nullable': field.nullable
        })
    
    schema_df = pd.DataFrame(schema_data)
    display(schema_df)
    
    # COMMAND ----------
    print(f"\nSample Data (first 10 rows):")
    print("-" * 80)
    
    sample_df = df.limit(10)
    display(sample_df)

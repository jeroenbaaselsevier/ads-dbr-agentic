# Databricks notebook source

# COMMAND ----------
# Write rosetta analysis to output using dbutils

# Get all tables
all_tables = spark.sql("SHOW TABLES IN mi_team").collect()
all_table_names = [row['tableName'] for row in all_tables]

# Filter rosetta
rosetta_tables = sorted([t for t in all_table_names if t.startswith('rosetta_')], reverse=True)

print("=" * 80)
print("ROSETTA TABLES IN mi_team:")
print("=" * 80)
print(f"\nTotal: {len(rosetta_tables)} rosetta tables\n")

for i, table in enumerate(rosetta_tables, 1):
    print(f"{i:3d}. {table}")
    if i >= 20:
        if len(rosetta_tables) > 20:
            print(f"... and {len(rosetta_tables) - 20} more")
        break

# COMMAND ----------
# Analyze the latest
if rosetta_tables:
    latest = rosetta_tables[0]
    print(f"\n{'=' * 80}")
    print(" ANALYSIS OF LATEST ROSETTA TABLE")
    print("=" * 80)
    print(f"\nLatest: mi_team.{latest}\n")
    
    df = spark.table(f"mi_team.{latest}")
    row_count = df.count()
    schema = df.schema
    
    print(f"Statistics:")
    print(f"  Row count: {row_count:,}")
    print(f"  Columns:   {len(schema.fields)}")
    
    # COMMAND ----------
    print(f"\nSchema:")
    print("-" * 80)
    
    for field in schema.fields:
        nullable = "NULLABLE" if field.nullable else "NOT NULL"
        print(f"  {field.name:40s} {str(field.dataType):30s} {nullable}")
    
    # COMMAND ----------
    print(f"\nSample Data (first 10 rows):")
    print("-" * 80)
    display(df.limit(10))

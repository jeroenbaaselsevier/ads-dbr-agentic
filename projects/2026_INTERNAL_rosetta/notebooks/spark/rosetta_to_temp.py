# Databricks notebook source

# COMMAND ----------
# List all rosetta snapshots - write to a temp location we can read

import subprocess

# List tables
all_tables = spark.sql("SHOW TABLES IN mi_team").collect()
table_names = [row['tableName'] for row in all_tables]

# Filter rosetta tables
rosetta_tables = sorted([t for t in table_names if 'rosetta' in t.lower()], reverse=True)

# Write to dbfs for easy retrieval
output_path = "/tmp/rosetta_tables.txt"
with open(output_path, 'w') as f:
    f.write("ROSETTA TABLES FOUND:\n")
    f.write("=" * 80 + "\n")
    for table in rosetta_tables:
        f.write(f"{table}\n")
    f.write("\n\nTOTAL: " + str(len(rosetta_tables)) + " rosetta tables\n")

print(f"Wrote output to {output_path}")
print(f"Total rosetta tables: {len(rosetta_tables)}")

# COMMAND ----------
# Now analyze the latest one
if rosetta_tables:
    latest = rosetta_tables[0]
    print(f"\nAnalyzing latest: {latest}")
    
    df = spark.table(f"mi_team.{latest}")
    schema = df.schema
    row_count = df.count()
    
    # Write detailed output
    with open("/tmp/rosetta_analysis.txt", 'w') as f:
        f.write(f"LATEST ROSETTA TABLE: mi_team.{latest}\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"ROW COUNT: {row_count:,}\n")
        f.write(f"COLUMN COUNT: {len(schema.fields)}\n\n")
        
        f.write("SCHEMA:\n")
        f.write("-" * 80 + "\n")
        for field in schema.fields:
            nullable = "NULL" if field.nullable else "NOT NULL"
            f.write(f"{field.name:40s} {str(field.dataType):30s} {nullable}\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("SAMPLE DATA (first 20 rows):\n")
        f.write("=" * 80 + "\n\n")
        
        # Get sample data
        sample_df = df.limit(20).collect()
        for idx, row in enumerate(sample_df, 1):
            f.write(f"\n--- Row {idx} ---\n")
            for field_name in [f.name for f in schema.fields]:
                value = row[field_name]
                f.write(f"  {field_name}: {value}\n")
    
    print("Detailed analysis written to /tmp/rosetta_analysis.txt")

# COMMAND ----------
print("Done - output files ready")

# Databricks notebook source
import json
from datetime import datetime

# COMMAND ----------
# Explore rosetta tables and write JSON output to S3

# List all tables in mi_team
all_tables = spark.sql("SHOW TABLES IN mi_team").collect()
all_table_names = sorted([row['tableName'] for row in all_tables])

# Filter rosetta tables
rosetta_tables = sorted([t for t in all_table_names if 'rosetta' in t.lower()], reverse=True)

output_data = {
    "timestamp": datetime.now().isoformat(),
    "total_tables_in_mi_team": len(all_table_names),
    "all_table_names": all_table_names,
    "rosetta_table_count": len(rosetta_tables),
    "rosetta_tables": rosetta_tables,
}

# If we have rosetta tables, analyze the latest
if rosetta_tables:
    latest_rosetta = rosetta_tables[0]
    df = spark.table(f"mi_team.{latest_rosetta}")
    schema = df.schema
    row_count = df.count()
    
    schema_info = []
    for field in schema.fields:
        schema_info.append({
            "name": field.name,
            "type": str(field.dataType),
            "nullable": field.nullable
        })
    
    # Get sample rows
    sample_rows = df.limit(5).collect()
    sample_data = []
    for row in sample_rows:
        row_dict = row.asDict()
        # Convert complex types to strings for JSON serialization
        row_dict_str = {}
        for k, v in row_dict.items():
            if isinstance(v, (str, int, float, bool, type(None))):
                row_dict_str[k] = v
            else:
                row_dict_str[k] = str(v)
        sample_data.append(row_dict_str)
    
    output_data["latest_rosetta_table"] = latest_rosetta
    output_data["latest_rosetta_row_count"] = row_count
    output_data["latest_rosetta_column_count"] = len(schema_info)
    output_data["latest_rosetta_schema"] = schema_info
    output_data["latest_rosetta_sample_5_rows"] = sample_data

# Write to S3 as JSON
import boto3
s3 = boto3.client('s3')
json_content = json.dumps(output_data, indent=2, default=str)

remote_path = '/mnt/els/rads-projects/short_term/2026/rosetta_exploration_result.json'
print(f"Writing to {remote_path}")

# Write via dbutils
dbutils.fs.put(remote_path, json_content, overwrite=True)
print("✓ Written to S3")

# Also print a summary
print("\n" + "=" * 80)
print("ROSETTA TABLE EXPLORATION SUMMARY")
print("=" * 80)
print(f"Total tables in mi_team: {output_data['total_tables_in_mi_team']}")
print(f"Rosetta tables found: {output_data['rosetta_table_count']}")
if rosetta_tables:
    for table in rosetta_tables[:5]:
        print(f"  - {table}")
    if len(rosetta_tables) > 5:
        print(f"  ... and {len(rosetta_tables) - 5} more")
        
    print(f"\nLatest rosetta table: {output_data['latest_rosetta_table']}")
    print(f"  Row count: {output_data['latest_rosetta_row_count']:,}")
    print(f"  Columns: {output_data['latest_rosetta_column_count']}")
    print("\nSchema:")
    for col in output_data['latest_rosetta_schema'][:10]:
        print(f"  {col['name']:40s} {col['type']}")
    if len(output_data['latest_rosetta_schema']) > 10:
        print(f"  ... and {len(output_data['latest_rosetta_schema']) - 10} more columns")

# Databricks notebook source
# Simple direct SQL query to explore rosetta tables

# COMMAND ----------
# Use SQL to list tables
result_df = spark.sql("""
    SHOW TABLES IN mi_team 
    WHERE TABLE_NAME LIKE '%rosetta%'
    ORDER BY TABLE_NAME DESC
""")

print("Rosetta tables found:")
print(result_df.toPandas())

# COMMAND ----------
# Get details on the latest rosetta table
latest_tables = spark.sql("""
    SHOW TABLES IN mi_team
    WHERE TABLE_NAME LIKE '%rosetta%'
    ORDER BY TABLE_NAME DESC
    LIMIT 1
""").collect()

if latest_tables:
    latest_table_name = latest_tables[0]['tableName']
    print(f"\n\nLatest rosetta table: {latest_table_name}")
    print("=" * 80)
    
    # Describe the table
    schema_df = spark.sql(f"DESCRIBE TABLE mi_team.{latest_table_name}")
    for row in schema_df.collect():
        print(f"{row['col_name']:40s} {row['data_type']}")
    
    # Get row count
    count_result = spark.sql(f"SELECT COUNT(*) as row_count FROM mi_team.{latest_table_name}").collect()
    row_count = count_result[0]['row_count']
    print(f"\n\nRow count: {row_count:,}")
    
    # Get sample
    print("\nSample data (first 5 rows):")
    spark.sql(f"SELECT * FROM mi_team.{latest_table_name} LIMIT 5").show(truncate=False)

EOF

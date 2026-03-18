# Databricks notebook source

# Look specifically for rosetta tables in mi_team

from pyspark.sql import SparkSession
import re

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Get all tables in mi_team and filter for rosetta
try:
    result = spark.sql("SHOW TABLES IN mi_team").collect()
    print(f"Total tables in mi_team: {len(result)}")
    
    rosetta_tables = []
    for row in result:
        table_name = row[1]  # tableName is at index 1
        if 'rosetta' in table_name.lower():
            rosetta_tables.append(table_name)
    
    print(f"\nRosetta tables found: {len(rosetta_tables)}")
    rosetta_tables_sorted = sorted(rosetta_tables, reverse=True)
    
    for table_name in rosetta_tables_sorted[:30]:  # Show all found
        print(f"  {table_name}")
        
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

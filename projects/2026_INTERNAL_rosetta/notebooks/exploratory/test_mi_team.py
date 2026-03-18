# Databricks notebook source

# Simple test to see if we can access mi_team database

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Try to access mi_team database
try:
    result = spark.sql("SHOW DATABASES").collect()
    print(f"Total databases: {len(result)}")
    for row in result:
        db_name = row[0]
        print(f"  {db_name}")
except Exception as e:
    print(f"Error listing databases: {e}")

# COMMAND ----------

# Try to show tables in mi_team
try:
    result = spark.sql("SHOW TABLES IN mi_team").collect()
    print(f"\nTables in mi_team: {len(result)}")
    for row in result[:20]:  # Show first 20
        print(f"  {row[1]}")
except Exception as e:
    print(f"Error querying mi_team: {e}")

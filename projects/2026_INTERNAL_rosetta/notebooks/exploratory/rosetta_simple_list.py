# Databricks notebook source

# COMMAND ----------
# Simple rosetta table explorer - just list tables first

all_tables = spark.sql("SHOW TABLES IN mi_team").collect()
print("All tables in mi_team:")
for row in all_tables:
    print(row['tableName'])

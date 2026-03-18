# Databricks notebook source

# COMMAND ----------
import sys
sys.path.append('/Workspace/rads/library/')
import snapshot_functions

# Check topiccluster_keywords schema
df_kw = snapshot_functions.scival.get_table('topiccluster_keywords')
print("topiccluster_keywords schema:")
df_kw.printSchema()
df_kw.show(3, truncate=False)

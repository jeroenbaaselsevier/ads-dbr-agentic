# Databricks notebook source
# Inspect the ANI table schema and sample data

# COMMAND ----------

ani_stamp = '20260301'
df_ani = spark.table(f'scopus.ani_{ani_stamp}')

# COMMAND ----------
# Full schema
df_ani.printSchema()

# COMMAND ----------
# Row count
print(f'Total rows: {df_ani.count():,}')

# COMMAND ----------
# Column names and types summary
for field in df_ani.schema.fields:
    print(f'{field.name}\t{field.dataType.simpleString()}')

# COMMAND ----------
# Sample 3 rows (truncated for readability)
df_ani.limit(3).show(truncate=80, vertical=True)

# COMMAND ----------
# Check which ANI snapshots are available
tables = spark.sql("SHOW TABLES IN scopus LIKE 'ani_*'").select('tableName').orderBy('tableName').collect()
for t in tables[-12:]:
    print(t.tableName)

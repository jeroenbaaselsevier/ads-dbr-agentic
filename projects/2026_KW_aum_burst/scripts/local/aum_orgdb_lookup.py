# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F
import sys
sys.path.append('/Workspace/rads/library/')
import orgdb_functions

orgdb_date = orgdb_functions.get_last_orgdb_date()
print(f"OrgDB date: {orgdb_date}")

df_orgdb = spark.table(f'orgdb_support.orgdb_{orgdb_date}')

# Search for AUM
df_aum = (
    df_orgdb
    .filter(F.lower(F.col('orgname')).contains('american university of the middle east'))
    .filter(F.col('orglevel') != 'Skeletal')
    .filter(F.col('orgvisibility') == 'true')
    .select('org_id', 'orgname', 'orgtype', 'orglevel', 'country')
)
df_aum.show(20, truncate=False)

# Also get hierarchy children
aum_ids = [r.org_id for r in df_aum.select('org_id').distinct().collect()]
print(f"Top-level IDs: {aum_ids}")

df_children = (
    spark.table(f'orgdb_support.hierarchy_{orgdb_date}')
    .filter(F.col('final_attribution') == 'include')
    .filter(F.col('toplevel_orgid').isin(aum_ids))
    .select('org_id', 'toplevel_orgid')
    .distinct()
)
df_children.show(50, truncate=False)
print(f"Total afids: {df_children.count()}")

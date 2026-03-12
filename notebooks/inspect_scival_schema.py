# Databricks notebook source

# COMMAND ----------
import sys
sys.path.append('/Workspace/rads/library/')
import snapshot_functions
from pyspark.sql import functions as F
import json

print("SciVal tables:", snapshot_functions.scival.list_tables())

# COMMAND ----------
# For each table: list available snapshots and load latest
tables = snapshot_functions.scival.list_tables()
for tbl in tables:
    try:
        snaps = snapshot_functions.scival.list(tbl)
        print(f"{tbl}: {len(snaps)} snapshots, first={snaps[0] if snaps else 'none'}, last={snaps[-1] if snaps else 'none'}")
    except Exception as e:
        print(f"{tbl}: ERROR {e}")

# COMMAND ----------
# Load each table, print schema + row count + sample
for tbl in tables:
    try:
        df = snapshot_functions.scival.get_table(tbl)
        cnt = df.count()
        print(f"\n=== {tbl} ({cnt:,} rows) ===")
        df.printSchema()
        sample = df.limit(3).toJSON().collect()
        for row in sample:
            print(json.dumps(json.loads(row), indent=2, default=str))
    except Exception as e:
        print(f"\n=== {tbl}: ERROR ===\n{e}")

# COMMAND ----------
# topic_eid: join to ANI to understand EidString format
print("=== topic_eid EidString sample ===")
df_te = snapshot_functions.scival.get_table('topic_eid')
df_te.show(5)

# COMMAND ----------
# topic_prominence: distribution stats
print("=== topic_prominence stats ===")
df_tp = snapshot_functions.scival.get_table('topic_prominence')
df_tp.describe().show()

# COMMAND ----------
# institution: show sample
print("=== institution sample ===")
df_inst = snapshot_functions.scival.get_table('institution')
df_inst.show(10, truncate=False)
print(f"Total rows: {df_inst.count():,}")
print(f"Distinct institution_ids: {df_inst.select('institution_id').distinct().count():,}")
print(f"Distinct afids: {df_inst.select('afid').distinct().count():,}")

# COMMAND ----------
# institution_metadata: print schema + sample
print("=== institution_metadata ===")
df_im = snapshot_functions.scival.get_table('institution_metadata')
print(f"Row count: {df_im.count():,}")
df_im.printSchema()
sample = df_im.limit(1).toJSON().collect()
if sample:
    print(json.dumps(json.loads(sample[0]), indent=2, default=str))

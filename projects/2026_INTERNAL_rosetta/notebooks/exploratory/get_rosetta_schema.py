"""
Get rosetta table schema from information_schema
"""

# Get all rosetta tables and their columns
df_columns = spark.sql("""
    SELECT table_name, column_name, data_type, is_nullable, ordinal_position
    FROM information_schema.columns 
    WHERE table_schema = 'mi_team' 
      AND table_name LIKE 'rosetta_%'
      AND table_name NOT LIKE '%test%'
      AND table_name NOT LIKE '%upload%'
    ORDER BY table_name DESC, ordinal_position
""")

print("\n" + "="*100)
print("ROSETTA TABLE SCHEMA")
print("="*100)

# Display
df_columns.show(200, truncate=False)

# Also get just the table names
df_tables = spark.sql("""
    SELECT DISTINCT table_name
    FROM information_schema.columns 
    WHERE table_schema = 'mi_team' 
      AND table_name LIKE 'rosetta_%'
      AND table_name NOT LIKE '%test%'
      AND table_name NOT LIKE '%upload%'
    ORDER BY table_name DESC
""")

print("\n" + "="*100)
print("ROSETTA TABLES (by name, latest first)")
print("="*100)
df_tables.show(100, truncate=False)

# Get row counts for each table
print("\n" + "="*100)
print("ROSETTA TABLE ROW COUNTS")
print("="*100)

table_names = [row.table_name for row in df_tables.collect()]
for tname in table_names[:5]:  # Top 5 by name (most recent)
    try:
        count = spark.table(f"mi_team.{tname}").count()
        print(f"{tname}: {count:,} rows")
    except Exception as e:
        print(f"{tname}: ERROR - {e}")

print("="*100 + "\n")

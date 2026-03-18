-- Get rosetta table metadata
-- COMMAND ----------

SELECT * 
FROM information_schema.columns 
WHERE table_schema = 'mi_team' 
  AND table_name LIKE 'rosetta_%'
  AND table_name NOT LIKE '%test%'
  AND table_name NOT LIKE '%upload%'
ORDER BY table_name DESC, ordinal_position

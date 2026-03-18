-- Explore rosetta table structure
-- Databricks notebook source

-- COMMAND ----------

SELECT 'Checking mi_team access...' as status;

-- COMMAND ----------

SHOW TABLES IN mi_team LIKE 'rosetta_%';

-- COMMAND ----------

-- Get the latest rosetta table
WITH latest_rosetta AS (
  SELECT table_name 
  FROM information_schema.tables 
  WHERE table_schema = 'mi_team' 
  AND table_name LIKE 'rosetta_%'
  ORDER BY table_name DESC
  LIMIT 1
)
SELECT * FROM latest_rosetta;

-- COMMAND ----------

-- Describe schema (will use dynamic SQL if needed)
DESCRIBE mi_team.rosetta_*;

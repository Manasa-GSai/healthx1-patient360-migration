-- Databricks notebook source
-- MAGIC %md
-- MAGIC # WO-004 — Unity Catalog Validation
-- MAGIC
-- MAGIC Validates all acceptance criteria for WO-004:
-- MAGIC 1. Catalogs exist with correct schemas
-- MAGIC 2. External locations point to ADLS Gen2
-- MAGIC 3. Volumes are accessible
-- MAGIC 4. RBAC grants enforce least-privilege
-- MAGIC 5. Audit logging is active

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## AC-1: Verify catalogs and schemas exist

-- COMMAND ----------

-- Verify patient360_dev catalog and schemas
SELECT catalog_name, schema_name, comment
FROM system.information_schema.schemata
WHERE catalog_name IN ('patient360_dev', 'patient360_stg', 'patient360_prd')
  AND schema_name IN ('bronze', 'silver', 'gold_billing', 'gold_quality', 'gold_care_mgmt', 'reference')
ORDER BY catalog_name, schema_name;

-- COMMAND ----------

-- Count: expect 18 rows (3 catalogs x 6 schemas)
SELECT
  COUNT(*) AS total_schemas,
  COUNT(DISTINCT catalog_name) AS total_catalogs,
  CASE
    WHEN COUNT(*) = 18 AND COUNT(DISTINCT catalog_name) = 3
    THEN 'PASS'
    ELSE 'FAIL'
  END AS ac1_result
FROM system.information_schema.schemata
WHERE catalog_name IN ('patient360_dev', 'patient360_stg', 'patient360_prd')
  AND schema_name IN ('bronze', 'silver', 'gold_billing', 'gold_quality', 'gold_care_mgmt', 'reference');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## AC-2: Verify external locations point to ADLS Gen2

-- COMMAND ----------

SELECT
  external_location_name,
  url,
  credential_name,
  comment,
  CASE
    WHEN url LIKE 'abfss://%@%.dfs.core.windows.net/%' THEN 'PASS'
    ELSE 'FAIL'
  END AS adls_format_check
FROM system.information_schema.external_locations
WHERE external_location_name LIKE 'patient360_%'
ORDER BY external_location_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## AC-3: Verify volumes are accessible

-- COMMAND ----------

-- List files in dev bronze volume
LIST '/Volumes/patient360_dev/bronze/bronze_files/';

-- COMMAND ----------

-- Verify all volumes exist across environments
SELECT
  catalog_name,
  schema_name,
  volume_name,
  volume_type,
  storage_location
FROM system.information_schema.volumes
WHERE catalog_name IN ('patient360_dev', 'patient360_stg', 'patient360_prd')
ORDER BY catalog_name, schema_name, volume_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## AC-4: Verify RBAC grants

-- COMMAND ----------

-- Check catalog-level grants
SHOW GRANTS ON CATALOG patient360_dev;

-- COMMAND ----------

-- Verify engineer group has USE_CATALOG
SHOW GRANTS `patient360-dev-engineers` ON CATALOG patient360_dev;

-- COMMAND ----------

-- Verify analyst group has SELECT only on gold schemas (not bronze/silver)
SHOW GRANTS `patient360-dev-analysts` ON SCHEMA patient360_dev.gold_billing;

-- COMMAND ----------

-- Verify analysts do NOT have access to bronze/silver
SHOW GRANTS `patient360-dev-analysts` ON SCHEMA patient360_dev.bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## AC-5: Verify audit logging

-- COMMAND ----------

-- Check Unity Catalog audit log system table
SELECT
  event_time,
  service_name,
  action_name,
  request_params,
  response.status_code
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND event_time > current_timestamp() - INTERVAL 1 HOUR
ORDER BY event_time DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC
-- MAGIC | AC | Description | Verification |
-- MAGIC |---|---|---|
-- MAGIC | AC-1 | Catalogs + schemas | Query system.information_schema.schemata — expect 18 rows |
-- MAGIC | AC-2 | External locations | Query external_locations — all URLs match `abfss://` pattern |
-- MAGIC | AC-3 | Volumes | LIST command returns without error |
-- MAGIC | AC-4 | RBAC | Engineers: USE_CATALOG + USE_SCHEMA + CREATE_TABLE; Analysts: SELECT on gold only |
-- MAGIC | AC-5 | Audit logging | system.access.audit contains unityCatalog events |

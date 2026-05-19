# Databricks notebook source
# MAGIC %md
# MAGIC # WO-013: Canonical dim_encounter_type — Delta Table
# MAGIC
# MAGIC Creates the authoritative encounter type mapping in Unity Catalog.
# MAGIC Resolves RTM-DRIFT-001/002: semantic drift where billing derived
# MAGIC encounter_type via DECODE(visit_type_c) while care management read
# MAGIC it from Caboodle PatientDim.last_encounter_type.
# MAGIC
# MAGIC This table is the single source of truth for all downstream marts.
# MAGIC Changes require a Git commit — no manual edits.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType, BooleanType
)
from datetime import date

# COMMAND ----------

CATALOG = spark.conf.get("patient360.catalog", "patient360_dev")

# COMMAND ----------

# MAGIC %md ## Schema Definition

# COMMAND ----------

ENCOUNTER_TYPE_SCHEMA = StructType([
    StructField("visit_type_c", IntegerType(), nullable=False),
    StructField("encounter_type", StringType(), nullable=False),
    StructField("description", StringType(), nullable=False),
    StructField("effective_date", DateType(), nullable=False),
    StructField("is_active", BooleanType(), nullable=False),
])

# COMMAND ----------

# MAGIC %md ## Seed Data
# MAGIC
# MAGIC Authoritative mapping from Epic visit_type_c codes to standardized
# MAGIC encounter type strings. Derived from the DECODE logic in
# MAGIC m_clarity_to_billing_mart, with a catch-all for unmapped codes.

# COMMAND ----------

ENCOUNTER_TYPE_DATA = [
    (101, "OFFICE",    "Office visit — primary care or specialist",              date(2024, 1, 1), True),
    (102, "FOLLOWUP",  "Follow-up visit — post-procedure or post-discharge",     date(2024, 1, 1), True),
    (103, "WELLNESS",  "Annual wellness visit — preventive care",                date(2024, 1, 1), True),
    (201, "ACUTE",     "Acute care — urgent or emergency encounter",             date(2024, 1, 1), True),
    (0,   "OTHER",     "Unmapped encounter type — catch-all for unknown codes",  date(2024, 1, 1), True),
]

# COMMAND ----------

# MAGIC %md ## Create / Replace Table

# COMMAND ----------

df = spark.createDataFrame(ENCOUNTER_TYPE_DATA, schema=ENCOUNTER_TYPE_SCHEMA)

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.reference.dim_encounter_type")

spark.sql(f"""
    ALTER TABLE {CATALOG}.reference.dim_encounter_type
    SET TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'quality' = 'reference',
        'source' = 'WO-013',
        'description' = 'Canonical mapping from visit_type_c to encounter_type. Single source of truth for all marts.'
    )
""")

# COMMAND ----------

# MAGIC %md ## Validation

# COMMAND ----------

result = spark.table(f"{CATALOG}.reference.dim_encounter_type")
display(result.orderBy("visit_type_c"))

row_count = result.count()
distinct_keys = result.select("visit_type_c").distinct().count()
has_other = result.filter("encounter_type = 'OTHER'").count()

assert row_count == 5, f"Expected 5 rows, got {row_count}"
assert row_count == distinct_keys, f"Duplicate visit_type_c found: {row_count} rows but {distinct_keys} distinct keys"
assert has_other >= 1, "Missing catch-all OTHER row for unmapped codes"

print(f"✅ dim_encounter_type: {row_count} rows, {distinct_keys} distinct keys, catch-all present")

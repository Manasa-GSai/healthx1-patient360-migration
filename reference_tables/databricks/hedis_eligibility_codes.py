# Databricks notebook source
# MAGIC %md
# MAGIC # WO-015: Normalized HEDIS Eligibility Code Set — Delta Table
# MAGIC
# MAGIC Resolves RTM-HEDIS-001: INSTR substring matching against
# MAGIC comma-delimited CPT/ICD-10 strings in HEDIS_MeasureDefinition
# MAGIC caused false-positive eligibility matches (e.g., CPT '9921'
# MAGIC matching inside '99213').
# MAGIC
# MAGIC This table normalizes the delimited strings into one row per
# MAGIC measure_id + code combination. Quality mart uses exact-match
# MAGIC joins against this table instead of INSTR().
# MAGIC
# MAGIC Supports annual HEDIS value set updates without code changes —
# MAGIC refresh the data, not the logic.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DateType
)
from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

CATALOG = spark.conf.get("patient360.catalog", "patient360_dev")

# COMMAND ----------

# MAGIC %md ## Schema Definition

# COMMAND ----------

HEDIS_CODE_SCHEMA = StructType([
    StructField("measure_id", StringType(), nullable=False),
    StructField("code_type", StringType(), nullable=False),
    StructField("code_value", StringType(), nullable=False),
    StructField("effective_date", DateType(), nullable=False),
    StructField("expiration_date", DateType(), nullable=True),
])

# COMMAND ----------

# MAGIC %md ## Seed Data
# MAGIC
# MAGIC Production data would be loaded from the HEDIS Technical Specifications
# MAGIC value set files. This seed data matches the existing
# MAGIC HEDIS_MeasureDefinition lookup but in normalized form.

# COMMAND ----------

HEDIS_CODE_DATA = [
    # Comprehensive Diabetes Care (CDC)
    ("CDC", "CPT",   "99213", date(2024, 1, 1), None),
    ("CDC", "CPT",   "99214", date(2024, 1, 1), None),
    ("CDC", "ICD10", "E119",  date(2024, 1, 1), None),
    # Annual Wellness Visit (AWV)
    ("AWV", "CPT",   "99395", date(2024, 1, 1), None),
    ("AWV", "CPT",   "G0438", date(2024, 1, 1), None),
    ("AWV", "CPT",   "G0439", date(2024, 1, 1), None),
    # Controlling High Blood Pressure (CBP)
    ("CBP", "ICD10", "I10",   date(2024, 1, 1), None),
    # Appropriate Asthma Treatment (AAB)
    ("AAB", "ICD10", "J45909", date(2024, 1, 1), None),
    # Low Back Pain Imaging (LBP)
    ("LBP", "ICD10", "M545",  date(2024, 1, 1), None),
]

# COMMAND ----------

# MAGIC %md ## Create / Replace Table

# COMMAND ----------

df = spark.createDataFrame(HEDIS_CODE_DATA, schema=HEDIS_CODE_SCHEMA)

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.reference.hedis_eligibility_codes")

spark.sql(f"""
    ALTER TABLE {CATALOG}.reference.hedis_eligibility_codes
    SET TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'quality' = 'reference',
        'source' = 'WO-015',
        'description' = 'Normalized HEDIS eligibility codes. Replaces INSTR substring matching with exact-match joins.'
    )
""")

# COMMAND ----------

# MAGIC %md ## Validation

# COMMAND ----------

result = spark.table(f"{CATALOG}.reference.hedis_eligibility_codes")
display(result.orderBy("measure_id", "code_type", "code_value"))

row_count = result.count()
distinct_keys = result.select("measure_id", "code_type", "code_value").distinct().count()

assert row_count == 9, f"Expected 9 rows, got {row_count}"
assert row_count == distinct_keys, f"Duplicate keys found: {row_count} rows but {distinct_keys} distinct"

# Verify no partial/substring codes
short_codes = result.filter(
    ((F.col("code_type") == "CPT") & (F.length("code_value") < 4)) |
    ((F.col("code_type") == "ICD10") & (F.length("code_value") < 3))
)
assert short_codes.count() == 0, f"Found {short_codes.count()} suspiciously short codes"

# Verify no delimiter characters in code_value
delimited = result.filter(
    F.col("code_value").contains(",") | F.col("code_value").contains(" ")
)
assert delimited.count() == 0, f"Found {delimited.count()} codes with delimiters"

# Verify code_type is valid
invalid_types = result.filter(~F.col("code_type").isin("CPT", "ICD10"))
assert invalid_types.count() == 0, f"Found {invalid_types.count()} invalid code_type values"

# Count per measure for cross-reference
measure_counts = result.groupBy("measure_id").count().orderBy("measure_id")
display(measure_counts)

print(f"✅ hedis_eligibility_codes: {row_count} rows, {distinct_keys} distinct keys, no partial codes, no delimiters")

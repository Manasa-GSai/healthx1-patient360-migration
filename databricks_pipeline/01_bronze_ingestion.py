# Databricks notebook source
# MAGIC %md
# MAGIC # WO-019: Bronze Layer Ingestion (Auto Loader)
# MAGIC
# MAGIC Production append-only ingestion from the ADLS Gen2 landing zone into Bronze Delta tables.
# MAGIC
# MAGIC **Landing zone:** `/Volumes/{catalog}/bronze/bronze_files/landing/{entity}/`
# MAGIC **Targets:** `{catalog}.bronze.{encounter, charge_detail, dim_patient, hedis_measure_definition, icd10_reference}`
# MAGIC
# MAGIC - Auto Loader (`cloudFiles`) for incremental Parquet ingestion
# MAGIC - Source schema preserved; metadata: `_ingestion_timestamp`, `_source_file`, `_batch_id`
# MAGIC - Partitioned by `_ingestion_date` (derived from `_ingestion_timestamp`)
# MAGIC - Append-only — no updates or deletes at this layer

# COMMAND ----------

import uuid

from pyspark.sql.functions import col, current_timestamp, lit, to_date
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

CATALOG = spark.conf.get("patient360.catalog", "patient360_dev")
LANDING_ZONE = f"/Volumes/{CATALOG}/bronze/bronze_files/landing"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/bronze/bronze_files/checkpoints"
BATCH_ID = str(uuid.uuid4())

print(f"Catalog: {CATALOG}")
print(f"Batch ID: {BATCH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source schemas
# MAGIC
# MAGIC Explicit schemas prevent drift from `inferSchema` and match Informatica landing-zone extracts.

# COMMAND ----------

ENCOUNTER_SCHEMA = StructType([
    StructField("pat_id", StringType(), False),
    StructField("encounter_id", StringType(), False),
    StructField("visit_type_c", IntegerType(), True),
    StructField("encounter_dttm", TimestampType(), True),
    StructField("dept_id", StringType(), True),
    StructField("prov_id", StringType(), True),
    StructField("primary_dx_id", StringType(), True),
    StructField("encounter_status", StringType(), True),
])

CHARGE_DETAIL_SCHEMA = StructType([
    StructField("encounter_id", StringType(), False),
    StructField("charge_id", StringType(), False),
    StructField("cpt_code", StringType(), True),
    StructField("billed_amt", DecimalType(12, 2), True),
    StructField("paid_amt", DecimalType(12, 2), True),
    StructField("allowed_amt", DecimalType(12, 2), True),
    StructField("claim_status", StringType(), True),
    StructField("charge_dttm", TimestampType(), True),
])

DIM_PATIENT_SCHEMA = StructType([
    StructField("pat_id", StringType(), False),
    StructField("mrn", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("birth_date", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("payer_name", StringType(), True),
])

HEDIS_MEASURE_DEFINITION_SCHEMA = StructType([
    StructField("measure_id", StringType(), False),
    StructField("measure_name", StringType(), True),
    StructField("eligibility_cpt_set", StringType(), True),
    StructField("eligibility_icd10_set", StringType(), True),
    StructField("compliance_criteria", StringType(), True),
    StructField("measurement_period_days", IntegerType(), True),
])

ICD10_REFERENCE_SCHEMA = StructType([
    StructField("icd10_code", StringType(), False),
    StructField("short_description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("chronic_flag", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable Auto Loader helper

# COMMAND ----------


def ingest_bronze_cloudfiles(
    entity_name: str,
    source_schema: StructType,
    target_table: str,
    batch_id: str = BATCH_ID,
) -> dict:
    """
    Incrementally ingest Parquet files from the landing zone into a Bronze Delta table.

    Uses cloudFiles (Auto Loader) with append-only writes and _ingestion_date partitioning.
    Checkpoint and schema locations are isolated per entity for safe replays.
    """
    landing_path = f"{LANDING_ZONE}/{entity_name}/"
    checkpoint_path = f"{CHECKPOINT_BASE}/{entity_name}/"
    full_table = f"{CATALOG}.bronze.{target_table}"

    bronze_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(source_schema)
        .load(landing_path)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_ingestion_date", to_date(col("_ingestion_timestamp")))
    )

    query = (
        bronze_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_date")
        .trigger(availableNow=True)
        .table(full_table)
    )

    query.awaitTermination()

    row_count = spark.table(full_table).count()
    summary = {
        "entity": entity_name,
        "target_table": full_table,
        "rows": row_count,
        "batch_id": batch_id,
        "landing_path": landing_path,
    }
    print(
        f"Ingested {row_count:,} rows into {full_table} "
        f"(batch={batch_id}, landing={landing_path})"
    )
    return summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest all Bronze tables

# COMMAND ----------

BRONZE_INGEST_CONFIG = [
    ("encounter", ENCOUNTER_SCHEMA, "encounter"),
    ("charge_detail", CHARGE_DETAIL_SCHEMA, "charge_detail"),
    ("dim_patient", DIM_PATIENT_SCHEMA, "dim_patient"),
    ("hedis_measure_definition", HEDIS_MEASURE_DEFINITION_SCHEMA, "hedis_measure_definition"),
    ("icd10_reference", ICD10_REFERENCE_SCHEMA, "icd10_reference"),
]

ingest_summaries = []
for entity_name, schema, target_table in BRONZE_INGEST_CONFIG:
    ingest_summaries.append(
        ingest_bronze_cloudfiles(entity_name, schema, target_table)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion summary

# COMMAND ----------

summary_df = spark.createDataFrame(ingest_summaries)
display(summary_df)

for row in ingest_summaries:
    tbl = row["target_table"]
    spark.sql(f"""
        SELECT
          '{tbl}' AS table_name,
          COUNT(*) AS total_rows,
          COUNT(DISTINCT _batch_id) AS distinct_batches,
          MIN(_ingestion_timestamp) AS earliest_ingestion,
          MAX(_ingestion_timestamp) AS latest_ingestion
        FROM {tbl}
    """).show(truncate=False)

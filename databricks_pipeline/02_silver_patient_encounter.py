# Databricks notebook source
# MAGIC %md
# MAGIC # WO-020: Silver Layer — Patient, Encounter, and Charges
# MAGIC
# MAGIC Cleanses Bronze data into conformed Silver dimensions and facts.
# MAGIC
# MAGIC | Table | Pattern | Key behaviors |
# MAGIC |---|---|---|
# MAGIC | `silver.dim_patient` | SCD Type 2 | SHA-256 MRN tokenization, demographic null defaults |
# MAGIC | `silver.dim_encounter` | MERGE on `encounter_id` | Dedup by latest `encounter_dttm`; canonical `encounter_type` via `reference.dim_encounter_type` |
# MAGIC | `silver.fact_charges` | MERGE on `(encounter_id, charge_id)` | Monetary nulls default to 0.00 |
# MAGIC
# MAGIC **Data quality:** `pat_id` and `encounter_id` must be non-null (filtered before load).

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

CATALOG = spark.conf.get("patient360.catalog", "patient360_dev")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Bronze → cleansed patient dimension

# COMMAND ----------

patient_staging = (
    spark.table(f"{CATALOG}.bronze.dim_patient")
    .filter(F.col("pat_id").isNotNull())
    .withColumn(
        "mrn_token",
        F.sha2(F.coalesce(F.col("mrn"), F.lit("")), 256),
    )
    .withColumn("first_name", F.coalesce(F.trim(F.col("first_name")), F.lit("UNKNOWN")))
    .withColumn("last_name", F.coalesce(F.trim(F.col("last_name")), F.lit("UNKNOWN")))
    .withColumn("gender", F.coalesce(F.trim(F.col("gender")), F.lit("U")))
    .withColumn("zip_code", F.coalesce(F.trim(F.col("zip_code")), F.lit("00000")))
    .withColumn("payer_name", F.coalesce(F.trim(F.col("payer_name")), F.lit("UNKNOWN")))
    .withColumn("birth_date", F.col("birth_date").cast("date"))
    .withColumn(
        "attr_hash",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("mrn_token"),
                F.col("first_name"),
                F.col("last_name"),
                F.col("birth_date").cast("string"),
                F.col("gender"),
                F.col("zip_code"),
                F.col("payer_name"),
            ),
            256,
        ),
    )
    .select(
        "pat_id",
        "mrn_token",
        "first_name",
        "last_name",
        "birth_date",
        "gender",
        "zip_code",
        "payer_name",
        "attr_hash",
    )
    .dropDuplicates(["pat_id", "attr_hash"])
)

patient_staging.createOrReplaceTempView("patient_staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.dim_patient` — SCD Type 2

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_patient (
    pat_id          STRING      NOT NULL,
    mrn_token       STRING      NOT NULL COMMENT 'SHA-256 hash of source MRN (deterministic tokenization)',
    first_name      STRING      NOT NULL,
    last_name       STRING      NOT NULL,
    birth_date      DATE,
    gender          STRING      NOT NULL,
    zip_code        STRING      NOT NULL,
    payer_name      STRING      NOT NULL,
    effective_date  DATE        NOT NULL,
    expiration_date DATE,
    is_current      BOOLEAN     NOT NULL,
    load_dttm       TIMESTAMP   NOT NULL
) USING DELTA
COMMENT 'Patient dimension with SCD Type 2 history. MRN stored as SHA-256 token only.'
""")

# Expire prior current rows when attributes change
spark.sql(f"""
MERGE INTO {CATALOG}.silver.dim_patient AS t
USING patient_staging AS s
ON t.pat_id = s.pat_id AND t.is_current = true
WHEN MATCHED AND t.mrn_token != s.mrn_token
     OR t.first_name != s.first_name
     OR t.last_name != s.last_name
     OR t.birth_date <=> s.birth_date = false
     OR t.gender != s.gender
     OR t.zip_code != s.zip_code
     OR t.payer_name != s.payer_name
THEN UPDATE SET
    expiration_date = current_date(),
    is_current = false
""")

# Insert new versions (new patients and changed attribute hashes)
spark.sql(f"""
INSERT INTO {CATALOG}.silver.dim_patient
SELECT
    s.pat_id,
    s.mrn_token,
    s.first_name,
    s.last_name,
    s.birth_date,
    s.gender,
    s.zip_code,
    s.payer_name,
    current_date()           AS effective_date,
    CAST(NULL AS DATE)       AS expiration_date,
    true                     AS is_current,
    current_timestamp()      AS load_dttm
FROM patient_staging s
LEFT JOIN {CATALOG}.silver.dim_patient t
  ON s.pat_id = t.pat_id AND t.is_current = true
WHERE t.pat_id IS NULL
   OR t.mrn_token != s.mrn_token
   OR t.first_name != s.first_name
   OR t.last_name != s.last_name
   OR t.birth_date <=> s.birth_date = false
   OR t.gender != s.gender
   OR t.zip_code != s.zip_code
   OR t.payer_name != s.payer_name
""")

print(f"silver.dim_patient rows: {spark.table(f'{CATALOG}.silver.dim_patient').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Bronze encounter → `dim_encounter`

# COMMAND ----------

encounter_staging = (
    spark.table(f"{CATALOG}.bronze.encounter")
    .filter(F.col("pat_id").isNotNull() & F.col("encounter_id").isNotNull())
    .withColumn("encounter_dttm", F.col("encounter_dttm").cast("timestamp"))
    .withColumn(
        "primary_dx_code",
        F.regexp_replace(F.col("primary_dx_id"), r"^DX_", ""),
    )
    .join(
        spark.table(f"{CATALOG}.reference.dim_encounter_type").filter(F.col("is_active") == True),
        on="visit_type_c",
        how="left",
    )
    .withColumn(
        "encounter_type",
        F.coalesce(F.col("encounter_type"), F.lit("OTHER")),
    )
    .select(
        "pat_id",
        "encounter_id",
        "visit_type_c",
        "encounter_type",
        "encounter_dttm",
        "dept_id",
        "prov_id",
        "primary_dx_id",
        "primary_dx_code",
        "encounter_status",
    )
)

# Deduplicate: one row per encounter_id (latest encounter_dttm wins)
encounter_window = Window.partitionBy("encounter_id").orderBy(F.col("encounter_dttm").desc_nulls_last())

encounter_deduped = (
    encounter_staging.withColumn("_rn", F.row_number().over(encounter_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("load_dttm", F.current_timestamp())
)

encounter_deduped.createOrReplaceTempView("encounter_staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.dim_encounter` — MERGE on `encounter_id`

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_encounter (
    pat_id             STRING    NOT NULL,
    encounter_id       STRING    NOT NULL,
    visit_type_c       INT,
    encounter_type     STRING    NOT NULL COMMENT 'Canonical type from reference.dim_encounter_type',
    encounter_dttm     TIMESTAMP,
    dept_id            STRING,
    prov_id            STRING,
    primary_dx_id      STRING,
    primary_dx_code    STRING    COMMENT 'Normalized ICD-10 code (DX_ prefix stripped)',
    encounter_status   STRING,
    load_dttm          TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'Deduplicated encounter dimension. encounter_type from reference.dim_encounter_type join.'
""")

(
    DeltaTable.forName(spark, f"{CATALOG}.silver.dim_encounter")
    .alias("t")
    .merge(
        encounter_deduped.alias("s"),
        "t.encounter_id = s.encounter_id",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"silver.dim_encounter rows: {spark.table(f'{CATALOG}.silver.dim_encounter').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging: Bronze charge_detail → `fact_charges`

# COMMAND ----------

charges_staging = (
    spark.table(f"{CATALOG}.bronze.charge_detail")
    .filter(F.col("encounter_id").isNotNull() & F.col("charge_id").isNotNull())
    .withColumn("billed_amt", F.coalesce(F.col("billed_amt"), F.lit(0.00)).cast("decimal(12,2)"))
    .withColumn("paid_amt", F.coalesce(F.col("paid_amt"), F.lit(0.00)).cast("decimal(12,2)"))
    .withColumn("allowed_amt", F.coalesce(F.col("allowed_amt"), F.lit(0.00)).cast("decimal(12,2)"))
    .withColumn("charge_dttm", F.col("charge_dttm").cast("timestamp"))
    .select(
        "encounter_id",
        "charge_id",
        "cpt_code",
        "billed_amt",
        "paid_amt",
        "allowed_amt",
        "claim_status",
        "charge_dttm",
        F.current_timestamp().alias("load_dttm"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.fact_charges` — MERGE on `(encounter_id, charge_id)`

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.fact_charges (
    encounter_id  STRING          NOT NULL,
    charge_id     STRING          NOT NULL,
    cpt_code      STRING,
    billed_amt    DECIMAL(12, 2)  NOT NULL,
    paid_amt      DECIMAL(12, 2)  NOT NULL,
    allowed_amt   DECIMAL(12, 2)  NOT NULL,
    claim_status  STRING,
    charge_dttm   TIMESTAMP,
    load_dttm     TIMESTAMP       NOT NULL
) USING DELTA
COMMENT 'Charge fact with idempotent MERGE on (encounter_id, charge_id).'
""")

(
    DeltaTable.forName(spark, f"{CATALOG}.silver.fact_charges")
    .alias("t")
    .merge(
        charges_staging.alias("s"),
        "t.encounter_id = s.encounter_id AND t.charge_id = s.charge_id",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"silver.fact_charges rows: {spark.table(f'{CATALOG}.silver.fact_charges').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks

# COMMAND ----------

for table, col_name in [
    (f"{CATALOG}.silver.dim_patient", "pat_id"),
    (f"{CATALOG}.silver.dim_encounter", "pat_id"),
    (f"{CATALOG}.silver.dim_encounter", "encounter_id"),
    (f"{CATALOG}.silver.fact_charges", "encounter_id"),
]:
    null_count = spark.table(table).filter(F.col(col_name).isNull()).count()
    total = spark.table(table).count()
    null_pct = (null_count / total * 100) if total else 0
    status = "PASS" if null_count == 0 else "FAIL"
    print(f"[{status}] {table}.{col_name}: {null_pct:.2f}% null ({null_count}/{total})")
    if null_count > 0:
        raise ValueError(f"Null rate > 0% for {table}.{col_name}")

# Verify encounter_type is never sourced from inline DECODE (join-based only)
encounter_type_check = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {CATALOG}.silver.dim_encounter e
    LEFT JOIN {CATALOG}.reference.dim_encounter_type r
      ON e.visit_type_c = r.visit_type_c
    WHERE e.visit_type_c IS NOT NULL
      AND r.encounter_type IS NULL
      AND e.encounter_type != 'OTHER'
""")
unmapped = encounter_type_check.collect()[0]["cnt"]
assert unmapped == 0, f"Found {unmapped} encounters with unexpected encounter_type mapping"

print("All silver data quality checks passed.")

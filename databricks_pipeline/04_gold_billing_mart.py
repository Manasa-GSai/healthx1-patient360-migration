# Databricks notebook source
# MAGIC %md
# MAGIC # WO-022: Gold Billing Mart — `gold_billing.claims_processed`
# MAGIC
# MAGIC Production replacement for **m_clarity_to_billing_mart**. Materializes processed
# MAGIC claims from Silver encounter/charge dimensions with idempotent MERGE semantics.
# MAGIC
# MAGIC | Requirement | Implementation |
# MAGIC |---|---|
# MAGIC | Idempotent load | MERGE on `(encounter_id, charge_id)` |
# MAGIC | 90-day lookback | `encounter_dttm >= current_date() - lookback_days` |
# MAGIC | Denied exclusion | `claim_status != 'DENIED'` |
# MAGIC | Canonical encounter type | `reference.dim_encounter_type` via `silver.dim_encounter` |
# MAGIC | Patient responsibility | `allowed_amt - paid_amt` (contracted liability) |
# MAGIC | High-cost flag | `billed_amt > 10000` → `'Y'` / `'N'` |

# COMMAND ----------

from delta.tables import DeltaTable

CATALOG = spark.conf.get("patient360.catalog", "patient360_dev")
LOOKBACK_DAYS = int(spark.conf.get("patient360.billing.lookback_days", "90"))
HIGH_COST_THRESHOLD = float(spark.conf.get("patient360.billing.high_cost_threshold", "10000"))

TARGET_TABLE = f"{CATALOG}.gold_billing.claims_processed"

print(f"Catalog: {CATALOG} | Lookback: {LOOKBACK_DAYS} days | High-cost threshold: {HIGH_COST_THRESHOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure target schema exists

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    pat_id                  STRING          NOT NULL,
    encounter_id            STRING          NOT NULL,
    charge_id               STRING          NOT NULL,
    encounter_dttm          TIMESTAMP,
    encounter_type          STRING,
    cpt_code                STRING,
    primary_dx_code         STRING,
    payer_id                STRING,
    billed_amt              DECIMAL(12, 2),
    allowed_amt             DECIMAL(12, 2),
    paid_amt                DECIMAL(12, 2),
    patient_responsibility  DECIMAL(12, 2),
    high_cost_flag          STRING,
    claim_status            STRING,
    load_dttm               TIMESTAMP,
    updated_dttm            TIMESTAMP
)
USING DELTA
COMMENT 'Revenue Cycle billing mart. Idempotent UPSERT on (encounter_id, charge_id).'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build batch
# MAGIC
# MAGIC Joins `silver.dim_encounter` (canonical encounter grain) to `silver.fact_charges`
# MAGIC (charge grain). Encounter type is sourced from `reference.dim_encounter_type` via
# MAGIC `visit_type_c` on the encounter dimension — no inline DECODE.

# COMMAND ----------

batch = spark.sql(f"""
SELECT
    e.pat_id,
    e.encounter_id,
    c.charge_id,
    e.encounter_dttm,
    det.encounter_type,
    c.cpt_code,
    e.primary_dx_code,
    CAST(NULL AS STRING)                                                      AS payer_id,
    c.billed_amt,
    c.allowed_amt,
    c.paid_amt,
    (c.allowed_amt - c.paid_amt)                                              AS patient_responsibility,
    CASE WHEN c.billed_amt > {HIGH_COST_THRESHOLD} THEN 'Y' ELSE 'N' END     AS high_cost_flag,
    c.claim_status,
    current_timestamp()                                                       AS load_dttm,
    current_timestamp()                                                       AS updated_dttm
FROM {CATALOG}.silver.dim_encounter e
INNER JOIN {CATALOG}.silver.fact_charges c
    ON e.encounter_id = c.encounter_id
INNER JOIN {CATALOG}.reference.dim_encounter_type det
    ON e.visit_type_c = det.visit_type_c
   AND det.is_active = true
WHERE e.encounter_dttm >= date_sub(current_date(), {LOOKBACK_DAYS})
  AND c.claim_status != 'DENIED'
""")

batch_count = batch.count()
print(f"Batch row count: {batch_count}")
if batch_count == 0:
    print("WARNING: empty batch — MERGE will not insert or update any rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE (UPSERT)
# MAGIC
# MAGIC Re-running against the same source data produces zero duplicate keys.

# COMMAND ----------

target = DeltaTable.forName(spark, TARGET_TABLE)

(
    target.alias("t")
    .merge(
        batch.alias("s"),
        "t.encounter_id = s.encounter_id AND t.charge_id = s.charge_id",
    )
    .whenMatchedUpdate(
        set={
            "pat_id": "s.pat_id",
            "encounter_dttm": "s.encounter_dttm",
            "encounter_type": "s.encounter_type",
            "cpt_code": "s.cpt_code",
            "primary_dx_code": "s.primary_dx_code",
            "payer_id": "s.payer_id",
            "billed_amt": "s.billed_amt",
            "allowed_amt": "s.allowed_amt",
            "paid_amt": "s.paid_amt",
            "patient_responsibility": "s.patient_responsibility",
            "high_cost_flag": "s.high_cost_flag",
            "claim_status": "s.claim_status",
            "updated_dttm": "s.updated_dttm",
        }
    )
    .whenNotMatchedInsert(
        values={
            "pat_id": "s.pat_id",
            "encounter_id": "s.encounter_id",
            "charge_id": "s.charge_id",
            "encounter_dttm": "s.encounter_dttm",
            "encounter_type": "s.encounter_type",
            "cpt_code": "s.cpt_code",
            "primary_dx_code": "s.primary_dx_code",
            "payer_id": "s.payer_id",
            "billed_amt": "s.billed_amt",
            "allowed_amt": "s.allowed_amt",
            "paid_amt": "s.paid_amt",
            "patient_responsibility": "s.patient_responsibility",
            "high_cost_flag": "s.high_cost_flag",
            "claim_status": "s.claim_status",
            "load_dttm": "s.load_dttm",
            "updated_dttm": "s.updated_dttm",
        }
    )
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-load verification

# COMMAND ----------

result = spark.sql(f"""
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT encounter_id, charge_id) AS distinct_keys
FROM {TARGET_TABLE}
""").collect()[0]

row_count = result["row_count"]
distinct_keys = result["distinct_keys"]

print(f"{TARGET_TABLE}: {row_count} rows, {distinct_keys} distinct (encounter_id, charge_id)")

if row_count > 0 and row_count != distinct_keys:
    raise RuntimeError(
        f"Idempotency check failed for {TARGET_TABLE}: "
        f"{row_count} rows but {distinct_keys} distinct keys — duplicates present."
    )

print("Idempotency check passed.")
spark.conf.set("patient360.dq.last_mart", "gold_billing.claims_processed")

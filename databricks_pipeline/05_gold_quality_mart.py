# Databricks notebook source
# MAGIC %md
# MAGIC # WO-023: Gold Quality Mart — `gold_quality.measures`
# MAGIC
# MAGIC Production replacement for **m_caboodle_to_quality_mart**. Materializes Silver
# MAGIC HEDIS eligibility into the Gold measures fact with idempotent UPSERT semantics.
# MAGIC
# MAGIC | Requirement | Implementation |
# MAGIC |---|---|
# MAGIC | Exact-match eligibility | Inherited from `silver.hedis_eligibility` (no INSTR) |
# MAGIC | Canonical compliance | `measure_compliant` from Silver `encounter_type` |
# MAGIC | Age at encounter | `YEAR(encounter_dttm) - YEAR(birth_date)` |
# MAGIC | Idempotent load | MERGE on `(pat_id, encounter_id, measure_id)` |

# COMMAND ----------

from delta.tables import DeltaTable

CATALOG = spark.conf.get("patient360.catalog", "patient360_dev")
MEASUREMENT_YEAR = int(spark.conf.get("patient360.quality.measurement_year", "2026"))

TARGET_TABLE = f"{CATALOG}.gold_quality.measures"
SOURCE_TABLE = f"{CATALOG}.silver.hedis_eligibility"

print(f"Catalog: {CATALOG} | Measurement year: {MEASUREMENT_YEAR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure target schema exists

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    pat_id            STRING          NOT NULL,
    encounter_id      STRING          NOT NULL,
    encounter_dttm    TIMESTAMP,
    mrn               STRING,
    age_at_encounter  INT,
    measure_id        STRING          NOT NULL,
    measure_name      STRING,
    measure_eligible  STRING,
    measure_compliant STRING,
    encounter_type    STRING,
    load_dttm         TIMESTAMP,
    updated_dttm      TIMESTAMP
)
USING DELTA
COMMENT 'HEDIS quality measures. Idempotent UPSERT on (pat_id, encounter_id, measure_id).'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build batch from Silver eligibility
# MAGIC
# MAGIC Eligibility logic lives in `silver.hedis_eligibility` (WO-021) using exact-match
# MAGIC joins to `reference.hedis_eligibility_codes`. This Gold notebook only materializes
# MAGIC and enriches — it does not re-implement eligibility matching.

# COMMAND ----------

batch = spark.sql(f"""
SELECT
    h.pat_id,
    h.encounter_id,
    enc.encounter_dttm,
    p.mrn_token                                           AS mrn,
    CAST(
        YEAR(enc.encounter_dttm) - YEAR(p.birth_date) AS INT
    )                                                     AS age_at_encounter,
    h.measure_id,
    h.measure_name,
    h.measure_eligible,
    h.measure_compliant,
    h.encounter_type,
    current_timestamp()                                   AS load_dttm,
    current_timestamp()                                   AS updated_dttm
FROM {SOURCE_TABLE} h
INNER JOIN {CATALOG}.silver.dim_encounter enc
    ON h.encounter_id = enc.encounter_id
INNER JOIN {CATALOG}.silver.dim_patient p
    ON h.pat_id = p.pat_id
   AND p.is_current = true
WHERE h.measure_eligible = 'Y'
  AND YEAR(enc.encounter_dttm) = {MEASUREMENT_YEAR}
""")

batch_count = batch.count()
print(f"Batch row count: {batch_count}")
if batch_count == 0:
    print("WARNING: empty batch — MERGE will not insert or update any rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE (UPSERT)

# COMMAND ----------

target = DeltaTable.forName(spark, TARGET_TABLE)

(
    target.alias("t")
    .merge(
        batch.alias("s"),
        """
        t.pat_id = s.pat_id
        AND t.encounter_id = s.encounter_id
        AND t.measure_id = s.measure_id
        """,
    )
    .whenMatchedUpdate(
        set={
            "encounter_dttm": "s.encounter_dttm",
            "mrn": "s.mrn",
            "age_at_encounter": "s.age_at_encounter",
            "measure_name": "s.measure_name",
            "measure_eligible": "s.measure_eligible",
            "measure_compliant": "s.measure_compliant",
            "encounter_type": "s.encounter_type",
            "updated_dttm": "s.updated_dttm",
        }
    )
    .whenNotMatchedInsert(
        values={
            "pat_id": "s.pat_id",
            "encounter_id": "s.encounter_id",
            "encounter_dttm": "s.encounter_dttm",
            "mrn": "s.mrn",
            "age_at_encounter": "s.age_at_encounter",
            "measure_id": "s.measure_id",
            "measure_name": "s.measure_name",
            "measure_eligible": "s.measure_eligible",
            "measure_compliant": "s.measure_compliant",
            "encounter_type": "s.encounter_type",
            "load_dttm": "s.load_dttm",
            "updated_dttm": "s.updated_dttm",
        }
    )
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-load summary

# COMMAND ----------

summary = spark.sql(f"""
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT pat_id, encounter_id, measure_id) AS distinct_keys,
    SUM(CASE WHEN measure_compliant = 'Y' THEN 1 ELSE 0 END) AS compliant_count
FROM {TARGET_TABLE}
WHERE YEAR(encounter_dttm) = {MEASUREMENT_YEAR}
""").collect()[0]

print(
    f"{TARGET_TABLE} ({MEASUREMENT_YEAR}): "
    f"{summary['row_count']} rows, {summary['distinct_keys']} distinct keys, "
    f"{summary['compliant_count']} compliant"
)

if summary["row_count"] > 0 and summary["row_count"] != summary["distinct_keys"]:
    raise RuntimeError(
        f"Idempotency check failed for {TARGET_TABLE}: duplicate composite keys detected."
    )

spark.conf.set("patient360.dq.last_mart", "gold_quality.measures")

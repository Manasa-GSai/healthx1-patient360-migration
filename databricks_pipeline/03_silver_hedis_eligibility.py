# Databricks notebook source
# MAGIC %md
# MAGIC # WO-021: Silver Layer — HEDIS Eligibility (Exact-Match)
# MAGIC
# MAGIC Evaluates measure eligibility using **exact-match** joins to `reference.hedis_eligibility_codes`.
# MAGIC No `INSTR`, `LIKE`, or substring matching anywhere in this pipeline.
# MAGIC
# MAGIC **Target:** `{catalog}.silver.hedis_eligibility`
# MAGIC
# MAGIC | Column | Source |
# MAGIC |---|---|
# MAGIC | `measure_eligible` | Inner join match on CPT (`fact_charges`) or ICD-10 (`dim_encounter`) |
# MAGIC | `measure_compliant` | Canonical `encounter_type` from `silver.dim_encounter` |
# MAGIC | `age_at_encounter` | `months_between(encounter_dttm, birth_date) / 12` |
# MAGIC | `measure_name` | `bronze.hedis_measure_definition` |

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

CATALOG = spark.conf.get("patient360.catalog", "patient360_dev")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build eligibility batch (exact-match joins only)

# COMMAND ----------

eligibility_batch = spark.sql(f"""
WITH current_patient AS (
    SELECT pat_id, birth_date
    FROM {CATALOG}.silver.dim_patient
    WHERE is_current = true
),
encounters AS (
    SELECT
        e.pat_id,
        e.encounter_id,
        e.encounter_dttm,
        e.encounter_type,
        e.primary_dx_code
    FROM {CATALOG}.silver.dim_encounter e
    WHERE e.pat_id IS NOT NULL
      AND e.encounter_id IS NOT NULL
),
eligibility_by_cpt AS (
    SELECT DISTINCT
        enc.pat_id,
        enc.encounter_id,
        enc.encounter_dttm,
        enc.encounter_type,
        h.measure_id,
        md.measure_name
    FROM encounters enc
    INNER JOIN {CATALOG}.silver.fact_charges ch
        ON enc.encounter_id = ch.encounter_id
    INNER JOIN {CATALOG}.reference.hedis_eligibility_codes h
        ON h.code_type = 'CPT'
       AND h.code_value = ch.cpt_code
       AND (h.expiration_date IS NULL OR h.expiration_date >= current_date())
       AND h.effective_date <= current_date()
    LEFT JOIN {CATALOG}.bronze.hedis_measure_definition md
        ON h.measure_id = md.measure_id
    WHERE ch.cpt_code IS NOT NULL
),
eligibility_by_icd AS (
    SELECT DISTINCT
        enc.pat_id,
        enc.encounter_id,
        enc.encounter_dttm,
        enc.encounter_type,
        h.measure_id,
        md.measure_name
    FROM encounters enc
    INNER JOIN {CATALOG}.reference.hedis_eligibility_codes h
        ON h.code_type = 'ICD10'
       AND h.code_value = enc.primary_dx_code
       AND (h.expiration_date IS NULL OR h.expiration_date >= current_date())
       AND h.effective_date <= current_date()
    LEFT JOIN {CATALOG}.bronze.hedis_measure_definition md
        ON h.measure_id = md.measure_id
    WHERE enc.primary_dx_code IS NOT NULL
),
eligible AS (
    SELECT * FROM eligibility_by_cpt
    UNION
    SELECT * FROM eligibility_by_icd
)
SELECT
    e.pat_id,
    e.encounter_id,
    e.measure_id,
    COALESCE(e.measure_name, e.measure_id) AS measure_name,
    'Y'                                    AS measure_eligible,
    CASE
        WHEN e.encounter_type IN ('WELLNESS', 'FOLLOWUP') THEN 'Y'
        ELSE 'N'
    END                                    AS measure_compliant,
    CAST(
        FLOOR(months_between(e.encounter_dttm, p.birth_date) / 12) AS INT
    )                                      AS age_at_encounter,
    e.encounter_type,
    current_timestamp()                    AS load_dttm
FROM eligible e
INNER JOIN current_patient p
    ON e.pat_id = p.pat_id
WHERE p.birth_date IS NOT NULL
  AND e.encounter_dttm IS NOT NULL
""")

print(f"Eligibility batch rows: {eligibility_batch.count():,}")
display(eligibility_batch.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create target table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.hedis_eligibility (
    pat_id             STRING    NOT NULL,
    encounter_id       STRING    NOT NULL,
    measure_id         STRING    NOT NULL,
    measure_name       STRING    NOT NULL,
    measure_eligible   STRING    NOT NULL,
    measure_compliant  STRING    NOT NULL,
    age_at_encounter   INT,
    encounter_type     STRING    NOT NULL COMMENT 'Canonical encounter_type from silver.dim_encounter',
    load_dttm          TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'HEDIS eligibility per patient-encounter-measure. Exact-match joins only; UPSERT on composite key.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPSERT on `(pat_id, encounter_id, measure_id)`

# COMMAND ----------

(
    DeltaTable.forName(spark, f"{CATALOG}.silver.hedis_eligibility")
    .alias("t")
    .merge(
        eligibility_batch.alias("s"),
        """
        t.pat_id = s.pat_id
        AND t.encounter_id = s.encounter_id
        AND t.measure_id = s.measure_id
        """,
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

result = spark.table(f"{CATALOG}.silver.hedis_eligibility")
display(result.orderBy("pat_id", "encounter_id", "measure_id"))

# CPT '9921' must NOT match measure requiring '99213' (substring false-positive guard)
false_positive = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {CATALOG}.silver.hedis_eligibility e
    INNER JOIN {CATALOG}.silver.fact_charges ch
        ON e.encounter_id = ch.encounter_id
    WHERE ch.cpt_code = '9921'
      AND e.measure_id IN (
          SELECT measure_id
          FROM {CATALOG}.reference.hedis_eligibility_codes
          WHERE code_type = 'CPT' AND code_value = '99213'
      )
""")
fp_count = false_positive.collect()[0]["cnt"]
assert fp_count == 0, (
    f"False-positive eligibility detected: CPT 9921 matched measure requiring 99213 ({fp_count} rows)"
)

dup_keys = result.groupBy("pat_id", "encounter_id", "measure_id").count().filter("count > 1")
assert dup_keys.count() == 0, "Duplicate keys found in silver.hedis_eligibility after UPSERT"

print(
    f"silver.hedis_eligibility: {result.count():,} rows, "
    f"no substring false-positives, no duplicate keys"
)

# Databricks notebook source
# MAGIC %md
# MAGIC # WO-024: Gold Care Gap Queue — `gold_care_mgmt.gap_queue`
# MAGIC
# MAGIC Production replacement for **m_caboodle_to_care_gaps**. Builds the prioritized
# MAGIC outreach work queue from open quality measure gaps.
# MAGIC
# MAGIC | Requirement | Implementation |
# MAGIC |---|---|
# MAGIC | Open gaps only | `measure_eligible = 'Y' AND measure_compliant = 'N'` |
# MAGIC | Gap priority | HIGH (>365d), MEDIUM (>180d), LOW (≤180d) |
# MAGIC | Canonical outreach type | `reference.dim_encounter_type` via latest `silver.dim_encounter` |
# MAGIC | Idempotent load | MERGE on `(pat_id, measure_id)` |

# COMMAND ----------

from delta.tables import DeltaTable

CATALOG = spark.conf.get("patient360.catalog", "patient360_dev")

TARGET_TABLE = f"{CATALOG}.gold_care_mgmt.gap_queue"
QUALITY_TABLE = f"{CATALOG}.gold_quality.measures"

print(f"Catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure target schema exists

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    pat_id                       STRING          NOT NULL,
    mrn                          STRING,
    measure_id                   STRING          NOT NULL,
    measure_name                 STRING,
    days_since_last_encounter    INT,
    gap_priority                 STRING,
    encounter_type_for_outreach  STRING,
    primary_payer_id             STRING,
    load_dttm                    TIMESTAMP,
    updated_dttm                 TIMESTAMP
)
USING DELTA
COMMENT 'Care gap work queue. Idempotent UPSERT on (pat_id, measure_id).'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build open-gap batch
# MAGIC
# MAGIC `encounter_type_for_outreach` is the canonical type of the patient's most recent
# MAGIC encounter, resolved through `reference.dim_encounter_type` — not
# MAGIC `PatientDim.last_encounter_type`.

# COMMAND ----------

batch = spark.sql(f"""
WITH open_gaps AS (
    SELECT
        pat_id,
        measure_id,
        measure_name
    FROM {QUALITY_TABLE}
    WHERE measure_eligible = 'Y'
      AND measure_compliant = 'N'
),
latest_encounter AS (
    SELECT
        pat_id,
        MAX(encounter_dttm) AS last_encounter_dttm
    FROM {CATALOG}.silver.dim_encounter
    GROUP BY pat_id
),
last_encounter_canonical AS (
    SELECT
        e.pat_id,
        e.encounter_dttm AS last_encounter_dttm,
        det.encounter_type
    FROM {CATALOG}.silver.dim_encounter e
    INNER JOIN latest_encounter le
        ON e.pat_id = le.pat_id
       AND e.encounter_dttm = le.last_encounter_dttm
    INNER JOIN {CATALOG}.reference.dim_encounter_type det
        ON e.visit_type_c = det.visit_type_c
       AND det.is_active = true
)
SELECT
    g.pat_id,
    p.mrn_token                                           AS mrn,
    g.measure_id,
    g.measure_name,
    DATEDIFF(CURRENT_DATE(), lec.last_encounter_dttm)     AS days_since_last_encounter,
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), lec.last_encounter_dttm) > 365 THEN 'HIGH'
        WHEN DATEDIFF(CURRENT_DATE(), lec.last_encounter_dttm) > 180 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                   AS gap_priority,
    lec.encounter_type                                    AS encounter_type_for_outreach,
    p.payer_name                                          AS primary_payer_id,
    current_timestamp()                                   AS load_dttm,
    current_timestamp()                                   AS updated_dttm
FROM open_gaps g
LEFT JOIN {CATALOG}.silver.dim_patient p
    ON g.pat_id = p.pat_id
   AND p.is_current = true
LEFT JOIN last_encounter_canonical lec
    ON g.pat_id = lec.pat_id
""")

batch_count = batch.count()
print(f"Open gap batch row count: {batch_count}")

priority_dist = batch.groupBy("gap_priority").count().orderBy("gap_priority").collect()
for row in priority_dist:
    print(f"  {row['gap_priority']}: {row['count']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE (UPSERT)

# COMMAND ----------

target = DeltaTable.forName(spark, TARGET_TABLE)

(
    target.alias("t")
    .merge(
        batch.alias("s"),
        "t.pat_id = s.pat_id AND t.measure_id = s.measure_id",
    )
    .whenMatchedUpdate(
        set={
            "mrn": "s.mrn",
            "measure_name": "s.measure_name",
            "days_since_last_encounter": "s.days_since_last_encounter",
            "gap_priority": "s.gap_priority",
            "encounter_type_for_outreach": "s.encounter_type_for_outreach",
            "primary_payer_id": "s.primary_payer_id",
            "updated_dttm": "s.updated_dttm",
        }
    )
    .whenNotMatchedInsert(
        values={
            "pat_id": "s.pat_id",
            "mrn": "s.mrn",
            "measure_id": "s.measure_id",
            "measure_name": "s.measure_name",
            "days_since_last_encounter": "s.days_since_last_encounter",
            "gap_priority": "s.gap_priority",
            "encounter_type_for_outreach": "s.encounter_type_for_outreach",
            "primary_payer_id": "s.primary_payer_id",
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
    COUNT(DISTINCT pat_id, measure_id) AS distinct_keys,
    SUM(CASE WHEN gap_priority = 'HIGH' THEN 1 ELSE 0 END) AS high_priority
FROM {TARGET_TABLE}
""").collect()[0]

print(
    f"{TARGET_TABLE}: {summary['row_count']} rows, "
    f"{summary['distinct_keys']} distinct (pat_id, measure_id), "
    f"{summary['high_priority']} HIGH priority"
)

if summary["row_count"] > 0 and summary["row_count"] != summary["distinct_keys"]:
    raise RuntimeError(
        f"Idempotency check failed for {TARGET_TABLE}: duplicate composite keys detected."
    )

spark.conf.set("patient360.dq.last_mart", "gold_care_mgmt.gap_queue")

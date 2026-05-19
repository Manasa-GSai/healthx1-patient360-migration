# Databricks notebook source
# MAGIC %md
# MAGIC # WO-025: Post-Load Data Quality Assertion Framework
# MAGIC
# MAGIC Reusable assertion functions executed after each Gold mart materialization.
# MAGIC Failed assertions are logged to `audit.dq_results` and raise an exception to
# MAGIC halt downstream pipeline stages.
# MAGIC
# MAGIC **Workflow integration:** invoke this notebook (or `%run` it) as a task after
# MAGIC notebooks `04`, `05`, and `06`. Set widget `mart_to_validate` to the mart
# MAGIC that just completed, or `ALL` to validate every Gold mart.

# COMMAND ----------

import json
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = spark.conf.get("patient360.catalog", "patient360_dev")
ROW_COUNT_THRESHOLD = float(spark.conf.get("patient360.dq.row_count_threshold", "0.80"))
TRAILING_DAYS = int(spark.conf.get("patient360.dq.trailing_days", "7"))

AUDIT_TABLE = f"{CATALOG}.audit.dq_results"

BILLING_TABLE = f"{CATALOG}.gold_billing.claims_processed"
QUALITY_TABLE = f"{CATALOG}.gold_quality.measures"
CARE_GAPS_TABLE = f"{CATALOG}.gold_care_mgmt.gap_queue"

dbutils.widgets.dropdown(
    "mart_to_validate",
    "ALL",
    ["ALL", "billing", "quality", "care_gaps"],
    "Mart to validate after load",
)
MART_TO_VALIDATE = dbutils.widgets.get("mart_to_validate")

print(
    f"Catalog: {CATALOG} | Row-count threshold: {ROW_COUNT_THRESHOLD:.0%} "
    f"of {TRAILING_DAYS}-day average | Mart: {MART_TO_VALIDATE}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit results table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    assertion_run_id  STRING,
    assertion_dttm    TIMESTAMP,
    mart_name         STRING,
    assertion_name    STRING,
    status            STRING,
    details           STRING
)
USING DELTA
COMMENT 'Data quality assertion outcomes. Failed rows halt downstream stages.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable assertion functions

# COMMAND ----------


def check_null_rate(
    spark: SparkSession,
    table_name: str,
    columns: list[str],
    max_null_rate: float = 0.0,
) -> dict[str, Any]:
    """Fail when any column's null rate exceeds max_null_rate (default 0%)."""
    total = spark.table(table_name).count()
    if total == 0:
        return {
            "assertion_name": f"null_rate:{','.join(columns)}",
            "status": "PASS",
            "details": json.dumps({"table": table_name, "row_count": 0, "columns": {}}),
        }

    null_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns
    ]
    null_counts = spark.table(table_name).agg(*null_exprs).collect()[0].asDict()

    column_results = {
        c: {
            "null_count": int(null_counts[c]),
            "null_rate": round(null_counts[c] / total, 6),
        }
        for c in columns
    }
    failures = {
        c: v for c, v in column_results.items() if v["null_rate"] > max_null_rate
    }
    status = "FAIL" if failures else "PASS"

    return {
        "assertion_name": f"null_rate:{','.join(columns)}",
        "status": status,
        "details": json.dumps(
            {
                "table": table_name,
                "row_count": total,
                "max_null_rate": max_null_rate,
                "columns": column_results,
                "failures": failures,
            }
        ),
    }


def check_row_count_threshold(
    spark: SparkSession,
    table_name: str,
    threshold_ratio: float = ROW_COUNT_THRESHOLD,
    trailing_days: int = TRAILING_DAYS,
) -> dict[str, Any]:
    """Fail when current row count is below threshold_ratio of trailing average."""
    current_count = spark.table(table_name).count()

    history = (
        spark.table(AUDIT_TABLE)
        .filter(
            (F.col("mart_name") == table_name)
            & (F.col("assertion_name") == "row_count_snapshot")
            & (F.col("status") == "PASS")
            & (
                F.col("assertion_dttm")
                >= F.date_sub(F.current_timestamp(), trailing_days)
            )
        )
        .select(
            F.get_json_object(F.col("details"), "$.row_count")
            .cast("long")
            .alias("row_count")
        )
    )

    history_counts = [r["row_count"] for r in history.collect() if r["row_count"] is not None]

    if not history_counts:
        status = "PASS"
        details = {
            "table": table_name,
            "current_count": current_count,
            "trailing_avg": None,
            "threshold_ratio": threshold_ratio,
            "message": "No trailing history — baseline pass; snapshot recorded.",
        }
    else:
        trailing_avg = sum(history_counts) / len(history_counts)
        minimum_required = int(trailing_avg * threshold_ratio)
        status = "PASS" if current_count >= minimum_required else "FAIL"
        details = {
            "table": table_name,
            "current_count": current_count,
            "trailing_avg": round(trailing_avg, 2),
            "minimum_required": minimum_required,
            "threshold_ratio": threshold_ratio,
            "trailing_days": trailing_days,
            "history_samples": len(history_counts),
        }

    return {
        "assertion_name": "row_count_threshold",
        "status": status,
        "details": json.dumps(details),
    }


def check_referential_integrity(
    spark: SparkSession,
    child_table: str,
    parent_table: str,
    key_column: str = "pat_id",
) -> dict[str, Any]:
    """Fail when child keys are not present in the parent table."""
    child = spark.table(child_table).select(key_column).distinct()
    parent = spark.table(parent_table).select(key_column).distinct()

    orphans = child.join(parent, key_column, "left_anti")
    orphan_count = orphans.count()
    status = "FAIL" if orphan_count > 0 else "PASS"

    sample_orphans = (
        [r[key_column] for r in orphans.limit(10).collect()] if orphan_count else []
    )

    return {
        "assertion_name": f"referential_integrity:{child_table}->{parent_table}",
        "status": status,
        "details": json.dumps(
            {
                "child_table": child_table,
                "parent_table": parent_table,
                "key_column": key_column,
                "orphan_count": orphan_count,
                "sample_orphans": sample_orphans,
            }
        ),
    }


def record_row_count_snapshot(
    spark: SparkSession,
    table_name: str,
    mart_name: str,
    run_id: str,
) -> None:
    """Persist current row count for trailing-average calculations on future runs."""
    count = spark.table(table_name).count()
    snapshot = spark.createDataFrame(
        [
            (
                run_id,
                datetime.utcnow(),
                mart_name,
                "row_count_snapshot",
                "PASS",
                json.dumps({"table": table_name, "row_count": count}),
            )
        ],
        "assertion_run_id string, assertion_dttm timestamp, mart_name string, "
        "assertion_name string, status string, details string",
    )
    snapshot.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)


def log_dq_results(spark: SparkSession, results: list[dict[str, Any]], mart_name: str, run_id: str) -> None:
    """Append assertion outcomes to audit.dq_results."""
    rows = [
        (
            run_id,
            datetime.utcnow(),
            mart_name,
            r["assertion_name"],
            r["status"],
            r["details"],
        )
        for r in results
    ]
    df = spark.createDataFrame(
        rows,
        "assertion_run_id string, assertion_dttm timestamp, mart_name string, "
        "assertion_name string, status string, details string",
    )
    df.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)


def send_dq_alert(
    mart_name: str,
    failed_assertions: list[dict[str, Any]],
    webhook_url: str | None = None,
) -> None:
    """
    Alert stub for PagerDuty / Slack webhook integration.

    Production: set spark.conf patient360.dq.alert_webhook_url or pass webhook_url.
    Wire to requests.post() or Databricks notification destinations.
    """
    webhook = webhook_url or spark.conf.get("patient360.dq.alert_webhook_url", "")
    payload = {
        "source": "patient360_dq",
        "mart": mart_name,
        "severity": "critical",
        "failed_assertions": [
            {"name": a["assertion_name"], "details": a["details"]}
            for a in failed_assertions
        ],
    }
    print(f"[ALERT STUB] mart={mart_name} failures={len(failed_assertions)}")
    print(json.dumps(payload, indent=2))
    if webhook:
        print(f"Webhook configured ({webhook[:32]}...) — integrate HTTP POST in production.")


def run_assertions_for_mart(
    spark: SparkSession,
    mart_key: str,
    run_id: str,
) -> list[dict[str, Any]]:
    """Execute all assertions for a single Gold mart."""
    configs = {
        "billing": {
            "table": BILLING_TABLE,
            "null_columns": ["pat_id", "encounter_id", "charge_id"],
            "referential": None,
        },
        "quality": {
            "table": QUALITY_TABLE,
            "null_columns": ["pat_id", "encounter_id", "measure_id"],
            "referential": None,
        },
        "care_gaps": {
            "table": CARE_GAPS_TABLE,
            "null_columns": ["pat_id", "measure_id"],
            "referential": None,
        },
    }

    if mart_key not in configs:
        raise ValueError(f"Unknown mart key: {mart_key}")

    cfg = configs[mart_key]
    table = cfg["table"]
    results: list[dict[str, Any]] = []

    results.append(check_null_rate(spark, table, cfg["null_columns"]))
    results.append(
        check_row_count_threshold(
            spark, table, threshold_ratio=ROW_COUNT_THRESHOLD, trailing_days=TRAILING_DAYS
        )
    )

    if cfg["referential"]:
        child, parent, key = cfg["referential"]
        results.append(check_referential_integrity(spark, child, parent, key))

    log_dq_results(spark, results, table, run_id)

    failures = [r for r in results if r["status"] == "FAIL"]
    if failures:
        send_dq_alert(table, failures)
        names = ", ".join(r["assertion_name"] for r in failures)
        raise RuntimeError(
            f"Data quality assertions FAILED for {table}: {names}. "
            "Downstream pipeline stages halted."
        )

    record_row_count_snapshot(spark, table, table, run_id)
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute assertions

# COMMAND ----------

import uuid

RUN_ID = str(uuid.uuid4())
MARTS_TO_RUN = (
    ["billing", "quality", "care_gaps"]
    if MART_TO_VALIDATE == "ALL"
    else [MART_TO_VALIDATE]
)

all_results: dict[str, list[dict[str, Any]]] = {}

for mart in MARTS_TO_RUN:
    print(f"\n--- Validating {mart} ---")
    all_results[mart] = run_assertions_for_mart(spark, mart, RUN_ID)
    for result in all_results[mart]:
        print(f"  [{result['status']}] {result['assertion_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-mart referential integrity (quality → billing)
# MAGIC
# MAGIC Runs when validating quality or ALL marts.

# COMMAND ----------

if MART_TO_VALIDATE in ("ALL", "quality"):
    ref_result = check_referential_integrity(spark, QUALITY_TABLE, BILLING_TABLE, "pat_id")
    log_dq_results(spark, [ref_result], QUALITY_TABLE, RUN_ID)

    if ref_result["status"] == "FAIL":
        send_dq_alert(QUALITY_TABLE, [ref_result])
        raise RuntimeError(
            f"Referential integrity FAILED: orphaned pat_id values in {QUALITY_TABLE} "
            f"not found in {BILLING_TABLE}."
        )
    print(f"  [PASS] {ref_result['assertion_name']}")

print(f"\nAll assertions passed. Run ID: {RUN_ID}")

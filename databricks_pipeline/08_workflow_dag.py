# Databricks notebook source
# MAGIC %md
# MAGIC # WO-028: Patient360 Workflow DAG
# MAGIC
# MAGIC Creates or updates the nightly **Patient360** Databricks Workflow with strict task
# MAGIC dependencies, failure webhooks, per-stage retries (repair-run compatible), and
# MAGIC orchestration logging to `audit.orchestration_log`.
# MAGIC
# MAGIC **Pipeline order (sequential):**
# MAGIC `bronze_ingestion` → `silver_patient_encounter` → `silver_hedis_eligibility` →
# MAGIC `gold_billing_mart` → `billing_dq` → `gold_quality_mart` → `quality_dq` →
# MAGIC `gold_care_gaps` → `care_gaps_dq`
# MAGIC
# MAGIC **Cross-mart gates:**
# MAGIC - `gold_quality_mart` waits for `gold_billing_mart` **and** `billing_dq`
# MAGIC - `gold_care_gaps` waits for `gold_quality_mart` **and** `quality_dq`
# MAGIC
# MAGIC **SLA:** all Gold stages complete by **06:00 AM ET** (`America/New_York`).

# COMMAND ----------

import json
import os
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

# COMMAND ----------

CATALOG = spark.conf.get("patient360.catalog", "patient360_dev")
NOTEBOOK_BASE = spark.conf.get(
    "patient360.notebook_base",
    "/Workspace/Repos/patient360/databricks_pipeline",
)
JOB_NAME = spark.conf.get("patient360.workflow.job_name", "patient360_nightly_pipeline")
WEBHOOK_ID = spark.conf.get("patient360.workflow.alert_webhook_id", "")
SLA_DEADLINE_ET = "06:00"
SLA_TIMEZONE = "America/New_York"
CLUSTER_KEY = "patient360_job_cluster"

print(f"Catalog: {CATALOG} | Job: {JOB_NAME} | Notebook base: {NOTEBOOK_BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestration audit table

# COMMAND ----------

ORCHESTRATION_TABLE = f"{CATALOG}.audit.orchestration_log"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ORCHESTRATION_TABLE} (
    run_id           STRING      NOT NULL,
    stage_name       STRING      NOT NULL,
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    status           STRING      NOT NULL,
    row_count        BIGINT,
    details          STRING,
    logged_at        TIMESTAMP   NOT NULL
)
USING DELTA
COMMENT 'Per-stage orchestration metrics for Patient360 nightly workflow (WO-028).'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline stage registry
# MAGIC
# MAGIC Each stage maps to a notebook path and explicit `depends_on` task keys.
# MAGIC Linear predecessors are listed; additional cross-mart gates are merged below.

# COMMAND ----------

PIPELINE_STAGES: list[dict[str, Any]] = [
    {
        "task_key": "bronze_ingestion",
        "notebook": "01_bronze_ingestion",
        "depends_on": [],
        "row_count_table": f"{CATALOG}.bronze.encounter",
    },
    {
        "task_key": "silver_patient_encounter",
        "notebook": "02_silver_patient_encounter",
        "depends_on": ["bronze_ingestion"],
        "row_count_table": f"{CATALOG}.silver.dim_encounter",
    },
    {
        "task_key": "silver_hedis_eligibility",
        "notebook": "03_silver_hedis_eligibility",
        "depends_on": ["silver_patient_encounter"],
        "row_count_table": f"{CATALOG}.silver.hedis_eligibility",
    },
    {
        "task_key": "gold_billing_mart",
        "notebook": "04_gold_billing_mart",
        "depends_on": ["silver_hedis_eligibility"],
        "row_count_table": f"{CATALOG}.gold_billing.claims_processed",
    },
    {
        "task_key": "billing_dq",
        "notebook": "07_data_quality_assertions",
        "depends_on": ["gold_billing_mart"],
        "notebook_params": {"mart_to_validate": "billing"},
        "row_count_table": f"{CATALOG}.gold_billing.claims_processed",
    },
    {
        "task_key": "gold_quality_mart",
        "notebook": "05_gold_quality_mart",
        "depends_on": ["gold_billing_mart", "billing_dq"],
        "row_count_table": f"{CATALOG}.gold_quality.measures",
    },
    {
        "task_key": "quality_dq",
        "notebook": "07_data_quality_assertions",
        "depends_on": ["gold_quality_mart"],
        "notebook_params": {"mart_to_validate": "quality"},
        "row_count_table": f"{CATALOG}.gold_quality.measures",
    },
    {
        "task_key": "gold_care_gaps",
        "notebook": "06_gold_care_gaps",
        "depends_on": ["gold_quality_mart", "quality_dq"],
        "row_count_table": f"{CATALOG}.gold_care_mgmt.gap_queue",
    },
    {
        "task_key": "care_gaps_dq",
        "notebook": "07_data_quality_assertions",
        "depends_on": ["gold_care_gaps"],
        "notebook_params": {"mart_to_validate": "care_gaps"},
        "row_count_table": f"{CATALOG}.gold_care_mgmt.gap_queue",
    },
]

STAGE_ORDER = [s["task_key"] for s in PIPELINE_STAGES]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job cluster (autoscaling)

# COMMAND ----------


def build_job_cluster() -> jobs.JobCluster:
    """Autoscaling job cluster for all pipeline tasks."""
    spark_version = spark.conf.get(
        "patient360.workflow.spark_version",
        os.environ.get("DATABRICKS_RUNTIME_VERSION", "14.3.x-scala2.12"),
    )
    node_type = spark.conf.get("patient360.workflow.node_type_id", "Standard_DS3_v2")
    min_workers = int(spark.conf.get("patient360.workflow.min_workers", "2"))
    max_workers = int(spark.conf.get("patient360.workflow.max_workers", "8"))

    return jobs.JobCluster(
        job_cluster_key=CLUSTER_KEY,
        new_cluster=jobs.ClusterSpec(
            spark_version=spark_version,
            node_type_id=node_type,
            autoscale=jobs.AutoScale(min_workers=min_workers, max_workers=max_workers),
            spark_conf={
                "patient360.catalog": CATALOG,
                "spark.databricks.delta.preview.enabled": "true",
            },
            custom_tags={
                "project": "patient360",
                "work_order": "WO-028",
                "layer": "orchestration",
            },
        ),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Webhook notification stub
# MAGIC
# MAGIC Configure `patient360.workflow.alert_webhook_id` to a Databricks notification destination
# MAGIC ID. Failures at any stage trigger `on_failure` webhook delivery within minutes.

# COMMAND ----------


def build_webhook_notifications() -> list[jobs.WebhookNotification]:
    """Failure routing — any task failure triggers webhook alert."""
    if not WEBHOOK_ID:
        print(
            "[WEBHOOK STUB] patient360.workflow.alert_webhook_id not set — "
            "configure a Databricks notification destination for production alerts."
        )
        return []

    return [
        jobs.WebhookNotification(
            on_failure=[WEBHOOK_ID],
            on_duration_warning_threshold_exceeded=[WEBHOOK_ID],
        )
    ]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task builder

# COMMAND ----------


def build_pipeline_tasks() -> list[jobs.Task]:
    """Materialize Databricks Job tasks from PIPELINE_STAGES."""
    task_list: list[jobs.Task] = []

    for stage in PIPELINE_STAGES:
        notebook_path = f"{NOTEBOOK_BASE}/{stage['notebook']}"
        params = stage.get("notebook_params", {})

        task_list.append(
            jobs.Task(
                task_key=stage["task_key"],
                description=f"Patient360 stage: {stage['task_key']}",
                depends_on=[
                    jobs.TaskDependency(task_key=dep) for dep in stage["depends_on"]
                ],
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters=params,
                    source=jobs.Source.WORKSPACE,
                ),
                job_cluster_key=CLUSTER_KEY,
                max_retries=int(spark.conf.get("patient360.workflow.max_retries", "2")),
                min_retry_interval_millis=60_000,
                retry_on_timeout=True,
                timeout_seconds=int(
                    spark.conf.get("patient360.workflow.task_timeout_seconds", "7200")
                ),
            )
        )

    return task_list


# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestration logging helpers
# MAGIC
# MAGIC Captures `start_time`, `end_time`, `status`, and `row_counts` per stage from the
# MAGIC Jobs API after each run (or incrementally via repair run).

# COMMAND ----------


def log_orchestration_stage(
    run_id: str,
    stage_name: str,
    start_time: datetime | None,
    end_time: datetime | None,
    status: str,
    row_count: int | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """Append one stage record to audit.orchestration_log."""
    row = spark.createDataFrame(
        [
            (
                run_id,
                stage_name,
                start_time,
                end_time,
                status,
                row_count,
                json.dumps(details or {}),
                datetime.utcnow(),
            )
        ],
        "run_id string, stage_name string, start_time timestamp, end_time timestamp, "
        "status string, row_count long, details string, logged_at timestamp",
    )
    row.write.format("delta").mode("append").saveAsTable(ORCHESTRATION_TABLE)


def fetch_row_count(table_name: str) -> int | None:
    """Best-effort row count for a stage output table."""
    try:
        return spark.table(table_name).count()
    except Exception as exc:
        print(f"Row count unavailable for {table_name}: {exc}")
        return None


def sync_run_orchestration_logs(
    client: WorkspaceClient,
    job_run_id: int,
    stage_by_key: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Pull task-run timings from the Jobs API and persist orchestration metrics.

    Supports full runs and repair runs (only failed/downstream tasks re-execute).
    """
    run = client.jobs.get_run(run_id=job_run_id)
    run_id_str = str(job_run_id)
    summaries: list[dict[str, Any]] = []

    tasks = run.tasks or []
    for task_run in tasks:
        key = task_run.task_key or ""
        if key not in stage_by_key:
            continue

        state = task_run.state
        result_state = (
            state.result_state.value if state and state.result_state else "UNKNOWN"
        )
        lifecycle = state.life_cycle_state.value if state and state.life_cycle_state else ""

        status = result_state if result_state != "NONE" else lifecycle

        start_ms = task_run.start_time or 0
        end_ms = task_run.end_time or 0
        start_time = datetime.utcfromtimestamp(start_ms / 1000) if start_ms else None
        end_time = datetime.utcfromtimestamp(end_ms / 1000) if end_ms else None

        row_count = fetch_row_count(stage_by_key[key].get("row_count_table", ""))

        log_orchestration_stage(
            run_id=run_id_str,
            stage_name=key,
            start_time=start_time,
            end_time=end_time,
            status=status,
            row_count=row_count,
            details={
                "run_page_url": run.run_page_url,
                "attempt_number": task_run.attempt_number,
                "repair_run": bool(run.repair_history),
            },
        )

        summaries.append(
            {
                "stage_name": key,
                "start_time": start_time,
                "end_time": end_time,
                "status": status,
                "row_count": row_count,
            }
        )

    return summaries


def check_sla_compliance(run_end_time: datetime | None) -> dict[str, Any]:
    """
    Verify all Gold stages completed before 06:00 AM ET on the run date.

    Returns SLA metadata for dashboarding (WO-032).
    """
    tz = ZoneInfo(SLA_TIMEZONE)
    now_et = (run_end_time or datetime.now(tz)).astimezone(tz)
    deadline_hour, deadline_minute = (int(SLA_DEADLINE_ET.split(":")[0]), 0)
    deadline = now_et.replace(
        hour=deadline_hour, minute=deadline_minute, second=0, microsecond=0
    )
    met = now_et <= deadline
    return {
        "sla_deadline_et": SLA_DEADLINE_ET,
        "sla_timezone": SLA_TIMEZONE,
        "run_completed_et": now_et.isoformat(),
        "sla_met": met,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or update workflow (Jobs API)

# COMMAND ----------


def build_job_settings() -> jobs.JobSettings:
    """Assemble JobSettings for create/update."""
    cron_expr = spark.conf.get(
        "patient360.workflow.schedule_cron",
        "0 0 1 * * ?",  # 01:00 daily — allows 5h window before 06:00 ET SLA
    )

    return jobs.JobSettings(
        name=JOB_NAME,
        max_concurrent_runs=1,
        format=jobs.Format.MULTI_TASK,
        timeout_seconds=int(
            spark.conf.get("patient360.workflow.job_timeout_seconds", "18000")
        ),
        schedule=jobs.CronSchedule(
            quartz_cron_expression=cron_expr,
            timezone_id=SLA_TIMEZONE,
            pause_status=jobs.PauseStatus.UNPAUSED,
        ),
        job_clusters=[build_job_cluster()],
        tasks=build_pipeline_tasks(),
        webhook_notifications=build_webhook_notifications(),
        tags={
            "project": "patient360",
            "sla_deadline_et": SLA_DEADLINE_ET,
            "work_order": "WO-028",
        },
        email_notifications=jobs.JobEmailNotifications(
            on_failure=[spark.conf.get("patient360.workflow.alert_email", "")],
        )
        if spark.conf.get("patient360.workflow.alert_email", "")
        else None,
    )


def upsert_workflow(client: WorkspaceClient) -> int:
    """Create the workflow if missing; otherwise reset (update) settings."""
    settings = build_job_settings()
    existing = list(client.jobs.list(name=JOB_NAME))

    if existing:
        job_id = existing[0].job_id
        client.jobs.reset(job_id=job_id, new_settings=settings)
        print(f"Updated workflow job_id={job_id} ({JOB_NAME})")
        return job_id

    created = client.jobs.create(**settings.as_dict())
    print(f"Created workflow job_id={created.job_id} ({JOB_NAME})")
    return created.job_id


# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy workflow

# COMMAND ----------

w = WorkspaceClient()
stage_lookup = {s["task_key"]: s for s in PIPELINE_STAGES}

job_id = upsert_workflow(w)

print("Pipeline stage order:", " → ".join(STAGE_ORDER))
print(f"SLA target: all Gold stages complete by {SLA_DEADLINE_ET} {SLA_TIMEZONE}")
print(
    "Repair run: re-run a failed task via Jobs API without re-executing upstream tasks — "
    "e.g. w.jobs.repair_run(run_id=<run_id>, rerun_tasks=[jobs.RepairTask(task_key='gold_billing_mart')])"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: trigger run and sync orchestration logs
# MAGIC
# MAGIC Set widget `trigger_run` to `true` to start a job run after deploy.

# COMMAND ----------

dbutils.widgets.dropdown("trigger_run", "false", ["true", "false"])
TRIGGER_RUN = dbutils.widgets.get("trigger_run").lower() == "true"

if TRIGGER_RUN:
    run_now = w.jobs.run_now(job_id=job_id)
    run_id = run_now.run_id
    print(f"Triggered run_id={run_id} — poll with w.jobs.get_run(run_id={run_id})")

    # Poll until terminal state (simplified — production may use separate monitoring job)
    import time

    terminal = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
    while True:
        run = w.jobs.get_run(run_id=run_id)
        state = run.state.life_cycle_state.value if run.state else ""
        if state in terminal:
            break
        time.sleep(30)

    summaries = sync_run_orchestration_logs(w, run_id, stage_lookup)
    sla = check_sla_compliance(
        datetime.utcfromtimestamp((run.end_time or 0) / 1000) if run.end_time else None
    )
    print(f"Orchestration stages logged: {len(summaries)}")
    print(f"SLA check: {json.dumps(sla, indent=2)}")

    if run.state.result_state and run.state.result_state.value == "FAILED":
        raise RuntimeError(
            f"Workflow run {run_id} failed — use repair_run to retry failed stage only. "
            f"See {run.run_page_url}"
        )
else:
    print("Deploy-only mode (trigger_run=false). Set trigger_run=true to execute and log.")

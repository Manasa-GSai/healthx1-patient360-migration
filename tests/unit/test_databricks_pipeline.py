"""
Unit tests for Patient360 Databricks pipeline notebooks (WO-019 through WO-028).

Validates file existence, code patterns, and acceptance-criteria compliance without
requiring a live Spark cluster or Databricks workspace.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PIPELINE_DIR = ROOT / "databricks_pipeline"

PIPELINE_NOTEBOOKS = [
    "01_bronze_ingestion.py",
    "02_silver_patient_encounter.py",
    "03_silver_hedis_eligibility.py",
    "04_gold_billing_mart.py",
    "05_gold_quality_mart.py",
    "06_gold_care_gaps.py",
    "07_data_quality_assertions.py",
    "08_workflow_dag.py",
]

BRONZE_TABLES = [
    "encounter",
    "charge_detail",
    "dim_patient",
    "hedis_measure_definition",
    "icd10_reference",
]

CLOUD_PATH_PATTERNS = ("abfss://", "s3://", "gs://")

CATALOG_PATTERN = 'spark.conf.get("patient360.catalog", "patient360_dev")'


def _read_notebook(filename: str) -> str:
    path = PIPELINE_DIR / filename
    assert path.is_file(), f"Missing pipeline notebook: {path}"
    return path.read_text()


def _executable_code(code: str) -> str:
    """Strip comment-only lines so acceptance checks target runnable code."""
    lines = []
    for line in code.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        lines.append(line)
    return "\n".join(lines)


def _strip_magic_markdown(code: str) -> str:
    """Remove Databricks MAGIC markdown cells from pattern checks."""
    return re.sub(r"# MAGIC.*", "", code)


# ──────────────────────────────────────────────────────────────────────
# WO-019: Bronze ingestion
# ──────────────────────────────────────────────────────────────────────


class TestBronzeIngestion:
    """WO-019: Bronze layer Auto Loader ingestion."""

    @pytest.fixture(autouse=True)
    def load(self):
        self.code = _read_notebook("01_bronze_ingestion.py")
        self.executable = _executable_code(self.code)

    def test_databricks_notebook_format(self):
        assert self.code.startswith("# Databricks notebook source")
        assert "# COMMAND ----------" in self.code
        assert "# MAGIC %md" in self.code

    @pytest.mark.parametrize("table", BRONZE_TABLES)
    def test_all_five_source_tables(self, table):
        assert table in self.code

    def test_cloudfiles_auto_loader_pattern(self):
        assert 'format("cloudFiles")' in self.code
        assert 'option("cloudFiles.format", "parquet")' in self.code

    def test_append_only_no_merge_update_delete(self):
        assert '.outputMode("append")' in self.code
        upper = self.executable.upper()
        assert "MERGE INTO" not in upper
        assert "UPDATE " not in upper
        assert "DELETE FROM" not in upper

    @pytest.mark.parametrize(
        "col",
        ["_ingestion_timestamp", "_source_file", "_batch_id"],
    )
    def test_metadata_columns(self, col):
        assert col in self.code

    def test_partitioned_by_ingestion_date(self):
        assert '_ingestion_date' in self.code
        assert '.partitionBy("_ingestion_date")' in self.code

    def test_parameterized_catalog(self):
        assert CATALOG_PATTERN in self.code


# ──────────────────────────────────────────────────────────────────────
# WO-020: Silver patient / encounter
# ──────────────────────────────────────────────────────────────────────


class TestSilverPatientEncounter:
    """WO-020: Silver patient, encounter, and charges."""

    @pytest.fixture(autouse=True)
    def load(self):
        self.code = _read_notebook("02_silver_patient_encounter.py")
        self.executable = _executable_code(self.code)

    @pytest.mark.parametrize("col", ["effective_date", "expiration_date", "is_current"])
    def test_scd_type_2_columns(self, col):
        assert col in self.code

    def test_mrn_tokenization_sha256(self):
        assert "sha2" in self.code.lower() or "SHA-256" in self.code
        assert "mrn_token" in self.code

    def test_encounter_type_via_reference_join(self):
        assert "reference.dim_encounter_type" in self.code

    def test_merge_semantics_present(self):
        assert "MERGE INTO" in self.executable.upper() or ".merge(" in self.code

    def test_null_numeric_defaults_to_zero(self):
        assert "coalesce" in self.code.lower()
        assert "0.00" in self.code

    def test_parameterized_catalog(self):
        assert CATALOG_PATTERN in self.code


# ──────────────────────────────────────────────────────────────────────
# WO-021: Silver HEDIS eligibility
# ──────────────────────────────────────────────────────────────────────


class TestSilverHedisEligibility:
    """WO-021: Silver HEDIS eligibility with exact-match joins."""

    @pytest.fixture(autouse=True)
    def load(self):
        self.code = _read_notebook("03_silver_hedis_eligibility.py")
        self.executable = _executable_code(self.code)

    def test_inner_join_hedis_eligibility_codes(self):
        assert "hedis_eligibility_codes" in self.code
        assert "INNER JOIN" in self.code

    def test_no_instr_in_executable_code(self):
        assert "instr(" not in self.executable.lower()

    def test_upsert_composite_key(self):
        assert "pat_id = s.pat_id" in self.code
        assert "encounter_id = s.encounter_id" in self.code
        assert "measure_id = s.measure_id" in self.code

    def test_age_at_encounter_calculation(self):
        assert "age_at_encounter" in self.code
        assert "months_between" in self.code or "birth_date" in self.code

    def test_measure_compliant_uses_canonical_encounter_type(self):
        assert "encounter_type" in self.code
        assert "measure_compliant" in self.code
        assert "last_encounter_type" not in self.code

    def test_parameterized_catalog(self):
        assert CATALOG_PATTERN in self.code


# ──────────────────────────────────────────────────────────────────────
# WO-022: Gold billing mart
# ──────────────────────────────────────────────────────────────────────


class TestGoldBillingMart:
    """WO-022: Gold billing mart with MERGE semantics."""

    @pytest.fixture(autouse=True)
    def load(self):
        self.code = _read_notebook("04_gold_billing_mart.py")
        self.executable = _executable_code(self.code)

    def test_merge_on_encounter_and_charge(self):
        assert "encounter_id = s.encounter_id AND t.charge_id = s.charge_id" in self.code

    def test_ninety_day_lookback(self):
        assert "lookback" in self.code.lower() or "date_sub" in self.code.lower()
        assert "90" in self.code

    def test_denied_claim_exclusion(self):
        assert "DENIED" in self.code

    def test_patient_responsibility_allowed_minus_paid(self):
        assert re.search(
            r"\(c\.allowed_amt\s*-\s*c\.paid_amt\)|\(e\.allowed_amt\s*-\s*e\.paid_amt\)",
            self.executable,
        )
        assert "billed_amt - paid_amt" not in re.sub(r"\s+", "", self.executable)

    def test_high_cost_flag_logic(self):
        assert "high_cost_flag" in self.code
        assert "billed_amt" in self.code

    @pytest.mark.parametrize("col", ["load_dttm", "updated_dttm"])
    def test_load_and_updated_timestamps(self, col):
        assert col in self.code

    @pytest.mark.parametrize("prefix", CLOUD_PATH_PATTERNS)
    def test_no_cloud_specific_paths(self, prefix):
        assert prefix not in self.code

    def test_parameterized_catalog(self):
        assert CATALOG_PATTERN in self.code


# ──────────────────────────────────────────────────────────────────────
# WO-023: Gold quality mart
# ──────────────────────────────────────────────────────────────────────


class TestGoldQualityMart:
    """WO-023: Gold quality measures mart."""

    @pytest.fixture(autouse=True)
    def load(self):
        self.code = _read_notebook("05_gold_quality_mart.py")
        self.executable = _executable_code(self.code)

    def test_upsert_composite_key(self):
        assert "pat_id = s.pat_id" in self.code
        assert "encounter_id = s.encounter_id" in self.code
        assert "measure_id = s.measure_id" in self.code

    def test_no_instr_in_executable_code(self):
        assert "instr(" not in self.executable.lower()

    def test_canonical_encounter_type_usage(self):
        assert "encounter_type" in self.code
        assert "last_encounter_type" not in self.code

    def test_parameterized_catalog(self):
        assert CATALOG_PATTERN in self.code


# ──────────────────────────────────────────────────────────────────────
# WO-024: Gold care gaps
# ──────────────────────────────────────────────────────────────────────


class TestGoldCareGaps:
    """WO-024: Gold care gap queue."""

    @pytest.fixture(autouse=True)
    def load(self):
        self.code = _read_notebook("06_gold_care_gaps.py")
        self.executable = _executable_code(_strip_magic_markdown(self.code))

    def test_upsert_pat_id_measure_id(self):
        assert "pat_id = s.pat_id AND t.measure_id = s.measure_id" in self.code

    @pytest.mark.parametrize("priority", ["HIGH", "MEDIUM", "LOW"])
    def test_gap_priority_tiers(self, priority):
        assert priority in self.code

    def test_gap_priority_thresholds(self):
        assert "365" in self.code
        assert "180" in self.code

    def test_open_gaps_filter(self):
        assert "measure_compliant = 'N'" in self.code

    def test_canonical_encounter_type(self):
        assert "reference.dim_encounter_type" in self.executable
        assert "last_encounter_type" not in self.executable

    def test_parameterized_catalog(self):
        assert CATALOG_PATTERN in self.code


# ──────────────────────────────────────────────────────────────────────
# WO-025: Data quality framework
# ──────────────────────────────────────────────────────────────────────


class TestDataQualityFramework:
    """WO-025: Post-load data quality assertions."""

    @pytest.fixture(autouse=True)
    def load(self):
        self.code = _read_notebook("07_data_quality_assertions.py")
        self.executable = _executable_code(self.code)

    def test_null_check_function(self):
        assert "def check_null_rate(" in self.code

    def test_row_count_function(self):
        assert "def check_row_count_threshold(" in self.code

    def test_referential_integrity_function(self):
        assert "def check_referential_integrity(" in self.code

    def test_results_logged_to_audit_dq_results(self):
        assert "audit.dq_results" in self.code
        assert "def log_dq_results(" in self.code

    def test_failed_assertions_raise(self):
        assert "raise" in self.executable

    def test_alert_webhook_stub(self):
        assert "def send_dq_alert(" in self.code
        assert "webhook" in self.code.lower()

    def test_parameterized_catalog(self):
        assert CATALOG_PATTERN in self.code


# ──────────────────────────────────────────────────────────────────────
# WO-028: Workflow DAG
# ──────────────────────────────────────────────────────────────────────


class TestWorkflowDAG:
    """WO-028: Databricks Workflow DAG orchestration."""

    @pytest.fixture(autouse=True)
    def load(self):
        self.code = _read_notebook("08_workflow_dag.py")

    @pytest.mark.parametrize(
        "stage",
        [
            "bronze_ingestion",
            "silver_patient_encounter",
            "silver_hedis_eligibility",
            "gold_billing_mart",
            "billing_dq",
            "gold_quality_mart",
            "quality_dq",
            "gold_care_gaps",
            "care_gaps_dq",
        ],
    )
    def test_all_pipeline_stages_referenced(self, stage):
        assert stage in self.code

    def test_stage_order_in_registry(self):
        idx_bronze = self.code.index('"bronze_ingestion"')
        idx_silver = self.code.index('"silver_patient_encounter"')
        idx_hedis = self.code.index('"silver_hedis_eligibility"')
        idx_billing = self.code.index('"gold_billing_mart"')
        idx_billing_dq = self.code.index('"billing_dq"')
        idx_quality = self.code.index('"gold_quality_mart"')
        idx_quality_dq = self.code.index('"quality_dq"')
        idx_care = self.code.index('"gold_care_gaps"')
        idx_care_dq = self.code.index('"care_gaps_dq"')
        order = [
            idx_bronze,
            idx_silver,
            idx_hedis,
            idx_billing,
            idx_billing_dq,
            idx_quality,
            idx_quality_dq,
            idx_care,
            idx_care_dq,
        ]
        assert order == sorted(order), "PIPELINE_STAGES must list tasks in execution order"

    def test_quality_waits_for_billing_and_billing_dq(self):
        quality_block = self.code[self.code.index('"gold_quality_mart"'):]
        assert '"gold_billing_mart"' in quality_block
        assert '"billing_dq"' in quality_block

    def test_care_gaps_waits_for_quality_and_quality_dq(self):
        care_block = self.code[self.code.index('"gold_care_gaps"'):]
        assert '"gold_quality_mart"' in care_block
        assert '"quality_dq"' in care_block

    def test_webhook_alert_configuration(self):
        assert "webhook" in self.code.lower()
        assert "WebhookNotification" in self.code or "webhook_notifications" in self.code

    def test_retry_and_repair_capability(self):
        assert "max_retries" in self.code
        assert "repair_run" in self.code.lower()

    def test_sla_reference_0600_et(self):
        assert "06:00" in self.code
        assert "America/New_York" in self.code or "New_York" in self.code

    def test_orchestration_logging_fields(self):
        for field in ["start_time", "end_time", "status", "row_count"]:
            assert field in self.code
        assert "orchestration_log" in self.code

    def test_jobs_api_programmatic_deploy(self):
        assert "WorkspaceClient" in self.code
        assert "upsert_workflow" in self.code or "jobs.create" in self.code or "jobs.reset" in self.code

    def test_autoscaling_cluster(self):
        assert "autoscale" in self.code.lower() or "AutoScale" in self.code

    def test_parameterized_catalog(self):
        assert CATALOG_PATTERN in self.code


# ──────────────────────────────────────────────────────────────────────
# Cross-WO consistency
# ──────────────────────────────────────────────────────────────────────


class TestCrossWOConsistency:
    """Cross-cutting requirements across WO-019 through WO-028."""

    @pytest.mark.parametrize("filename", PIPELINE_NOTEBOOKS)
    def test_notebook_exists(self, filename):
        assert (PIPELINE_DIR / filename).is_file()

    @pytest.mark.parametrize("filename", PIPELINE_NOTEBOOKS)
    def test_databricks_notebook_format(self, filename):
        text = _read_notebook(filename)
        assert text.startswith("# Databricks notebook source")
        assert "# COMMAND ----------" in text

    @pytest.mark.parametrize("filename", PIPELINE_NOTEBOOKS)
    def test_parameterized_catalog(self, filename):
        code = _read_notebook(filename)
        assert CATALOG_PATTERN in code

    @pytest.mark.parametrize("filename", PIPELINE_NOTEBOOKS)
    def test_catalog_default_patient360_dev(self, filename):
        code = _read_notebook(filename)
        assert "patient360_dev" in code

    @pytest.mark.parametrize("filename", PIPELINE_NOTEBOOKS)
    @pytest.mark.parametrize("prefix", CLOUD_PATH_PATTERNS)
    def test_no_cloud_specific_paths(self, filename, prefix):
        code = _strip_magic_markdown(_read_notebook(filename))
        assert prefix not in code, f"{filename} must not use cloud-specific URI {prefix}"

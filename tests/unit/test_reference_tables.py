"""
Unit tests for WO-013 (dim_encounter_type) and WO-015 (hedis_eligibility_codes).

Validates SQL DDL, Databricks notebook code, and seed data files for
correctness, completeness, and compliance with acceptance criteria.
"""

import csv
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "reference_tables" / "sql_server"
DBX_DIR = ROOT / "reference_tables" / "databricks"
SEED_DIR = ROOT / "reference_tables" / "seed_data"

REQUIRED_ENCOUNTER_TYPES = {101: "OFFICE", 102: "FOLLOWUP", 103: "WELLNESS", 201: "ACUTE", 0: "OTHER"}
REQUIRED_ENCOUNTER_COLUMNS = ["visit_type_c", "encounter_type", "description", "effective_date", "is_active"]

REQUIRED_HEDIS_CODE_TYPES = {"CPT", "ICD10"}
REQUIRED_HEDIS_COLUMNS = ["measure_id", "code_type", "code_value", "effective_date", "expiration_date"]


# ──────────────────────────────────────────────────────────────────────
# WO-013: dim_encounter_type
# ──────────────────────────────────────────────────────────────────────

class TestDimEncounterTypeFileStructure:

    def test_sql_ddl_exists(self):
        assert (SQL_DIR / "dim_encounter_type.sql").is_file()

    def test_databricks_notebook_exists(self):
        assert (DBX_DIR / "dim_encounter_type.py").is_file()

    def test_seed_data_exists(self):
        assert (SEED_DIR / "dim_encounter_type.csv").is_file()


class TestDimEncounterTypeSQLDDL:

    @pytest.fixture(autouse=True)
    def load(self):
        self.sql = (SQL_DIR / "dim_encounter_type.sql").read_text()

    def test_creates_reference_schema(self):
        assert "CREATE SCHEMA" in self.sql
        assert "reference" in self.sql

    def test_table_name_correct(self):
        assert "reference.dim_encounter_type" in self.sql

    def test_primary_key_on_visit_type_c(self):
        assert "PRIMARY KEY" in self.sql
        assert "visit_type_c" in self.sql

    @pytest.mark.parametrize("col", REQUIRED_ENCOUNTER_COLUMNS)
    def test_required_column_exists(self, col):
        assert col in self.sql

    def test_has_check_constraint(self):
        assert "CHECK" in self.sql

    @pytest.mark.parametrize("code,etype", REQUIRED_ENCOUNTER_TYPES.items())
    def test_seed_data_contains_mapping(self, code, etype):
        assert str(code) in self.sql
        assert etype in self.sql

    def test_catch_all_other_row(self):
        assert "OTHER" in self.sql
        assert "unmapped" in self.sql.lower()


class TestDimEncounterTypeDatabricks:

    @pytest.fixture(autouse=True)
    def load(self):
        self.code = (DBX_DIR / "dim_encounter_type.py").read_text()

    def test_uses_catalog_variable(self):
        assert "CATALOG" in self.code
        assert "patient360_dev" in self.code

    def test_creates_delta_table(self):
        assert "delta" in self.code.lower()
        assert "saveAsTable" in self.code

    def test_writes_to_reference_schema(self):
        assert "reference.dim_encounter_type" in self.code

    @pytest.mark.parametrize("code,etype", REQUIRED_ENCOUNTER_TYPES.items())
    def test_seed_data_contains_mapping(self, code, etype):
        assert str(code) in self.code
        assert f'"{etype}"' in self.code

    def test_has_validation_assertions(self):
        assert "assert" in self.code

    def test_checks_row_count(self):
        assert "row_count" in self.code or "count()" in self.code

    def test_checks_distinct_keys(self):
        assert "distinct" in self.code.lower()

    def test_checks_catch_all(self):
        assert "OTHER" in self.code


class TestDimEncounterTypeSeedData:

    @pytest.fixture(autouse=True)
    def load(self):
        with open(SEED_DIR / "dim_encounter_type.csv") as f:
            reader = csv.DictReader(f)
            self.rows = list(reader)
            self.headers = reader.fieldnames

    def test_has_required_columns(self):
        for col in REQUIRED_ENCOUNTER_COLUMNS:
            assert col in self.headers, f"Missing column: {col}"

    def test_row_count(self):
        assert len(self.rows) == 5

    def test_all_required_mappings_present(self):
        seed_map = {int(r["visit_type_c"]): r["encounter_type"] for r in self.rows}
        for code, etype in REQUIRED_ENCOUNTER_TYPES.items():
            assert seed_map.get(code) == etype, f"Missing or wrong mapping for {code}"

    def test_no_duplicate_keys(self):
        keys = [r["visit_type_c"] for r in self.rows]
        assert len(keys) == len(set(keys))

    def test_all_active(self):
        for row in self.rows:
            assert row["is_active"] == "1"


# ──────────────────────────────────────────────────────────────────────
# WO-015: hedis_eligibility_codes
# ──────────────────────────────────────────────────────────────────────

class TestHedisCodesFileStructure:

    def test_sql_ddl_exists(self):
        assert (SQL_DIR / "hedis_eligibility_codes.sql").is_file()

    def test_databricks_notebook_exists(self):
        assert (DBX_DIR / "hedis_eligibility_codes.py").is_file()

    def test_seed_data_exists(self):
        assert (SEED_DIR / "hedis_eligibility_codes.csv").is_file()


class TestHedisCodesSQLDDL:

    @pytest.fixture(autouse=True)
    def load(self):
        self.sql = (SQL_DIR / "hedis_eligibility_codes.sql").read_text()

    def test_table_name_correct(self):
        assert "reference.hedis_eligibility_codes" in self.sql

    def test_composite_primary_key(self):
        assert "PRIMARY KEY" in self.sql
        assert "measure_id" in self.sql
        assert "code_type" in self.sql
        assert "code_value" in self.sql

    @pytest.mark.parametrize("col", REQUIRED_HEDIS_COLUMNS)
    def test_required_column_exists(self, col):
        assert col in self.sql

    def test_code_type_check_constraint(self):
        assert "CHECK" in self.sql
        assert "'CPT'" in self.sql
        assert "'ICD10'" in self.sql

    def test_no_delimiter_check(self):
        assert "NOT LIKE" in self.sql
        assert "%,%" in self.sql

    def test_uses_string_split_for_normalization(self):
        assert "STRING_SPLIT" in self.sql

    def test_normalizes_cpt_set(self):
        assert "eligibility_cpt_set" in self.sql

    def test_normalizes_icd10_set(self):
        assert "eligibility_icd10_set" in self.sql

    def test_trims_whitespace(self):
        assert "LTRIM" in self.sql
        assert "RTRIM" in self.sql


class TestHedisCodesDatabricks:

    @pytest.fixture(autouse=True)
    def load(self):
        self.code = (DBX_DIR / "hedis_eligibility_codes.py").read_text()

    def test_uses_catalog_variable(self):
        assert "CATALOG" in self.code
        assert "patient360_dev" in self.code

    def test_creates_delta_table(self):
        assert "delta" in self.code.lower()
        assert "saveAsTable" in self.code

    def test_writes_to_reference_schema(self):
        assert "reference.hedis_eligibility_codes" in self.code

    def test_has_validation_assertions(self):
        assert "assert" in self.code

    def test_checks_no_partial_codes(self):
        assert "short_codes" in self.code or "length" in self.code.lower()

    def test_checks_no_delimiters(self):
        assert "delimited" in self.code or "contains" in self.code

    def test_checks_valid_code_types(self):
        assert "CPT" in self.code
        assert "ICD10" in self.code

    def test_no_instr_substring_matching(self):
        """INSTR() must not be used as a function call in transformation logic."""
        code_lines = [
            line for line in self.code.splitlines()
            if not line.strip().startswith("# MAGIC")
            and not line.strip().startswith("#")
            and "'description'" not in line
        ]
        executable_code = "\n".join(code_lines)
        assert "INSTR(" not in executable_code
        assert "instr(" not in executable_code


class TestHedisCodesSeedData:

    @pytest.fixture(autouse=True)
    def load(self):
        with open(SEED_DIR / "hedis_eligibility_codes.csv") as f:
            reader = csv.DictReader(f)
            self.rows = list(reader)
            self.headers = reader.fieldnames

    def test_has_required_columns(self):
        for col in REQUIRED_HEDIS_COLUMNS:
            assert col in self.headers, f"Missing column: {col}"

    def test_row_count(self):
        assert len(self.rows) == 9

    def test_no_duplicate_keys(self):
        keys = [(r["measure_id"], r["code_type"], r["code_value"]) for r in self.rows]
        assert len(keys) == len(set(keys))

    def test_all_code_types_valid(self):
        for row in self.rows:
            assert row["code_type"] in REQUIRED_HEDIS_CODE_TYPES, (
                f"Invalid code_type: {row['code_type']}"
            )

    def test_no_partial_codes(self):
        for row in self.rows:
            code = row["code_value"]
            if row["code_type"] == "CPT":
                assert len(code) >= 4, f"CPT code too short: {code}"
            elif row["code_type"] == "ICD10":
                assert len(code) >= 3, f"ICD-10 code too short: {code}"

    def test_no_delimiter_in_codes(self):
        for row in self.rows:
            code = row["code_value"]
            assert "," not in code, f"Delimiter in code: {code}"
            assert " " not in code, f"Whitespace in code: {code}"

    def test_cdc_has_three_codes(self):
        cdc = [r for r in self.rows if r["measure_id"] == "CDC"]
        assert len(cdc) == 3

    def test_awv_has_three_codes(self):
        awv = [r for r in self.rows if r["measure_id"] == "AWV"]
        assert len(awv) == 3


# ──────────────────────────────────────────────────────────────────────
# Cross-table consistency
# ──────────────────────────────────────────────────────────────────────

class TestCrossTableConsistency:
    """Seed data in CSVs must match what's in the Databricks notebooks."""

    def test_encounter_type_csv_matches_notebook(self):
        csv_path = SEED_DIR / "dim_encounter_type.csv"
        nb_path = DBX_DIR / "dim_encounter_type.py"
        with open(csv_path) as f:
            csv_types = {row["encounter_type"] for row in csv.DictReader(f)}
        nb_code = nb_path.read_text()
        for etype in csv_types:
            assert etype in nb_code, f"Encounter type {etype} in CSV but not in notebook"

    def test_hedis_codes_csv_matches_notebook(self):
        csv_path = SEED_DIR / "hedis_eligibility_codes.csv"
        nb_path = DBX_DIR / "hedis_eligibility_codes.py"
        with open(csv_path) as f:
            csv_codes = {row["code_value"] for row in csv.DictReader(f)}
        nb_code = nb_path.read_text()
        for code in csv_codes:
            assert code in nb_code, f"Code {code} in CSV but not in notebook"

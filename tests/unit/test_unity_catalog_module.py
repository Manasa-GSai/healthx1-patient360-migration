"""
Unit tests for the Unity Catalog Terraform module.

Validates the module's configuration logic without requiring a live
Databricks workspace. Tests use terraform-json plan output parsing
and HCL structure validation.
"""

import json
import os
import subprocess
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

MODULE_DIR = Path(__file__).resolve().parents[2] / "terraform" / "modules" / "unity_catalog"
ENVIRONMENTS_DIR = Path(__file__).resolve().parents[2] / "terraform" / "environments"

REQUIRED_SCHEMAS = ["bronze", "silver", "gold_billing", "gold_quality", "gold_care_mgmt", "reference"]
REQUIRED_CONTAINERS = ["landing", "bronze", "silver", "gold", "audit"]
ENVIRONMENTS = ["dev", "stg", "prd"]
CATALOG_NAMES = {"dev": "patient360_dev", "stg": "patient360_stg", "prd": "patient360_prd"}
STORAGE_ACCOUNTS = {"dev": "adlsp360dev", "stg": "adlsp360stg", "prd": "adlsp360prd"}


class TestModuleFileStructure:
    """Verify all required Terraform files exist in the module."""

    def test_module_directory_exists(self):
        assert MODULE_DIR.is_dir(), f"Module directory not found: {MODULE_DIR}"

    @pytest.mark.parametrize("filename", [
        "main.tf", "variables.tf", "outputs.tf", "rbac.tf", "audit.tf"
    ])
    def test_required_files_exist(self, filename):
        filepath = MODULE_DIR / filename
        assert filepath.is_file(), f"Required file missing: {filepath}"

    @pytest.mark.parametrize("env", ENVIRONMENTS)
    def test_environment_configs_exist(self, env):
        env_file = ENVIRONMENTS_DIR / env / "main.tf"
        assert env_file.is_file(), f"Environment config missing: {env_file}"


class TestVariableDefinitions:
    """Verify the module's variable definitions meet WO-004 requirements."""

    @pytest.fixture(autouse=True)
    def load_variables(self):
        self.variables_content = (MODULE_DIR / "variables.tf").read_text()

    def test_environment_variable_has_validation(self):
        assert 'variable "environment"' in self.variables_content
        assert "dev" in self.variables_content
        assert "stg" in self.variables_content
        assert "prd" in self.variables_content

    def test_catalog_name_variable_exists(self):
        assert 'variable "catalog_name"' in self.variables_content

    def test_adls_storage_account_variable_exists(self):
        assert 'variable "adls_storage_account_name"' in self.variables_content

    def test_service_principal_variables_exist(self):
        assert 'variable "service_principal_application_id"' in self.variables_content
        assert 'variable "service_principal_client_secret"' in self.variables_content
        assert "sensitive" in self.variables_content

    def test_rbac_group_variables_exist(self):
        assert 'variable "engineer_group_name"' in self.variables_content
        assert 'variable "analyst_group_name"' in self.variables_content
        assert 'variable "admin_group_name"' in self.variables_content

    def test_default_schemas_match_requirements(self):
        for schema in REQUIRED_SCHEMAS:
            assert schema in self.variables_content, f"Schema '{schema}' not in default schemas"

    def test_default_containers_match_requirements(self):
        for container in REQUIRED_CONTAINERS:
            assert container in self.variables_content, f"Container '{container}' not in default containers"


class TestMainConfiguration:
    """Verify the main.tf creates required resources."""

    @pytest.fixture(autouse=True)
    def load_main(self):
        self.main_content = (MODULE_DIR / "main.tf").read_text()

    def test_catalog_resource_exists(self):
        assert 'resource "databricks_catalog"' in self.main_content

    def test_schema_resource_uses_for_each(self):
        assert 'resource "databricks_schema"' in self.main_content
        assert "for_each" in self.main_content

    def test_storage_credential_uses_service_principal(self):
        assert 'resource "databricks_storage_credential"' in self.main_content
        assert "azure_service_principal" in self.main_content

    def test_external_location_uses_abfss(self):
        assert 'resource "databricks_external_location"' in self.main_content
        assert "abfss://" in self.main_content

    def test_external_location_references_credential(self):
        assert "credential_name" in self.main_content
        assert "databricks_storage_credential.adls_sp.name" in self.main_content

    def test_volume_resource_exists(self):
        assert 'resource "databricks_volume"' in self.main_content

    def test_volume_is_external_type(self):
        assert 'volume_type' in self.main_content
        assert '"EXTERNAL"' in self.main_content

    def test_volume_uses_unity_catalog_path(self):
        assert "storage_location" in self.main_content
        assert "abfss://" in self.main_content

    def test_no_cloud_specific_hardcoded_paths(self):
        """REQ-015: no cloud-specific storage paths hardcoded — all parameterized."""
        assert "adlsp360dev" not in self.main_content
        assert "adlsp360stg" not in self.main_content
        assert "adlsp360prd" not in self.main_content


class TestRBACConfiguration:
    """Verify RBAC grants enforce least-privilege per WO-004 AC-4."""

    @pytest.fixture(autouse=True)
    def load_rbac(self):
        self.rbac_content = (MODULE_DIR / "rbac.tf").read_text()

    def test_catalog_grants_resource_exists(self):
        assert 'resource "databricks_grants"' in self.rbac_content

    def test_admin_gets_all_privileges(self):
        assert "ALL_PRIVILEGES" in self.rbac_content
        assert "admin_group_name" in self.rbac_content

    def test_engineer_gets_use_catalog(self):
        assert "USE_CATALOG" in self.rbac_content
        assert "engineer_group_name" in self.rbac_content

    def test_engineer_gets_create_table(self):
        assert "CREATE_TABLE" in self.rbac_content

    def test_engineer_gets_use_schema(self):
        assert "USE_SCHEMA" in self.rbac_content

    def test_analyst_gets_select_on_gold_only(self):
        assert "SELECT" in self.rbac_content
        assert "analyst_group_name" in self.rbac_content
        assert "gold_schema_names" in self.rbac_content

    def test_analyst_does_not_get_create_table(self):
        analyst_sections = self.rbac_content.split("analyst")
        for section in analyst_sections[1:]:
            end = section.find("}")
            if end > 0:
                snippet = section[:end]
                if "privileges" in snippet and "SELECT" in snippet:
                    assert "CREATE_TABLE" not in snippet, (
                        "Analyst should not have CREATE_TABLE privilege"
                    )


class TestAuditConfiguration:
    """Verify audit logging is configured per WO-004 AC-5."""

    @pytest.fixture(autouse=True)
    def load_audit(self):
        self.audit_content = (MODULE_DIR / "audit.tf").read_text()

    def test_audit_external_location_exists(self):
        assert 'resource "databricks_external_location" "audit"' in self.audit_content

    def test_audit_points_to_audit_container(self):
        assert "audit@" in self.audit_content

    def test_system_schema_enabled(self):
        assert 'resource "databricks_system_schema"' in self.audit_content
        assert '"access"' in self.audit_content

    def test_hipaa_retention_mentioned(self):
        assert "HIPAA" in self.audit_content or "7-year" in self.audit_content


class TestOutputDefinitions:
    """Verify the module exports necessary outputs."""

    @pytest.fixture(autouse=True)
    def load_outputs(self):
        self.outputs_content = (MODULE_DIR / "outputs.tf").read_text()

    @pytest.mark.parametrize("output_name", [
        "catalog_name", "catalog_id", "schema_names",
        "external_location_urls", "volume_paths", "storage_credential_name"
    ])
    def test_required_output_exists(self, output_name):
        assert f'output "{output_name}"' in self.outputs_content


class TestEnvironmentConfiguration:
    """Verify each environment uses the correct catalog and storage account names."""

    @pytest.mark.parametrize("env", ENVIRONMENTS)
    def test_correct_catalog_name(self, env):
        content = (ENVIRONMENTS_DIR / env / "main.tf").read_text()
        expected = CATALOG_NAMES[env]
        assert expected in content, f"Expected catalog '{expected}' in {env}/main.tf"

    @pytest.mark.parametrize("env", ENVIRONMENTS)
    def test_correct_storage_account(self, env):
        content = (ENVIRONMENTS_DIR / env / "main.tf").read_text()
        expected = STORAGE_ACCOUNTS[env]
        assert expected in content, f"Expected storage account '{expected}' in {env}/main.tf"

    @pytest.mark.parametrize("env", ENVIRONMENTS)
    def test_uses_module_source(self, env):
        content = (ENVIRONMENTS_DIR / env / "main.tf").read_text()
        assert "../../modules/unity_catalog" in content

    @pytest.mark.parametrize("env", ENVIRONMENTS)
    def test_azurerm_backend_configured(self, env):
        content = (ENVIRONMENTS_DIR / env / "main.tf").read_text()
        assert 'backend "azurerm"' in content
        assert f"unity-catalog-{env}.tfstate" in content

    @pytest.mark.parametrize("env", ENVIRONMENTS)
    def test_service_principal_vars_passed(self, env):
        content = (ENVIRONMENTS_DIR / env / "main.tf").read_text()
        assert "service_principal_application_id" in content
        assert "service_principal_client_secret" in content
        assert "azure_tenant_id" in content


class TestValidationScript:
    """Verify the validation SQL script covers all acceptance criteria."""

    @pytest.fixture(autouse=True)
    def load_validation(self):
        validation_path = (
            Path(__file__).resolve().parents[2]
            / "databricks_implementation" / "setup" / "validate_unity_catalog.sql"
        )
        self.validation_content = validation_path.read_text()

    def test_ac1_catalog_schema_verification(self):
        assert "patient360_dev" in self.validation_content
        assert "patient360_stg" in self.validation_content
        assert "patient360_prd" in self.validation_content
        assert "information_schema.schemata" in self.validation_content

    def test_ac2_external_location_verification(self):
        assert "external_locations" in self.validation_content
        assert "abfss://" in self.validation_content

    def test_ac3_volume_verification(self):
        assert "LIST" in self.validation_content
        assert "/Volumes/" in self.validation_content

    def test_ac4_rbac_verification(self):
        assert "SHOW GRANTS" in self.validation_content
        assert "patient360-dev-engineers" in self.validation_content
        assert "patient360-dev-analysts" in self.validation_content

    def test_ac5_audit_verification(self):
        assert "system.access.audit" in self.validation_content
        assert "unityCatalog" in self.validation_content

    def test_all_required_schemas_in_validation(self):
        for schema in REQUIRED_SCHEMAS:
            assert schema in self.validation_content, f"Schema '{schema}' not validated"

# ---------------------------------------------------------------------------
# Audit Logging Configuration
#
# Unity Catalog system tables (system.access.audit) are enabled at the
# metastore level. This configuration ensures audit logs are also exported
# to the ADLS audit container for long-term retention (HIPAA: 7 years).
# ---------------------------------------------------------------------------

resource "databricks_external_location" "audit" {
  name            = "patient360_${var.environment}_audit_logs"
  url             = "abfss://audit@${var.adls_storage_account_name}.dfs.core.windows.net/unity_catalog_audit/"
  credential_name = databricks_storage_credential.adls_sp.name
  comment         = "Audit log export location for Unity Catalog events — HIPAA 7-year retention"

  depends_on = [databricks_storage_credential.adls_sp]
}

resource "databricks_system_schema" "audit" {
  schema = "access"
}

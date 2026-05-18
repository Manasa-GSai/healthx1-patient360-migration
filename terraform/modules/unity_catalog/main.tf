terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

resource "databricks_catalog" "patient360" {
  name    = var.catalog_name
  comment = "Patient360 ${var.environment} catalog — Unity Catalog governed"

  properties = {
    environment = var.environment
    project     = "patient360"
    managed_by  = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

resource "databricks_schema" "schemas" {
  for_each = { for s in var.schemas : s.name => s }

  catalog_name = databricks_catalog.patient360.name
  name         = each.value.name
  comment      = each.value.comment

  properties = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Storage Credential (service principal auth to ADLS Gen2)
# ---------------------------------------------------------------------------

resource "databricks_storage_credential" "adls_sp" {
  name    = "patient360_${var.environment}_adls_credential"
  comment = "Service principal credential for ADLS Gen2 access — ${var.environment}"

  azure_service_principal {
    directory_id   = var.azure_tenant_id
    application_id = var.service_principal_application_id
    client_secret  = var.service_principal_client_secret
  }
}

# ---------------------------------------------------------------------------
# External Locations (one per ADLS container)
# ---------------------------------------------------------------------------

resource "databricks_external_location" "containers" {
  for_each = toset(var.adls_container_names)

  name            = "patient360_${var.environment}_${each.value}"
  url             = "abfss://${each.value}@${var.adls_storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.adls_sp.name
  comment         = "External location for ${each.value} container — ${var.environment}"

  depends_on = [databricks_storage_credential.adls_sp]
}

# ---------------------------------------------------------------------------
# Volumes (one per schema for cloud-agnostic file access)
# ---------------------------------------------------------------------------

resource "databricks_volume" "schema_volumes" {
  for_each = { for s in var.schemas : s.name => s }

  catalog_name     = databricks_catalog.patient360.name
  schema_name      = databricks_schema.schemas[each.key].name
  name             = "${each.key}_files"
  volume_type      = "EXTERNAL"
  storage_location = "abfss://${each.key}@${var.adls_storage_account_name}.dfs.core.windows.net/volumes/"
  comment          = "Unity Catalog volume for ${each.key} schema — cloud-agnostic storage access"

  depends_on = [
    databricks_schema.schemas,
    databricks_external_location.containers
  ]
}

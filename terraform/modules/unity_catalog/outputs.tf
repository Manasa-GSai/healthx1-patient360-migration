output "catalog_name" {
  description = "Name of the created Unity Catalog"
  value       = databricks_catalog.patient360.name
}

output "catalog_id" {
  description = "ID of the created Unity Catalog"
  value       = databricks_catalog.patient360.id
}

output "schema_names" {
  description = "Map of schema names to their full qualified names"
  value = {
    for k, v in databricks_schema.schemas : k => "${databricks_catalog.patient360.name}.${v.name}"
  }
}

output "external_location_urls" {
  description = "Map of external location names to their ADLS URLs"
  value = {
    for k, v in databricks_external_location.containers : k => v.url
  }
}

output "volume_paths" {
  description = "Map of volume names to their Unity Catalog paths"
  value = {
    for k, v in databricks_volume.schema_volumes : k => "/Volumes/${databricks_catalog.patient360.name}/${v.schema_name}/${v.name}"
  }
}

output "storage_credential_name" {
  description = "Name of the storage credential"
  value       = databricks_storage_credential.adls_sp.name
}

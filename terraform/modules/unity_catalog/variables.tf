variable "environment" {
  description = "Environment name: dev, stg, or prd"
  type        = string
  validation {
    condition     = contains(["dev", "stg", "prd"], var.environment)
    error_message = "Environment must be one of: dev, stg, prd."
  }
}

variable "catalog_name" {
  description = "Unity Catalog name for this environment (e.g. patient360_dev)"
  type        = string
}

variable "adls_storage_account_name" {
  description = "ADLS Gen2 storage account name (e.g. adlsp360dev)"
  type        = string
}

variable "adls_container_names" {
  description = "ADLS container names for external locations"
  type        = list(string)
  default     = ["landing", "bronze", "silver", "gold", "audit"]
}

variable "service_principal_application_id" {
  description = "Azure AD Application (client) ID for the storage credential service principal"
  type        = string
}

variable "service_principal_client_secret" {
  description = "Client secret for the storage credential service principal"
  type        = string
  sensitive   = true
}

variable "azure_tenant_id" {
  description = "Azure AD tenant ID"
  type        = string
}

variable "schemas" {
  description = "Schemas to create in the catalog"
  type = list(object({
    name    = string
    comment = string
  }))
  default = [
    { name = "bronze", comment = "Raw ingested data from source systems" },
    { name = "silver", comment = "Cleansed, conformed, and deduplicated data" },
    { name = "gold_billing", comment = "Revenue Cycle billing mart" },
    { name = "gold_quality", comment = "HEDIS clinical quality mart" },
    { name = "gold_care_mgmt", comment = "Care management gap queue" },
    { name = "reference", comment = "Canonical dimensions and lookup tables" }
  ]
}

variable "engineer_group_name" {
  description = "AAD group name for data engineers"
  type        = string
}

variable "analyst_group_name" {
  description = "AAD group name for analysts (read-only gold access)"
  type        = string
}

variable "admin_group_name" {
  description = "AAD group name for catalog administrators"
  type        = string
}

variable "gold_schema_names" {
  description = "Schema names that analysts get SELECT access to"
  type        = list(string)
  default     = ["gold_billing", "gold_quality", "gold_care_mgmt"]
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}

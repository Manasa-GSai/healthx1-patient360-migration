terraform {
  required_version = ">= 1.5"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-patient360-terraform"
    storage_account_name = "stp360tfstate"
    container_name       = "tfstate"
    key                  = "unity-catalog-prd.tfstate"
  }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  host = var.databricks_workspace_url
}

variable "databricks_workspace_url" {
  description = "URL of the production Databricks workspace"
  type        = string
}

variable "service_principal_application_id" {
  description = "Azure AD Application (client) ID for ADLS access"
  type        = string
}

variable "service_principal_client_secret" {
  description = "Client secret for the service principal"
  type        = string
  sensitive   = true
}

variable "azure_tenant_id" {
  description = "Azure AD tenant ID"
  type        = string
}

module "unity_catalog" {
  source = "../../modules/unity_catalog"

  environment                      = "prd"
  catalog_name                     = "patient360_prd"
  adls_storage_account_name        = "adlsp360prd"
  service_principal_application_id = var.service_principal_application_id
  service_principal_client_secret  = var.service_principal_client_secret
  azure_tenant_id                  = var.azure_tenant_id
  engineer_group_name              = "patient360-prd-operators"
  analyst_group_name               = "patient360-prd-analysts"
  admin_group_name                 = "patient360-prd-admins"

  tags = {
    environment = "production"
    project     = "patient360"
    cost_center = "data-engineering"
  }
}

output "catalog_name" {
  value = module.unity_catalog.catalog_name
}

output "schema_names" {
  value = module.unity_catalog.schema_names
}

output "volume_paths" {
  value = module.unity_catalog.volume_paths
}

output "external_location_urls" {
  value = module.unity_catalog.external_location_urls
}

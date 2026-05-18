# ---------------------------------------------------------------------------
# RBAC Grants — Least-Privilege Access Control
#
# Data Engineers:  USE CATALOG + USE SCHEMA + CREATE TABLE on all schemas
# Analysts:        USE CATALOG + USE SCHEMA + SELECT on gold schemas only
# Admins:          ALL PRIVILEGES on catalog
# ---------------------------------------------------------------------------

# --- Catalog-level grants ---

resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.patient360.name

  grant {
    principal  = var.admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = var.engineer_group_name
    privileges = ["USE_CATALOG"]
  }

  grant {
    principal  = var.analyst_group_name
    privileges = ["USE_CATALOG"]
  }
}

# --- Schema-level grants for engineers (all schemas) ---

resource "databricks_grants" "engineer_schemas" {
  for_each = { for s in var.schemas : s.name => s }

  schema = "${databricks_catalog.patient360.name}.${databricks_schema.schemas[each.key].name}"

  grant {
    principal  = var.engineer_group_name
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "CREATE_FUNCTION", "CREATE_VOLUME"]
  }

  grant {
    principal  = var.admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }
}

# --- Schema-level grants for analysts (gold schemas only, SELECT) ---

resource "databricks_grants" "analyst_gold_schemas" {
  for_each = toset(var.gold_schema_names)

  schema = "${databricks_catalog.patient360.name}.${databricks_schema.schemas[each.key].name}"

  grant {
    principal  = var.analyst_group_name
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

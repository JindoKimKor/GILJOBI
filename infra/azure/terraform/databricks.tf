# =============================================================================
# databricks.tf — Azure Databricks Workspace
# =============================================================================
# SPEC: Managed Spark, job cluster auto-create/destroy
# DAG uses DatabricksSubmitRunOperator → job cluster per run
# Workspace provides: Spark runtime, DBFS, cluster management, UI
# =============================================================================

resource "azurerm_databricks_workspace" "giljobi" {
  name                = "giljobi-databricks"
  location            = azurerm_resource_group.giljobi.location
  resource_group_name = azurerm_resource_group.giljobi.name
  sku                 = "standard"

  tags = {
    project = "giljobi"
    role    = "spark"
  }
}

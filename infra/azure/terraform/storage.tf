# =============================================================================
# storage.tf — Azure Blob Storage (Data Lake)
# =============================================================================
# SPEC: pipeline-data container
#   raw/skill-demand/     (Kaggle dataset)
#   processed/step1~4/    (intermediate parquet)
#   checkpoints/          (LLM batch checkpoints)
#
# Accessed by: VM (blobfuse2 mount) + Databricks (wasbs://)
# =============================================================================

resource "azurerm_storage_account" "giljobi" {
  name                     = var.storage_account_name
  location                 = azurerm_resource_group.giljobi.location
  resource_group_name      = azurerm_resource_group.giljobi.name
  account_tier             = "Standard"
  account_replication_type = "LRS"

  tags = {
    project = "giljobi"
    role    = "data-lake"
  }
}

resource "azurerm_storage_container" "pipeline_data" {
  name                  = "pipeline-data"
  storage_account_name  = azurerm_storage_account.giljobi.name
  container_access_type = "private"
}

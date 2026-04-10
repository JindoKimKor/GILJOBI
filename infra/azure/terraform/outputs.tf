# =============================================================================
# outputs.tf — Values displayed after terraform apply
# =============================================================================

output "vm_public_ip" {
  description = "Public IP of the Airflow VM"
  value       = azurerm_public_ip.giljobi.ip_address
}

output "ssh_command" {
  description = "SSH command to connect to VM"
  value       = "ssh ${var.admin_username}@${azurerm_public_ip.giljobi.ip_address}"
}

output "airflow_url" {
  description = "Airflow webserver URL"
  value       = "http://${azurerm_public_ip.giljobi.ip_address}:8090"
}

output "databricks_url" {
  description = "Databricks workspace URL"
  value       = azurerm_databricks_workspace.giljobi.workspace_url
}

output "storage_account_name" {
  description = "Blob Storage account name"
  value       = azurerm_storage_account.giljobi.name
}

output "storage_account_key" {
  description = "Blob Storage access key (sensitive)"
  value       = azurerm_storage_account.giljobi.primary_access_key
  sensitive   = true
}

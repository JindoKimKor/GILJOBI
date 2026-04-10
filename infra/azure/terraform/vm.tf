# =============================================================================
# vm.tf — Airflow VM (Docker Compose)
# =============================================================================
# SPEC: Standard_B2s (2 vCPU, 4GB RAM), Ubuntu 22.04 LTS
# Airflow only — Spark is on Databricks
# cloud-init installs Docker, clones repo, installs blobfuse2
# =============================================================================

resource "azurerm_linux_virtual_machine" "airflow" {
  name                  = "giljobi-airflow-vm"
  location              = azurerm_resource_group.giljobi.location
  resource_group_name   = azurerm_resource_group.giljobi.name
  size                  = var.vm_size
  admin_username        = var.admin_username
  network_interface_ids = [azurerm_network_interface.giljobi.id]

  admin_ssh_key {
    username   = var.admin_username
    public_key = file(pathexpand(var.ssh_public_key_path))
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
    disk_size_gb         = 32
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  custom_data = base64encode(file("${path.module}/cloud-init.yaml"))

  tags = {
    project = "giljobi"
    role    = "airflow"
  }
}

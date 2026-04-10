# =============================================================================
# main.tf — Azure Provider + Resource Group
# =============================================================================
# SPEC: infra/azure/SPEC.md
# Resource Group: giljobi-rg
# Region: koreacentral (closest to team)
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
  }
}

provider "azurerm" {
  features {}
}

# =============================================================================
# Resource Group — all resources live here
# terraform destroy deletes this group → everything inside gets deleted
# =============================================================================
resource "azurerm_resource_group" "giljobi" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    project     = "giljobi"
    environment = "demo"
    managed_by  = "terraform"
  }
}

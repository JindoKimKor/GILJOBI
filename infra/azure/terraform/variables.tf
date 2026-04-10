# =============================================================================
# variables.tf — Input variables
# =============================================================================
# Override in terraform.tfvars (gitignored) or -var flags
# =============================================================================

# ── Resource Group ───────────────────────────────────────────
variable "resource_group_name" {
  description = "Name of the Azure resource group"
  type        = string
  default     = "giljobi-rg"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}

# ── VM ───────────────────────────────────────────────────────
variable "vm_size" {
  description = "VM size for Airflow (scale down later if needed)"
  type        = string
  default     = "Standard_B4ms"
}

variable "admin_username" {
  description = "SSH admin username"
  type        = string
  default     = "azureuser"
}

variable "ssh_public_key_path" {
  description = "Path to SSH public key file"
  type        = string
  default     = "~/.ssh/giljobi_azure.pub"
}

variable "admin_ip" {
  description = "Admin IP for SSH access (CIDR). Use 'x.x.x.x/32' for single IP, '*' for any."
  type        = string
  default     = "*"
}

# ── Storage ──────────────────────────────────────────────────
variable "storage_account_name" {
  description = "Globally unique storage account name (lowercase, no hyphens)"
  type        = string
  default     = "giljobistorage"
}

# =============================================================================
# network.tf — VNet, Subnet, NSG, Public IP
# =============================================================================
# SPEC: giljobi-vnet (10.0.0.0/16), giljobi-subnet (10.0.1.0/24)
# NSG: SSH (22), HTTP (80), HTTPS (443)
# Public IP: static, for Airflow VM
# =============================================================================

# =============================================================================
# Virtual Network
# =============================================================================
resource "azurerm_virtual_network" "giljobi" {
  name                = "giljobi-vnet"
  location            = azurerm_resource_group.giljobi.location
  resource_group_name = azurerm_resource_group.giljobi.name
  address_space       = ["10.0.0.0/16"]
}

resource "azurerm_subnet" "giljobi" {
  name                 = "giljobi-subnet"
  resource_group_name  = azurerm_resource_group.giljobi.name
  virtual_network_name = azurerm_virtual_network.giljobi.name
  address_prefixes     = ["10.0.1.0/24"]
}

# =============================================================================
# Network Security Group
# =============================================================================
resource "azurerm_network_security_group" "giljobi" {
  name                = "giljobi-nsg"
  location            = azurerm_resource_group.giljobi.location
  resource_group_name = azurerm_resource_group.giljobi.name

  security_rule {
    name                       = "SSH"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = var.admin_ip
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "HTTP"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "80"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "HTTPS"
    priority                   = 1003
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "Airflow"
    priority                   = 1004
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "8090"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

resource "azurerm_subnet_network_security_group_association" "giljobi" {
  subnet_id                 = azurerm_subnet.giljobi.id
  network_security_group_id = azurerm_network_security_group.giljobi.id
}

# =============================================================================
# Public IP (static — survives VM stop/start)
# =============================================================================
resource "azurerm_public_ip" "giljobi" {
  name                = "giljobi-ip"
  location            = azurerm_resource_group.giljobi.location
  resource_group_name = azurerm_resource_group.giljobi.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

# =============================================================================
# Network Interface (VM에 연결됨)
# =============================================================================
resource "azurerm_network_interface" "giljobi" {
  name                = "giljobi-nic"
  location            = azurerm_resource_group.giljobi.location
  resource_group_name = azurerm_resource_group.giljobi.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.giljobi.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.giljobi.id
  }
}

resource "random_string" "suffix" {
  length  = 4
  upper   = false
  special = false
}

# Kafka endpoint exige o tier Standard (Basic nao suporta)
resource "azurerm_eventhub_namespace" "ns" {
  name                = "evhns-fininsight-${var.environment}-${random_string.suffix.result}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "Standard"
  capacity            = 1
}

resource "azurerm_eventhub" "cotacoes" {
  name                = "cotacoes-tempo-real"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  resource_group_name = var.resource_group_name
  partition_count     = 2
  message_retention   = 1
}

resource "azurerm_eventhub_namespace_authorization_rule" "databricks" {
  name                = "databricks"
  namespace_name      = azurerm_eventhub_namespace.ns.name
  resource_group_name = var.resource_group_name
  listen              = true
  send                = true
  manage              = false
}

# Secrets lidos pelo producer e pelo pipeline. O scope "fininsight" e criado pelo
# notebook 00_configurar_secrets.py (ele precisa existir antes deste apply).
resource "databricks_secret" "eventhub_connection_string" {
  scope        = var.secret_scope
  key          = "eventhub-connection-string"
  string_value = azurerm_eventhub_namespace_authorization_rule.databricks.primary_connection_string
}

resource "databricks_secret" "eventhub_bootstrap" {
  scope        = var.secret_scope
  key          = "eventhub-bootstrap"
  string_value = "${azurerm_eventhub_namespace.ns.name}.servicebus.windows.net:9093"
}

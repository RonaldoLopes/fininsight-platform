# Credencial de storage: usa a identidade gerenciada do Access Connector
resource "databricks_storage_credential" "external" {
  name = "cred-fininsight-${var.environment}"
  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
}

# External locations (container -> credencial)
resource "databricks_external_location" "bronze" {
  name            = "loc-bronze-${var.environment}"
  url             = "abfss://bronze@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.external.name
}

resource "databricks_external_location" "catalog_root" {
  name            = "loc-catalog-root-${var.environment}"
  url             = "abfss://unitycatalog@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.external.name
}

# NOVO: container landing, onde os notebooks de coleta gravam os arquivos brutos
resource "databricks_external_location" "landing" {
  name            = "loc-landing-${var.environment}"
  url             = "abfss://landing@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.external.name
}

# Catalogo e schemas
resource "databricks_catalog" "fininsight" {
  name         = "fininsight_${var.environment}"
  comment      = "Catalogo da plataforma FinInsight"
  storage_root = databricks_external_location.catalog_root.url

  depends_on = [databricks_external_location.catalog_root]
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.fininsight.name
  name         = "bronze"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.fininsight.name
  name         = "silver"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.fininsight.name
  name         = "gold"
}

# NOVO: schema landing + volume externo "raw" (caminho /Volumes/<catalogo>/landing/raw)
resource "databricks_schema" "landing" {
  catalog_name = databricks_catalog.fininsight.name
  name         = "landing"
}

resource "databricks_volume" "landing_raw" {
  catalog_name     = databricks_catalog.fininsight.name
  schema_name      = databricks_schema.landing.name
  name             = "raw"
  volume_type      = "EXTERNAL"
  storage_location = "abfss://landing@${var.storage_account_name}.dfs.core.windows.net/raw"
  comment          = "Arquivos brutos coletados das APIs (Auto Loader / COPY INTO leem daqui)"

  depends_on = [databricks_external_location.landing]
}

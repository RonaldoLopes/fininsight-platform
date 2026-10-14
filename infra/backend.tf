terraform {
    required_version = ">= 1.7.0"
    backend "azurerm" {
      resource_group_name  = "rg-tfstate-fininsight"
      storage_account_name = "tfstatefininsight002"
      container_name       = "tfstate"
      key                  = "fininsight.tfstate"
    }
    required_providers {
      azurerm = { source = "hashicorp/azurerm", version = "~> 3.100" }
      databricks = { source = "databricks/databricks", version = "~> 1.50" }
    }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  host = module.databricks.workspace_url
  azure_workspace_resource_id = module.databricks.workspace_resource_id
}
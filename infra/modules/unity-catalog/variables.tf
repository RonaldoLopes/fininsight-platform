variable "environment" {
  type = string
}

variable "location" {
  type = string
}

variable "storage_account_name" {
  type = string
}

variable "access_connector_id" {
  type = string
}

variable "metastore_id" {
  description = "ID do metastore existente na regiao (1 por regiao/conta). Associacao: Account Console."
  type        = string
}

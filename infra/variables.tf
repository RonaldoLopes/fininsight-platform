variable "environment" {
  description = "Ambiente: dev, hml ou prod"
  type        = string
}

variable "location" {
  description = "Regiao Azure"
  type        = string
  default     = "eastus2"
}

variable "metastore_id" {
  description = "ID do metastore Unity Catalog ja existente na conta/regiao (copie do Account Console)"
  type        = string
}

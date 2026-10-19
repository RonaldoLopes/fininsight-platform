variable "environment" {
  type = string
}

variable "location" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "secret_scope" {
  description = "Scope de secrets do Databricks (criado pelo notebook 00)"
  type        = string
  default     = "fininsight"
}

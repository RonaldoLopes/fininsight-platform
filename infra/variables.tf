variable "environment" {
  description = "Ambiente: dev, hml ou prod"
  type        = string
}
variable "location" {
  description = "Regiao Azure"
  type        = string
  default     = "eastus2"
}
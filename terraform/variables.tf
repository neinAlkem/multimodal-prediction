variable "credential" {
    description = "Project credentials account"
    default = "${HOME}/project/credentials/credential.json"
}

variable "project" {
    description = "Project ID"
    default =  "project-big-data-461104"
}

variable "region" {
  description = "Datalake Region"
  default     = "asia-southeast2"
}

variable "location" {
  description = "Default location"
  default     = "ASIA"
}

variable "GCS_storage_class" {
  description = "Storage Default Class"
  default     = "STANDART"
}
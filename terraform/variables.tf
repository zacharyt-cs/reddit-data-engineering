locals {
  data_lake_bucket = "datalake"
}

variable "project" {
  default     = "de-r-stocks"
  description = "GCP project ID"
}

variable "region" {
  type        = string
  default     = "asia-southeast1"
  description = "Region for GCP resources"
}

variable "storage_class" {
  default     = "STANDARD"
  description = "Storage class type for bucket"
}

variable "BQ_DATASET" {
  type        = string
  default     = "stocks_data"
  description = "BigQuery dataset that raw data from GCS will be written to"
}

variable "credentials" {
  type        = string
  default     = "/home/ztmj96/.google/credentials/de-r-stocks.json"
  description = "Path for GCP account credentials"
}



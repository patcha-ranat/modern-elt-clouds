# Global

variable "service_account_email" {
  type      = string
  sensitive = true
}

variable "project_id" {
  type      = string
  sensitive = true
}

variable "location" {
  type      = string
  sensitive = true
}

variable "environment" {
  type    = string
  default = "dev"
}

# Cloud Storage

variable "bucket_landing_name" {
  type    = string
  default = "kde_finance_landing"
}

variable "bucket_staging_name" {
  type    = string
  default = "kde_finance_staging"
}

variable "bucket_curated_name" {
  type    = string
  default = "kde_finance_curated"
}

# BigQuery

variable "bq_dataset_silver" {
  type    = string
  default = "kde_finance_silver"
}

variable "bq_dataset_gold" {
  type    = string
  default = "kde_finance_gold"
}

# Firestore

variable "firestore_db_id" {
  type    = string
  default = "(default)"
}

variable "firestore_type" {
  type    = string
  default = "FIRESTORE_NATIVE"
}

variable "firestore_deletion_policy" {
  type    = string
  default = "DELETE"
}
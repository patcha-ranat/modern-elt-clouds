terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.14.1"
    }
  }
}

# Provider

provider "google" {
  impersonate_service_account = var.service_account_email
  project                     = var.project_id
  region                      = var.location
}

# Cloud Storage

resource "google_storage_bucket" "bucket_landing" {
  name          = var.bucket_landing_name
  location      = var.location
  force_destroy = true
}

resource "google_storage_bucket" "bucket_staging" {
  name          = var.bucket_staging_name
  location      = var.location
  force_destroy = true
}

resource "google_storage_bucket" "bucket_curated" {
  name          = var.bucket_curated_name
  location      = var.location
  force_destroy = true
}

# BigQuery

resource "google_bigquery_dataset" "dataset_silver" {
  dataset_id                 = var.bq_dataset_silver
  location                   = var.location
  delete_contents_on_destroy = true

  labels = {
    env        = "dev"
    managed_by = "terraform"
  }
}

resource "google_bigquery_dataset" "dataset_gold" {
  dataset_id                 = var.bq_dataset_gold
  location                   = var.location
  delete_contents_on_destroy = true

  labels = {
    env        = "dev"
    managed_by = "terraform"
  }
}

# Firestore

resource "google_firestore_database" "database" {
  project     = var.project_id
  name        = var.firestore_db_id
  location_id = var.location
  type        = var.firestore_type
}
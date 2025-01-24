terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.84.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {}

# S3 Bucket

resource "aws_s3_bucket" "landing_bucket" {
  bucket        = var.landing_bucket_name
  force_destroy = true

  tags = {
    Environment = var.env
  }
}

resource "aws_s3_bucket" "staging_bucket" {
  bucket        = var.staging_bucket_name
  force_destroy = true

  tags = {
    Environment = var.env
  }
}

resource "aws_s3_bucket" "persist_bucket" {
  bucket        = var.persist_bucket_name
  force_destroy = true

  tags = {
    Environment = var.env
  }
}

# DynamoDB

resource "aws_dynamodb_table" "cards_data" {
  name         = var.dynamodb_table_schema_card_data.table_name
  billing_mode = var.dynamodb_billing_mode
  hash_key     = var.dynamodb_table_schema_card_data.partition_key
  range_key    = var.dynamodb_table_schema_card_data.sort_key

  attribute {
    name = var.dynamodb_table_schema_card_data.partition_key
    type = var.dynamodb_table_schema_card_data.partition_key_type
  }

  attribute {
    name = var.dynamodb_table_schema_card_data.sort_key
    type = var.dynamodb_table_schema_card_data.sort_key_type
  }

  lifecycle {
    prevent_destroy = false
  }

  tags = {
    Name        = var.domain
    Environment = var.env
  }
}
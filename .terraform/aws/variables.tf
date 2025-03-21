# Global

variable "profile" {
  type        = string
  description = "sso profile, check ~/.aws/config"
  sensitive   = true
}

variable "env" {
  type        = string
  description = "resouce environment"
  default     = "dev"
}

variable "domain" {
  type        = string
  description = "business domain"
  default     = "finance"
}

# S3 (Data Lake)

variable "landing_bucket_name" {
  type    = string
  default = "kde-landing"
}

variable "staging_bucket_name" {
  type    = string
  default = "kde-staging"
}

variable "persist_bucket_name" {
  type    = string
  default = "kde-persist"
}

variable "artifact_bucket_name" {
  type        = string
  description = "storing code, config, and etc."
  default     = "kde-artifact"
}

# DynamoDB (NoSQL)

variable "dynamodb_billing_mode" {
  type        = string
  description = "Pay-as-you-go OR buying a cluster"
  default     = "PAY_PER_REQUEST" # PROVISIONED
}

variable "dynamodb_table_schema_card_data" {
  description = "DynamoDB Table schema for table: card-data / type s=string, n=number"
  type = object({
    table_name         = string
    partition_key      = string
    partition_key_type = string
    sort_key           = string
    sort_key_type      = string
  })
  default = {
    table_name         = "cards-data"
    partition_key      = "id"
    partition_key_type = "N"
    sort_key           = "client_id"
    sort_key_type      = "N"
  }
}
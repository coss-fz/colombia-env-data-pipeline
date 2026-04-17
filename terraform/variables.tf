variable "project_id" {
  description = "GCP project ID that owns every resource in this stack."
  type        = string
}

variable "region" {
  description = "GCP region. 'southamerica-east1' is closest to Colombia; 'us-central1' is cheaper."
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Globally unique name for the raw parquet data lake bucket."
  type        = string
}

variable "dataset_prefix" {
  description = "Prefix applied to all BigQuery dataset IDs created here."
  type        = string
  default     = "colombia_env"
}

variable "create_service_account" {
  description = "Create a dedicated service account with storage+BQ roles. Set false if you prefer to manage identity separately."
  type        = bool
  default     = false
}

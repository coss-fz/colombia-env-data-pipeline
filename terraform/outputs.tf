output "gcs_bucket" {
  description = "Name of the raw data lake bucket. Feed this into GCS_BUCKET env vars."
  value       = google_storage_bucket.data_lake.name
}

output "raw_dataset" {
  description = "BigQuery dataset holding external tables that point at GCS."
  value       = google_bigquery_dataset.raw.dataset_id
}

output "service_account_email" {
  description = "Service account email (empty string if create_service_account = false)."
  value       = var.create_service_account ? google_service_account.pipeline_sa[0].email : ""
}

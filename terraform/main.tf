###############################################################################
# Colombia Environmental Data Pipeline — GCP infrastructure
# Provisions three things:
#   1. A regional GCS bucket for raw parquet files (landing zone)
#   2. A "raw" BigQuery dataset for external tables pointing at that bucket
###############################################################################


terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.40"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}


# ---------- API enablement ----------------------------------------------------
resource "google_project_service" "dataproc" {
  project            = var.project_id
  service            = "dataproc.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  project            = var.project_id
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}


# ---------- Raw data lake -----------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = false # tf destroy should NOT nuke real data by accident

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  # Autoclass moves cold objects to cheaper tiers automatically. With parquet
  # partitions we write once and read many, which matches its heuristics well.
  autoclass {
    enabled = true
  }

  # Versioning is cheap insurance against a bad overwrite.
  versioning {
    enabled = true
  }

  labels = {
    project = "colombia-env"
    managed = "terraform"
  }
}


# ---------- BigQuery: raw (external tables read from GCS) ---------------------
resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "${var.dataset_prefix}_raw"
  friendly_name              = "Colombia Env — Raw"
  description                = "External tables over parquet files in the GCS landing zone"
  location                   = var.region
  delete_contents_on_destroy = false

  labels = {
    project = "colombia-env"
    layer   = "raw"
    managed = "terraform"
  }
}

# ---------- Optional: dedicated service account for the pipeline --------------
# Only created when create_service_account = true, so the default flow stays
# simple for local/personal GCP accounts but a "do it right" path exists.

resource "google_service_account" "pipeline_sa" {
  count        = var.create_service_account ? 1 : 0
  account_id   = "colombia-env"
  display_name = "Colombia Env Pipeline"
  description  = "Runs Kestra ingestion + dbt + Spark jobs"
}

resource "google_project_iam_member" "sa_storage" {
  count   = var.create_service_account ? 1 : 0
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline_sa[0].email}"
}

resource "google_project_iam_member" "sa_bq_data" {
  count   = var.create_service_account ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_sa[0].email}"
}

resource "google_project_iam_member" "sa_bq_job" {
  count   = var.create_service_account ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa[0].email}"
}

# The SA also implicitly needs `roles/iam.serviceAccountUser` on *itself* — we
# grant that by binding the SA as a member on its own resource.
resource "google_project_iam_member" "sa_dataproc_worker" {
  count   = var.create_service_account ? 1 : 0
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.pipeline_sa[0].email}"
}

resource "google_service_account_iam_member" "sa_act_as_self" {
  count              = var.create_service_account ? 1 : 0
  service_account_id = google_service_account.pipeline_sa[0].name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.pipeline_sa[0].email}"
}

resource "google_project_iam_member" "sa_dataproc_editor" {
  count   = var.create_service_account ? 1 : 0
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.pipeline_sa[0].email}"
}
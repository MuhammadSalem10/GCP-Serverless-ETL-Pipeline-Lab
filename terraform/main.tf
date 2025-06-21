terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.40.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  zone        = var.zone
  credentials = "../keys/salem-463409-0daf3db86839.json"
}

resource "google_project_service" "required_apis" {
  for_each = toset([
    "compute.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "dataflow.googleapis.com",
    "composer.googleapis.com",
    "cloudbuild.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com"
  ])

  service = each.value
  project = var.project_id

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_storage_bucket" "source_bucket" {
  name     = "${var.project_id}-etl-source-data"
  location = var.region

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_bigquery_dataset" "sales_dataset" {
  dataset_id  = "sales_analytics"
  location    = var.region
  description = "Dataset for sales analytics pipeline"

  # Security best practice: Set access controls
  access {
    role          = "OWNER"
    user_by_email = data.google_client_openid_userinfo.me.email
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_bigquery_table" "sales_data" {
  dataset_id          = google_bigquery_dataset.sales_dataset.dataset_id
  table_id            = "sales_data"
  deletion_protection = false

  schema = jsonencode([
    {
      name = "id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "product"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "price"
      type = "FLOAT"
      mode = "REQUIRED"
    },
    {
      name = "quantity"
      type = "INTEGER"
      mode = "REQUIRED"
    },
    {
      name = "sale_date"
      type = "DATE"
      mode = "REQUIRED"
    },
    {
      name = "total_sale"
      type = "FLOAT"
      mode = "REQUIRED"
    }
  ])
}

resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-pipeline-sa"
  display_name = "Dataflow Pipeline Service Account"
  description  = "Service account for Dataflow ETL pipeline"
}

resource "google_service_account" "composer_sa" {
  account_id   = "composer-airflow-sa"
  display_name = "Composer Airflow Service Account"
  description  = "Service account for Cloud Composer environment"
}

resource "google_project_iam_member" "dataflow_permissions" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/storage.objectViewer",
    "roles/storage.objectCreator",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser"
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "composer_permissions" {
  for_each = toset([
    "roles/composer.worker",
    "roles/dataflow.admin",
    "roles/bigquery.admin",
    "roles/storage.admin"
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_composer_environment" "etl-composer" {
  name   = "etl-pipeline-composer"
  region = var.region
  config {
    software_config {
      image_version = "composer-3-airflow-2"

      pypi_packages = {
        "apache-beam" = ">=2.50.0"
      }
    }

    node_config {
      service_account = google_service_account.composer_sa.email
    }
  }

  depends_on = [
    google_project_service.required_apis,
    google_project_iam_member.composer_permissions
  ]
}

# Get current user info for permissions
data "google_client_openid_userinfo" "me" {}

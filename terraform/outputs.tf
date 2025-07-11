output "source_bucket_name" {
  description = "Name of the source data bucket"
  value       = google_storage_bucket.source_bucket.name
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.sales_dataset.dataset_id
}

output "bigquery_table_id" {
  description = "BigQuery table ID"
  value       = google_bigquery_table.sales_data.table_id
}

output "dataflow_service_account" {
  description = "Dataflow service account email"
  value       = google_service_account.dataflow_sa.email
}

output "composer_uri" {
  description = "Composer Airflow URI"
  value       = google_composer_environment.etl-composer.config.0.airflow_uri
}

output "composer_dag_gcs_prefix" {
  description = "GCS prefix for Composer DAGs"
  value       = google_composer_environment.etl-composer.config.0.dag_gcs_prefix
}

variable "project_id" {
  description = "Project ID from GCP"
  type        = string
}

variable "region" {
  description = "Region where the resources will be created."
  type        = string
  default     = "us-central1"
}

variable "landing_bucket_name" {
  description = "Unique name of the GCS bucket (Landing)"
  type        = string
}

variable "bronze_bucket_name" {
  description = "Unique name of the GCS bucket (Bronze)"
  type        = string
}

variable "gold_bucket_name" {
  description = "Unique name of the GCS bucket (Gold)"
  type        = string
}

variable "landing_pipeline_sa" {
  description = "Service Account ID (Landing)"
  type        = string
}

variable "bronze_pipeline_sa" {
  description = "Service Account ID (Bronze)"
  type        = string
}

variable "gold_pipeline_sa" {
  description = "Service Account ID (Gold)"
  type        = string
}

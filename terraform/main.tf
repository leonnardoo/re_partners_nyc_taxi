# Config for GCP
provider "google" {
  project = var.project_id
  region  = var.region
}

# Bucket for Bronze layer
resource "google_storage_bucket" "bronze_layer" {
  name          = var.bronze_bucket_name
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true # Recommended for simplifying permissions management via IAM.

  # Lifecycle Rule: Delete old files (5 years)
  lifecycle_rule {
    condition {
      age            = 1825                    # age in days (365*5)
      matches_prefix = ["nyc_taxi/trip_data/"] # Applies only to this path.
    }
    action {
      type = "Delete"
    }
  }

}

# Bucket for Gold layer
resource "google_storage_bucket" "gold_layer" {
  name          = var.gold_bucket_name
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true # Recommended for simplifying permissions management via IAM.

  # Lifecycle Rule: Delete old files (5 years)
  lifecycle_rule {
    condition {
      age            = 1825                    # age in days (365*5)
      matches_prefix = ["nyc_taxi/fact_trip/"] # Applies only to this path.
    }
    action {
      type = "Delete"
    }
  }

}

# Creating the Folder Structure
resource "google_storage_bucket_object" "trip_data_folder_structure" {
  name    = "nyc_taxi/trip_data/"
  content = " " # Empty content to initialize the "folder"
  bucket  = google_storage_bucket.bronze_layer.name
}

# Creating the Folder Structure
resource "google_storage_bucket_object" "taxi_zone_folder_structure" {
  name    = "nyc_taxi/taxi_zone/"
  content = " " # Empty content to initialize the "folder"
  bucket  = google_storage_bucket.bronze_layer.name
}

# Creating the Folder Structure
resource "google_storage_bucket_object" "fact_trip_folder_structure" {
  name    = "nyc_taxi/fact_trip/"
  content = " " # Empty content to initialize the "folder"
  bucket  = google_storage_bucket.gold_layer.name
}

# Creating the Folder Structure
resource "google_storage_bucket_object" "dim_location_folder_structure" {
  name    = "nyc_taxi/dim_location/"
  content = " " # Empty content to initialize the "folder"
  bucket  = google_storage_bucket.gold_layer.name
}

# Service Account for Pipeline (Bronze)
resource "google_service_account" "bronze_pipeline_sa" {
  account_id   = var.bronze_pipeline_sa
  display_name = "Service Account for Data Processing (Bronze)"
}

# Service Account for Pipeline (Gold)
resource "google_service_account" "gold_pipeline_sa" {
  account_id   = var.gold_pipeline_sa
  display_name = "Service Account for Data Processing (Gold)"
}

# Assignment of permissions (IAM)
resource "google_storage_bucket_iam_member" "bronze_admin" {
  bucket = google_storage_bucket.bronze_layer.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.bronze_pipeline_sa.email}"
}

# Assignment of permissions (IAM)
resource "google_storage_bucket_iam_member" "gold_admin" {
  bucket = google_storage_bucket.gold_layer.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.gold_pipeline_sa.email}"
}
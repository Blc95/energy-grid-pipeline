### VARIABLES ###
variable "region" {
  description = "Default GCP region/location for project resources"
  type        = string
  default     = "europe-west1"
}

variable "project" {
  description = "Energy grid pipeline project"
  type        = string
  default     = "energy-grid-pipeline"
}



terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}


resource "google_storage_bucket" "raw" {
  name          = "energy-grid-pipeline-raw"
  location      = var.region
  force_destroy = true

  public_access_prevention = "enforced"
}

resource "google_bigquery_dataset" "dataset_raw" {
  dataset_id                  = "raw"
  description                 = "Energy prices with timestamps"
  location                    = var.region
}


terraform {
    required_providers {

        google = {
            source = "hashicorp/google"
            version = "6.8.0"
        }
    }
}

provider "google" {
    project = var.project
    region = var.region
    credentials = file(var.credential)
  
}

resource "google_storage_bucket" "project-abd" {
  name          = "project-abd"
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 60
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
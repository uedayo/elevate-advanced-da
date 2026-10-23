terraform {
  required_version = ">= 1.3.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.46.0"
    }

    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 7.46.0"
    }

    external = {
      source  = "hashicorp/external"
      version = "~> 2.3"
    }

    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }

    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "gcs" {
    bucket = ""
    prefix = "streaming"
  }
}

provider "google" {
  project = var.project_id
  region  = var.gcp_region
}

provider "google-beta" {
  project = var.project_id
  region  = var.gcp_region
}

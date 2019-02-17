terraform {
  backend "gcs" {
    bucket  = "dataloom-infra-prod"
    prefix  = "terraform/state"
  }
}

provider "google" {
  project     = "dataloom"
  region      = "us-central1"
}

// Dataproc workflow
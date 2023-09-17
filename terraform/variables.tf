locals {
  data_lake_bucket = "de_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "the-data-engineering-project"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west3"
  type = string
}

variable "credentials" {
  description = "Path to the credential json file"
  default = "the-data-engineering-project-8223d009c2b0.json"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET_STAGING" {
  description = "BigQuery Dataset that serves as staging layer where validated dats tables are injested into"
  type = string
  default = "de_dataset_staging"
}

variable "BQ_DATASET_WAREHOUSE" {
  description = "BigQuery Dataset that serves as data warehouse layer where transformed data table is stored, ready for analytical use"
  type = string
  default = "de_dataset_warehouse"
}

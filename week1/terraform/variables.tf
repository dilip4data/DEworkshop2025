# variable "credentials" {
#   description = "My Credentials"
#   default     = "/c/Users/dilip/.gcp/dtcde-2025-b2fcc1633410.json"
#   #ex: if you have a directory where this file is called keys with your service account json file
#   #saved there as my-creds.json you could use default = "./keys/my-creds.json"
# }


variable "project" {
  description = "Project"
  default     = "dtcde-2025"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "us-central1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "trips_data_all"
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type        = string
  default     = "ny_trips"
}

locals {
  data_lake_bucket = "datalake"
}

# variable "gcs_bucket_name" {
#   description = "My Storage Bucket Name"
#   #Update the below to a unique bucket name
#   default     = "dtc_data_lake"
# }

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
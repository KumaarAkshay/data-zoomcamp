variable "credentials" {
  description = "My Credentials"
  type        = string
}

variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The region where the bucket will be created"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The zone within the region"
  type        = string
  default     = "us-central1-a"
}

variable "machine_type" {
  description = "type of matchine to be created"
  type        = string
}

variable "image" {
  description = "type of image to be created"
  type        = string
  default     = "debian-cloud/debian-11"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "tf_dataset"
}


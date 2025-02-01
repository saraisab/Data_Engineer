variable "credentials" {
  description = "My Credentials"
  default     = "./keys/credentials.json"
}

variable "project" {
  description = "Project"
  default     = "celtic-surface-447817-d0"
}

variable "location" {
  description = "Project Location"
  default     = "europe-southwest1"
}

variable "region" {
  description = "Region"
  default     = "europe-southwest1"
}

variable "bq_dataset_name" {
  description = "Dataset_ej1kestra_cgpt"
  default     = "dataset_ex_kestra"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "bucker_ejercicio_1_kestra_cgpt"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "file_csv" {
  description = "file csv name"
  default = "weather_data.csv"
}

variable "path_csv" {
  description = "Path to the csv file"
  default = "./files/weather_data.csv"
}
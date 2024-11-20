variable "ingest_bucket_arn" {
  default = "ingest_bucket_arn"
}

data "aws_ssm_parameter" "ingest_bucket_arn" {
  name = var.ingest_bucket_arn
}

variable "ingest_bucket_name" {
  default = "ingest_bucket_name"
}

data "aws_ssm_parameter" "ingest_bucket_name" {
  name = var.ingest_bucket_name
}

variable "transform_bucket_arn" {
  default = "transform_bucket_arn"
}

data "aws_ssm_parameter" "transform_bucket_arn" {
  name = var.transform_bucket_arn
}

variable "transform_bucket_name" {
  default = "transform_bucket_name"
}

data "aws_ssm_parameter" "transform_bucket_name" {
  name = var.transform_bucket_name
}

variable "ingest_lambda_name" {
  default = "ingest"
}

variable "transform_lambda_name" {
  default = "transform"
}

variable "python_runtime" {
  default = "python3.12"
}

variable "state_machine_name" {
  default = "totesys_state_machine"
}

variable "default_timeout" {
  default = 30
  type = number
}

variable "credentials_secret_arn" {
  default = "secret_db_credentials"
}

data "aws_ssm_parameter" "credentials_secret_arn" {
  name = var.credentials_secret_arn
}
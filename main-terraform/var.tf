# INGEST S3 BUCKET #######################################

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

# TRANSFORM S3 BUCKET ####################################

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

# LAMBDA FUNC NAMES #######################################

variable "ingest_lambda_name" {
  default = "ingest"
}

variable "transform_lambda_name" {
  default = "transform"
}

variable "load_lambda_name" {
  default = "load"
}

# STATE MACHINE NAME ######################################

variable "state_machine_name" {
  default = "totesys_state_machine"
}

# LAMBDA FUNC DEFAULTS ####################################

variable "python_runtime" {
  default = "python3.12"
}

variable "default_timeout" {
  default = 150
  type = number
}

# SECRETS ##################################################

variable "credentials_secret_arn" {
  default = "secret_db_credentials"
}

data "aws_ssm_parameter" "credentials_secret_arn" {
  name = var.credentials_secret_arn
}

variable "warehouse_credentials_secret_arn" {
  default = "secret_warehouse_credentials"
}

data "aws_ssm_parameter" "warehouse_credentials_secret_arn" {
  name = var.warehouse_credentials_secret_arn
}

# STATIC PATHS #############################################

variable "static_address_path" {
  default = "static/address.parquet"
}

variable "static_department_path" {
  default = "static/department.parquet"
}
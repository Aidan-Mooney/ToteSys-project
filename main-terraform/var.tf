variable "ingest_bucket_name" {
  default = "ingest_bucket_id"
}

data "aws_ssm_parameter" "ingest_bucket_name" {
  name = var.ingest_bucket_name
}

variable "transform_bucket_name" {
  default = "transform_bucket_id"
}

data "aws_ssm_parameter" "transform_bucket_name" {
  name = var.transform_bucket_name
}

variable "ingest_lambda_name" {
  default = "ingester"
}

variable "transform_lambda_name" {
  default = "transformer"
}
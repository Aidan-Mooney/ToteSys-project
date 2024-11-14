variable "ingest_bucket_arn" {
  default = "ingest_bucket_arn"
}

data "aws_ssm_parameter" "ingest_bucket_arn" {
  name = var.ingest_bucket_arn
}

variable "transform_bucket_arn" {
  default = "transform_bucket_arn"
}

data "aws_ssm_parameter" "transform_bucket_arn" {
  name = var.transform_bucket_arn
}

variable "ingest_lambda_name" {
  default = "ingester"
}

variable "transform_lambda_name" {
  default = "transformer"
}

variable "python_runtime" {
  default = "python3.12"
}

variable "state_machine_name" {
  default = "totesys_state_machine"
}
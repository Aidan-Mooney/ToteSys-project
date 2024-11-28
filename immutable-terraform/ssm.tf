resource "aws_ssm_parameter" "ingest_bucket_arn" {
  name  = "ingest_bucket_arn"
  type  = "String"
  value = "${aws_s3_bucket.ingest_bucket2.arn}"
#   overwrite = true
}

resource "aws_ssm_parameter" "transform_bucket_arn" {
  name  = "transform_bucket_arn"
  type  = "String"
  value = "${aws_s3_bucket.transform_bucket.arn}"
#   overwrite = true
}

resource "aws_ssm_parameter" "ingest_bucket_name" {
  name  = "ingest_bucket_name"
  type  = "String"
  value = "${aws_s3_bucket.ingest_bucket2.id}"
#   overwrite = true
}

resource "aws_ssm_parameter" "transform_bucket_name" {
  name  = "transform_bucket_name"
  type  = "String"
  value = "${aws_s3_bucket.transform_bucket.id}"
#   overwrite = true
}

resource "aws_ssm_parameter" "secret_arn" {
  name = "secret_db_credentials"
  type = "String"
  value = "${aws_secretsmanager_secret.db_credentials.arn}"
}

resource "aws_ssm_parameter" "warehouse_secret_arn" {
  name = "secret_warehouse_credentials"
  type = "String"
  value = "${aws_secretsmanager_secret.warehouse_credentials.arn}"
}
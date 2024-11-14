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
  value = "${aws_s3_bucket.ingest_bucket2_name}"
#   overwrite = true
}

resource "aws_ssm_parameter" "transform_bucket_name" {
  name  = "transform_bucket_name"
  type  = "String"
  value = "${aws_s3_bucket.transform_bucket_name}"
#   overwrite = true
}


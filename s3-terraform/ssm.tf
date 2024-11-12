resource "aws_ssm_parameter" "ingest_bucket_id" {
  name  = "ingest_bucket_id"
  type  = "String"
  value = "${aws_s3_bucket.ingest_bucket2.id}"
#   overwrite = true
}

resource "aws_ssm_parameter" "transform_bucket_id" {
  name  = "transform_bucket_id"
  type  = "String"
  value = "${aws_s3_bucket.transform_bucket.id}"
#   overwrite = true
}
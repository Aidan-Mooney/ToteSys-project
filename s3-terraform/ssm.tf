resource "aws_ssm_parameter" "ingest_bucket_id" {
  name  = "ingest_bucket_id"
  type  = "String"
  value = aws_s3_bucket.ingest_bucket.id
}

resource "aws_ssm_parameter" "transform_bucket_id" {
  name  = "ingest_bucket_id"
  type  = "String"
  value = aws_s3_bucket.transform_bucket.id
}
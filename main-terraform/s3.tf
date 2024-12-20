resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "code-bucket" # change to same prefix format to what matt C is making 
  tags = {
    Name    = "code_bucket"
    Purpose = "holds .py files and folders for lambda functions and layers."
  }
}

#########################################

resource "aws_s3_object" "utils_file" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "utils/utils.zip"
  source = "${path.module}/../packages/utils/utils.zip"
  etag   = filemd5(data.archive_file.utils.output_path)
}

resource "aws_s3_object" "dependencies_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "dependencies/dependencies.zip"
  source = "${path.module}/../packages/dependencies/dependencies.zip"
  etag   = filemd5(data.archive_file.dependencies.output_path)
}

resource "aws_s3_object" "ingest_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "ingest/ingest.zip"
  source = "${path.module}/../packages/ingester/ingest.zip"
  etag   = filemd5(data.archive_file.ingester.output_path)
}

resource "aws_s3_object" "transform_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "transform/transform.zip"
  source = "${path.module}/../packages/transformer/transform.zip"
  etag   = filemd5(data.archive_file.transformer.output_path)
}

resource "aws_s3_object" "load_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "load/load.zip"
  source = "${path.module}/../packages/loader/load.zip"
  etag   = filemd5(data.archive_file.loader.output_path)
}

resource "aws_s3_object" "ingest_publisher_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "ingest/publish_email.zip"
  source = "${path.module}/../packages/ingest_email_publisher/ingest_email_publisher.zip"
}

resource "aws_s3_object" "dim_date_parquet" {
  bucket = data.aws_ssm_parameter.transform_bucket_name.value
  key    = "dim_date/2024/11/20/1135000000.parquet"
  source = "${path.module}/../data/dim_date.parquet"
  etag   = filemd5("${path.module}/../data/dim_date.parquet")
}

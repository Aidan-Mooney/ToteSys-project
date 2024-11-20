resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "code-bucket" # change to same prefix format to what matt C is making 
  tags = {
    Name    = "code_bucket"
    Purpose = "holds .py files and folders for lambda functions and layers."
  }
}

#########################################

resource "aws_s3_object" "ingest_utils_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "utils/utils.zip"
  source = "${path.module}/../packages/utils/utils.zip"
  etag   = filemd5(data.archive_file.utils.output_path)
}

resource "aws_s3_object" "dependencies_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "layer/layer.zip"
  source = "${path.module}/../packages/layer/layer.zip"
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
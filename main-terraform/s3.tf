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
}

resource "aws_s3_object" "dependencies_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "layer/layer.zip"
  source = "${path.module}/../packages/layer/layer.zip"
}


resource "aws_s3_object" "ingest_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "ingest/ingest.zip"
  source = "${path.module}/../packages/ingester/ingest.zip"
}

/*
resource "aws_s3_object" "pg8000_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "pg8000-module.zip"
  source = "${path.module}/..." # module path to code to be saved
}
*/

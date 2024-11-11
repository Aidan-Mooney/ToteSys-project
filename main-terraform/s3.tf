resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "code-bucket" # change to same prefix format to what matt C is making 
}

/*
resource "aws_s3_object" "ingress_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "ingress.zip"
  source = "${path.module}/..." # module path to code to be saved
}

resource "aws_s3_object" "pg8000_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "pg8000-module.zip"
  source = "${path.module}/..." # module path to code to be saved
}
*/

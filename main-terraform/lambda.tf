data "archive_file" "utils"{
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../src/utils"
  output_path      = "${path.module}/../src-archive/utils.zip"
}

resource "aws_lambda_layer_version" "ingest_utils" {
  layer_name          = "ingest_utils"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.id
  s3_key              = "utils.zip"
  depends_on          = [aws_s3_object.ingest_utils_lambda_file]
  source_code_hash    = data.archive_file.utils.output_base64sha256
}
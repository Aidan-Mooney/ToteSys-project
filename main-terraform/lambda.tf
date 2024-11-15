data "archive_file" "ingester" {
  type             = "zip"
  output_file_mode = "0666"
  source_file       = "${path.module}/../src/lambdas/ingest.py"
  output_path      = "${path.module}/../packages/ingester/ingest.zip"
}

resource "aws_lambda_function" "ingest_lambda_function" {
  role                  = aws_iam_role.ingest_lambda_role.arn
  function_name         = var.ingest_lambda_name
  source_code_hash      = data.archive_file.ingester.output_base64sha256
  s3_bucket             = aws_s3_object.ingest_lambda_file.bucket
  s3_key                = aws_s3_object.ingest_lambda_file.id
  runtime               = var.python_runtime
  depends_on            = [aws_s3_object.dependencies_lambda_file,
                            aws_s3_object.ingest_lambda_file,
                            aws_s3_object.ingest_utils_lambda_file]
  timeout               = var.default_timeout
  handler               = "${var.ingest_lambda_name}.lambda_handler"
  layers                = [aws_lambda_layer_version.dependencies.arn, aws_lambda_layer_version.ingest_utils.arn]
  environment {
    variables = {s3_bucket = data.aws_ssm_parameter.ingest_bucket_name.value }
  }
}
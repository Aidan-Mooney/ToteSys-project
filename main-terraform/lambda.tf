data "archive_file" "ingester" {
  type             = "zip"
  output_file_mode = "0666"
  source_file       = "${path.module}/../src/lambdas/ingest.py"
  output_path      = "${path.module}/../packages/ingester/ingest.zip"
}

data "archive_file" "transformer" {
  type             = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/lambdas/transform.py"
  output_path = "${path.module}/../packages/transformer/transform.zip"
}

resource "aws_lambda_function" "ingest_lambda_function" {
  role                  = aws_iam_role.ingest_lambda_role.arn
  function_name         = var.ingest_lambda_name
  source_code_hash      = data.archive_file.ingester.output_base64sha256
  s3_bucket             = aws_s3_object.ingest_lambda_file.bucket
  s3_key                = aws_s3_object.ingest_lambda_file.id
  runtime               = var.python_runtime
  depends_on            = [ aws_s3_object.dependencies_lambda_file,
                            aws_s3_object.ingest_lambda_file,
                            aws_s3_object.utils_file,
                            aws_iam_role_policy_attachment.lambda_logs-for-ingest-policy,
                            aws_cloudwatch_log_group.totesys-cw-log-group,
                            ]
  timeout               = var.default_timeout
  handler               = "${var.ingest_lambda_name}.lambda_handler"
  layers                = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:14", aws_lambda_layer_version.utils.arn, aws_lambda_layer_version.dependencies.arn]
  environment {
    variables = { ingest_bucket_name = data.aws_ssm_parameter.ingest_bucket_name.value,
                  DEV_ENVIRONMENT = "deploy",
                  static_address_path = var.static_address_path,
                  static_department_path=var.static_department_path
                }
  }
  logging_config {
    log_format  = "Text"
    log_group   = aws_cloudwatch_log_group.totesys-cw-log-group.name
  }
}

resource "aws_lambda_function" "transform_lambda_function" {
  role                  = aws_iam_role.transform_lambda_role.arn
  function_name         = var.transform_lambda_name
  source_code_hash      = data.archive_file.transformer.output_base64sha256
  s3_bucket             = aws_s3_object.transform_lambda_file.bucket
  s3_key                = aws_s3_object.transform_lambda_file.id
  runtime               = var.python_runtime
  depends_on            = [ aws_s3_object.transform_lambda_file,
                            aws_iam_role_policy_attachment.s3_transform_policy,
                            aws_s3_object.utils_file,
                            aws_cloudwatch_log_group.totesys-cw-log-group
                          ]
  timeout               = var.default_timeout
  handler               = "${var.transform_lambda_name}.lambda_handler"
  layers                = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:14", aws_lambda_layer_version.utils.arn, aws_lambda_layer_version.dependencies.arn#utils
                          ] 
  environment {
    variables = {ingest_bucket_name = data.aws_ssm_parameter.ingest_bucket_name.value, 
                 transform_bucket_name = data.aws_ssm_parameter.transform_bucket_name.value, 
                 DEV_ENVIRONMENT = "deploy"
                 static_address_path = var.static_address_path,
                 static_department_path=var.static_department_path
                }
              }
  logging_config {
    log_format  = "Text"
    log_group   = aws_cloudwatch_log_group.totesys-cw-log-group.name
  }
}


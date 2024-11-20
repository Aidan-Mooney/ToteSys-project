data "archive_file" "ingester" {
  type             = "zip"
  output_file_mode = "0666"
  source_file       = "${path.module}/../src/lambdas/ingest.py"
  output_path      = "${path.module}/../packages/ingester/ingest.zip"
}

data "archive_file" "transformer" {
  type             = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}../src/lambdas/fake_transform.py" # change to 'transform.py' when the lambda handler is available
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
                            aws_s3_object.ingest_utils_lambda_file,
                            aws_iam_role_policy_attachment.lambda_logs-for-ingest-policy,
                            aws_cloudwatch_log_group.totesys-cw-log-group,
                            ]
  timeout               = var.default_timeout
  handler               = "${var.ingest_lambda_name}.lambda_handler"
  layers                = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:14", aws_lambda_layer_version.ingest_utils.arn, aws_lambda_layer_version.dependencies.arn]
  environment {
    variables = {bucket_name = data.aws_ssm_parameter.ingest_bucket_name.value, DEV_ENVIRONMENT = "deploy"}
  }
  logging_config {
    log_format  = "Text"
    log_group   = aws_cloudwatch_log_group.totesys-cw-log-group.name
  }
}

resource "aws_lambda_function" "transform_lambda_function" {
  role                  = #iam roles
  function_name         = var.transform_lambda_name
  source_code_hash      = #
  s3_bucket             = # code bucket
  s3_key                = #name of code we are assigning
  runtime               = var.python_runtime
  depends_on            = [ #all the other terraform stuffs 
                          ]
  timeout               = var.default_timeout
  handler               = "${var.transform_lambda_name}.lambda_handler"
  layers                = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:14", #utils
                          ] 
  environment {
    variables = {ingest_bucket_name = data.aws_ssm_parameter.ingest_bucket_name.value, 
                 transform_bucket_name = data.aws_ssm_parameter.transform_bucket_name.value, 
                 DEV_ENVIRONMENT = "deploy"
                }
              }
  logging_config {
    log_format  = "Text"
    log_group   = aws_cloudwatch_log_group.totesys-cw-log-group.name
  }
}


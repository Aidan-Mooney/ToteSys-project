# Metric filter to count critical logs
resource "aws_cloudwatch_log_metric_filter" "critical_count" {
  name = "CountCriticalLogs"
  log_group_name = aws_cloudwatch_log_group.totesys-cw-log-group.name
  pattern = "[CRITICAL]"
  metric_transformation {
    name = "CriticalCounter"
    namespace = "project"
    value = 1
    default_value = 0
  }
}
# Metric alarm which enters ALARM state when a critical log is logged
resource "aws_cloudwatch_metric_alarm" "critical_alarm" {
  alarm_name = "critical-logs"
  metric_name = lookup(aws_cloudwatch_log_metric_filter.critical_count.metric_transformation[0], "name")
  threshold = 1
  statistic = "SampleCount"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods = 1
  period = 10
  namespace = "project"
    alarm_actions = [aws_sns_topic.trigger_lambda.arn]
  alarm_description = "Detect a critical log from ingest lambda function"
  treat_missing_data = "notBreaching"
}

# Publisher function
data "archive_file" "publish" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/ingest_email_publisher.py"
  output_path      = "${path.module}/../src_archive/ingest_email_publisher.zip"
}
resource "aws_s3_object" "ingest_publisher_lambda_file" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "ingest/publish_email.zip"
  source = "${path.module}/../src_archive/ingest_email_publisher.zip"
}
resource "aws_lambda_function" "publish" {
  function_name = "publisher"
  s3_bucket             = aws_s3_object.ingest_publisher_lambda_file.bucket
  s3_key                = aws_s3_object.ingest_publisher_lambda_file.id
  handler = "publish.lambda_handler"
  runtime = var.python_runtime
  source_code_hash = data.archive_file.publish.output_base64sha256
  role = aws_iam_role.ingest_lambda_role.arn
  timeout = 12
  environment {
    variables = {"sns_topic_arn" = aws_sns_topic.send_email.arn
  }
}
}

# SNS topic and subscription
resource "aws_sns_topic" "trigger_lambda" {
  name = "critical-warnings-in-the-logs"
}

resource "aws_sns_topic_subscription" "trigger_lambda" {
  topic_arn = aws_sns_topic.trigger_lambda.arn
    protocol = "lambda"
    endpoint = aws_lambda_function.publish.arn
}

resource "aws_sns_topic" "send_email" {
  name = "critical-warnings-send-email"
}

resource "aws_sns_topic_subscription" "send_email" {
  topic_arn = aws_sns_topic.send_email.arn
  protocol = "email"
  endpoint = data.aws_secretsmanager_secret_version.sns_email_endpoint.secret_string
}

# Permission to allow lambda to be executed by SNS
resource "aws_lambda_permission" "SNS_lambda_permission" {
  statement_id = "AllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.publish.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.trigger_lambda.arn
}
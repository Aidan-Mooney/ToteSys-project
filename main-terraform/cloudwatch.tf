resource "aws_cloudwatch_log_group" "totesys-cw-log-group" {
  name              = "totesys-cw-log-group"
  retention_in_days = 14
}

# Metric filter to count critical logs
resource "aws_cloudwatch_log_metric_filter" "critical_count" {
  name           = "CountCriticalLogs"
  log_group_name = aws_cloudwatch_log_group.totesys-cw-log-group.name
  pattern        = "[CRITICAL]"
  metric_transformation {
    name          = "CriticalCounter"
    namespace     = "project"
    value         = 1
    default_value = 0
  }
}

# Metric alarm which enters ALARM state when a critical log is logged
resource "aws_cloudwatch_metric_alarm" "critical_alarm" {
  alarm_name          = "critical-logs"
  metric_name         = lookup(aws_cloudwatch_log_metric_filter.critical_count.metric_transformation[0], "name")
  threshold           = 1
  statistic           = "SampleCount"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  period              = 10
  namespace           = "project"
  alarm_actions       = [aws_sns_topic.trigger_lambda.arn]
  alarm_description   = "Detect a critical log from ingest lambda function"
  treat_missing_data  = "notBreaching"
}

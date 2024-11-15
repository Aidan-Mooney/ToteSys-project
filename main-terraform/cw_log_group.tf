resource "aws_cloudwatch_log_group" "totesys-cw-log-group" {
  name              = "totesys-cw-log-group"
  retention_in_days = 14
}
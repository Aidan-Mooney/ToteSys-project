data "aws_secretsmanager_secret" "sns_email_endpoint" {
  name = "sns_email_endpoint"
}

data "aws_secretsmanager_secret_version" "sns_email_endpoint" {
  secret_id = data.aws_secretsmanager_secret.sns_email_endpoint.id
}
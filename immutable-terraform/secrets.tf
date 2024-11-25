resource "aws_secretsmanager_secret_version" "db_v1" {
  secret_id     = aws_secretsmanager_secret.db_credentials.id
  secret_string = file("${path.module}/db_credentials.json")
}

resource "aws_secretsmanager_secret" "db_credentials" {
  name = "totesys_db_credentials"
  force_overwrite_replica_secret = true
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "warehouse_v1" {
  secret_id     = aws_secretsmanager_secret.warehouse_credentials.id
  secret_string = file("${path.module}/warehouse_credentials.json")
}

resource "aws_secretsmanager_secret" "warehouse_credentials" {
  name = "totesys_warehouse_credentials"
  force_overwrite_replica_secret = true
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "email_v1" {
  secret_id     = aws_secretsmanager_secret.sns_email_endpoint.id
  secret_string = file("${path.module}/email.txt")
}

resource "aws_secretsmanager_secret" "sns_email_endpoint" {
  name = "sns_email_endpoint"
  force_overwrite_replica_secret = true
  recovery_window_in_days = 0
}
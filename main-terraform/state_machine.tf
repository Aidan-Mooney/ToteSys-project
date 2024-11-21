
resource "aws_sfn_state_machine" "state_machine" {
  name_prefix = var.state_machine_name
  role_arn = aws_iam_role.state_role.arn
  definition = templatefile("${path.module}/../state-machine/state_machine.asl.json", {
    payload = file("${path.module}/../state-machine/init_payload.json")
    lambda_ingest = aws_lambda_function.ingest_lambda_function.arn
    lambda_transform = aws_lambda_function.transform_lambda_function.arn
  }
  )
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

resource "aws_sfn_state_machine" "state_machine" {
  name_prefix = var.state_machine_name
  role_arn = aws_iam_role.state_role.arn
  definition = templatefile("${path.module}/../state-machine/state_machine.asl.json", {
    LambdaIngest = aws_lambda_function.ingest_lambda_function.arn
    # LambdaTransform = aws_lambda_function.transform_lambda_function.arn
  }
  )
 
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
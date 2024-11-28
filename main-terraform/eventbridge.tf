resource "aws_cloudwatch_event_rule" "scheduler" {
  name = var.event_bridge_name
  description = "Triggers '${var.state_machine_name}' every 5 minutes."
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "step_func_target" {
  rule = aws_cloudwatch_event_rule.scheduler.name
  target_id = "StepFunctionTarget"
  arn = aws_sfn_state_machine.state_machine.arn
  role_arn = aws_iam_role.eventbridge_role.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_lambda_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_sfn_state_machine.state_machine.arn
}
resource "aws_cloudwatch_event_rule" "scheduler" {
  name = "every_5_mins_rule"
  description = "trigger step function seconds"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "step_func_target" {
  rule = aws_cloudwatch_event_rule.scheduler.name
  target_id = "StepFunctionTarget"
  arn = aws_sfn_state_machine.state_machine.arn
}
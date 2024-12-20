### IAM ROLES ###########################################

resource "aws_iam_role" "ingest_lambda_role" {
  name_prefix        = "role-${var.ingest_lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda_role_document.json
  description        = "IAM role used by '${var.ingest_lambda_name}' lambda function."
}

resource "aws_iam_role" "transform_lambda_role" {
  name_prefix        = "role-${var.transform_lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda_role_document.json
  description        = "IAM role used by '${var.transform_lambda_name}' lambda function."
}

resource "aws_iam_role" "load_lambda_role" {
  name_prefix        = "role-${var.load_lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda_role_document.json
  description        = "IAM role used by '${var.load_lambda_name}' lambda function."
}

resource "aws_iam_role" "state_role" {
  name_prefix        = "role-${var.state_machine_name}"
  assume_role_policy = data.aws_iam_policy_document.assume_state_role_document.json
  description        = "IAM role used by '${var.state_machine_name}'."
}

resource "aws_iam_role" "eventbridge_role" {
  name_prefix        = "role-eventbridge-"
  assume_role_policy = data.aws_iam_policy_document.assume_events_role_document.json
  description        = "IAM role used by '${var.event_bridge_name}' event bridge rule."
}
### ASSUME ROLE POLICY DOCUMENTS ########################

data "aws_iam_policy_document" "assume_lambda_role_document" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "assume_state_role_document" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "assume_events_role_document" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

### OTHER POLICY DOCUMENTS ##############################

data "aws_iam_policy_document" "s3_code_document" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.code_bucket.arn}/*"]
  }
}

data "aws_iam_policy_document" "ingest_policy_document" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${data.aws_ssm_parameter.ingest_bucket_arn.value}/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = ["${data.aws_ssm_parameter.ingest_bucket_arn.value}"]
  }
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["${data.aws_ssm_parameter.credentials_secret_arn.value}"]
  }
}

data "aws_iam_policy_document" "transform_policy_document" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${data.aws_ssm_parameter.transform_bucket_arn.value}/*"]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${data.aws_ssm_parameter.ingest_bucket_arn.value}/*"]
  }
}


data "aws_iam_policy_document" "load_policy_document" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${data.aws_ssm_parameter.transform_bucket_arn.value}/*"]
  }
  statement { 
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["${data.aws_ssm_parameter.warehouse_credentials_secret_arn.value}"]
  }
}


data "aws_iam_policy_document" "invoke_lambdas_document" {
  statement {
    actions   = [
      "lambda:InvokeFunction",
      "xray:PutTraceSegments",
      "xray:PutTelemetryRecords",
      "xray:GetSamplingRules",
      "xray:GetSamplingTargets" 
    ]
    resources = [
      "${aws_lambda_function.ingest_lambda_function.arn}*",
      "${aws_lambda_function.transform_lambda_function.arn}*",
      "${aws_lambda_function.load_lambda_function.arn}*",
    ]
  }
}

data "aws_iam_policy_document" "eventsbridge_document"{
  statement {
    actions   = ["states:StartExecution"]
    resources = ["${aws_sfn_state_machine.state_machine.arn}"]
  }
}
  
data "aws_iam_policy_document" "lambda_logging" {
  statement {
    effect    = "Allow"
    actions   = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

### POLICIES ############################################

resource "aws_iam_policy" "s3_code_policy" {
  name_prefix = "s3-code-policy-"
  policy      = data.aws_iam_policy_document.s3_code_document.json
  description = "allows cloud service to access objects inside '${aws_s3_bucket.code_bucket.id}'."
}

resource "aws_iam_policy" "ingest_policy" {
  name_prefix = "s3-policy-${var.ingest_lambda_name}"
  policy      = data.aws_iam_policy_document.ingest_policy_document.json
  description = "allows cloud service to write to '${data.aws_ssm_parameter.ingest_bucket_name.value}', list objects in '${data.aws_ssm_parameter.ingest_bucket_name.value}' and access the secret 'db_credentials'."
}

resource "aws_iam_policy" "transform_policy" {
  name_prefix = "s3-policy-${var.transform_lambda_name}"
  policy      = data.aws_iam_policy_document.transform_policy_document.json
  description = "allows cloud service to write to '${data.aws_ssm_parameter.transform_bucket_arn.value}' and read from '${data.aws_ssm_parameter.ingest_bucket_arn.value}'."
}

resource "aws_iam_policy" "load_policy" {
  name_prefix = "s3-policy-${var.load_lambda_name}"
  policy      = data.aws_iam_policy_document.load_policy_document.json
  description = "allows cloud service to read from '${data.aws_ssm_parameter.transform_bucket_arn.value}' and access the secret 'warehouse_credentials'"
}

resource "aws_iam_policy" "invoke_lambdas_policy" {
  name_prefix = "invoke-lambda-policy-for-${var.state_machine_name}"
  policy      = data.aws_iam_policy_document.invoke_lambdas_document.json
  description = "allows cloud service to invoke lambda functions '${var.ingest_lambda_name}', '${var.transform_lambda_name}' and '${var.load_lambda_name}'."
}

resource "aws_iam_policy" "eventbridge_policy" {
  name_prefix = "eventbridge-policy-for-${var.state_machine_name}"
  policy      = data.aws_iam_policy_document.eventsbridge_document.json
  description = "allows cloud service to start execution on state machine '${var.state_machine_name}'."
}

resource "aws_iam_policy" "lambda_logging_policy" {
  name_prefix = "lambda_logging"
  policy      = data.aws_iam_policy_document.lambda_logging.json
  description = "IAM policy for logging from a lambda."
}

### ATTACHING POLICIES TO ROLES #########################

resource "aws_iam_role_policy_attachment" "s3_ingest_code_policy" {
  role       = aws_iam_role.ingest_lambda_role.name
  policy_arn = aws_iam_policy.s3_code_policy.arn
}

resource "aws_iam_role_policy_attachment" "s3_transform_code_policy" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.s3_code_policy.arn
}

resource "aws_iam_role_policy_attachment" "s3_load_code_policy" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.s3_code_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingest_policy" {
  role       = aws_iam_role.ingest_lambda_role.name
  policy_arn = aws_iam_policy.ingest_policy.arn
}

resource "aws_iam_role_policy_attachment" "transform_policy" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_policy.arn
}

resource "aws_iam_role_policy_attachment" "load_policy" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.load_policy.arn
}

resource "aws_iam_role_policy_attachment" "invoke_lambdas_policy_attachment" {
  role       = aws_iam_role.state_role.name
  policy_arn = aws_iam_policy.invoke_lambdas_policy.arn
}

resource "aws_iam_role_policy_attachment" "eventbridge_policy_attachment" {
  role       = aws_iam_role.eventbridge_role.name
  policy_arn = aws_iam_policy.eventbridge_policy.arn
}
  
resource "aws_iam_role_policy_attachment" "lambda_logs_for_ingest_policy" {
  role       = aws_iam_role.ingest_lambda_role.name
  policy_arn = aws_iam_policy.lambda_logging_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_logs_for_transform_policy" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.lambda_logging_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_logs_for_load_policy" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.lambda_logging_policy.arn
}
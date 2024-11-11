resource "aws_iam_role" "ingest_lambda_role" {
    name_prefix = "role-${var.ingest_lambda_name}"
    assume_role_policy = data.aws_iam_policy_document.assume_role_document.json
}

resource "aws_iam_role" "transform_lambda_role" {
    name_prefix = "role-${var.transform_lambda_name}"
    assume_role_policy = data.aws_iam_policy_document.assume_role_document.json
}

#####################################################

data "aws_iam_policy_document" "assume_role_document" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "s3_code_document" {
  statement {

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/*",
    ]
  }
}

data "aws_iam_policy_document" "s3_ingest_document" {
  statement {

    actions = ["s3:WriteObject"]

    resources = [
      "${data.aws_ssm_parameter.ingest_bucket_name.value}/*",
    ]
  }
}

data "aws_iam_policy_document" "s3_transform_document" {
  statement {

    actions = ["s3:WriteObject"]

    resources = [
      "${data.aws_ssm_parameter.transform_bucket_name.value}/*",
    ]
  }
}

#######################################################

resource "aws_iam_policy" "s3_code_policy" {
    name_prefix = "s3-code-policy-"
    policy = data.aws_iam_policy_document.s3_code_document.json
}

resource "aws_iam_policy" "s3_ingest_policy" {
    name_prefix = "s3-policy-${var.ingest_lambda_name}"
    policy = data.aws_iam_policy_document.s3_ingest_document.json
}

resource "aws_iam_policy" "s3_transform_policy" {
    name_prefix = "s3-policy-${var.transform_lambda_name}"
    policy = data.aws_iam_policy_document.s3_transform_document.json
}

#########################################################

resource "iam_role_policy_attachment" "s3_ingest_code_policy" {
    role = aws_iam_role.ingest_lambda_role.name
    policy_arn = aws_iam_policy.s3_code_policy.arn
}

resource "iam_role_policy_attachment" "s3_transform_code_policy" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.s3_code_policy.arn
}

resource "iam_role_policy_attachment" "s3_ingest_policy" {
    role = aws_iam_role.ingest_lambda_role.name
    policy_arn = aws_iam_policy.s3_ingest_policy.arn
}

resource "iam_role_policy_attachment" "s3_transform_policy" {
    role = aws_iam_role.transform_lambda_role.name
    policy_arn = aws_iam_policy.s3_transform_policy.arn
}
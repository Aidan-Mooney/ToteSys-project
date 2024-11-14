terraform {
  required_providers {
    aws = {
      source ="hashicorp/aws"
      version="~> 5.0"
    }
  }
  backend "s3" {
    bucket = "laurentia-tf-backend"
    key    = "template/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws"{
  region = "eu-west-2"
  default_tags {
    tags={
      ProjectName   = "ETL Pipeline Project"
      Team          = "Laurentia-4"
      DeployedFrom  = "Terraform"
      Repository    = "ToteSys-project"
      Environment   = "Dev"
    }
  }
}

resource "aws_sfn_state_machine" "state_machine" {
  name_prefix = var.state_machine_name
  role_arn = aws_iam_role.state_role.arn
  definition = <<EOF
  # definition = <<EOF
  # {
  #   "Comment": "A description of my state machine",
  #   "StartAt": "extract",
  #   "States": {
  #     "extract": {
  #       "Type": "Task",
  #       "Resource": "arn:aws:states:::lambda:invoke",
  #       "OutputPath": "$.Payload",
  #       "Parameters": {
  #         "Payload.$": "$",
  #         "FunctionName": "arn:aws:lambda:eu-west-2:${var.aws_account_id}:function:extract:$LATEST"
  #       },
  #       "Retry": [
  #         {
  #           "ErrorEquals": [
  #             "Lambda.ServiceException",
  #             "Lambda.AWSLambdaException",
  #             "Lambda.SdkClientException",
  #             "Lambda.TooManyRequestsException"
  #           ],
  #           "IntervalSeconds": 1,
  #           "MaxAttempts": 3,
  #           "BackoffRate": 2
  #         }
  #       ],
  #       "Next": "transform"
  #     },
  #     "transform": {
  #       "Type": "Task",
  #       "Resource": "arn:aws:states:::lambda:invoke",
  #       "OutputPath": "$.Payload",
  #       "Parameters": {
  #         "Payload.$": "$",
  #         "FunctionName": "arn:aws:lambda:eu-west-2:${var.aws_account_id}:function:transform:$LATEST"
  #       },
  #       "Retry": [
  #         {
  #           "ErrorEquals": [
  #             "Lambda.ServiceException",
  #             "Lambda.AWSLambdaException",
  #             "Lambda.SdkClientException",
  #             "Lambda.TooManyRequestsException"
  #           ],
  #           "IntervalSeconds": 1,
  #           "MaxAttempts": 3,
  #           "BackoffRate": 2
  #         }
  #       ],
  #       "Next": "load"
  #     },
  #     "load": {
  #       "Type": "Task",
  #       "Resource": "arn:aws:states:::lambda:invoke",
  #       "OutputPath": "$.Payload",
  #       "Parameters": {
  #         "Payload.$": "$",
  #         "FunctionName": "arn:aws:lambda:eu-west-2:${var.aws_account_id}:function:load:$LATEST"
  #       },
  #       "Retry": [
  #         {
  #           "ErrorEquals": [
  #             "Lambda.ServiceException",
  #             "Lambda.AWSLambdaException",
  #             "Lambda.SdkClientException",
  #             "Lambda.TooManyRequestsException"
  #           ],
  #           "IntervalSeconds": 1,
  #           "MaxAttempts": 3,
  #           "BackoffRate": 2
  #         }
  #       ],
  #       "End": true
  #     }
  #   }
  # }
  # EOF
  EOF
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
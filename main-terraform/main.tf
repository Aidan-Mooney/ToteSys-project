terraform {
  required_providers {
    aws = {
      source ="hashicorp/aws"
      version="~> 5.0"
    }
  }
  backend "s3" {
    bucket = "laurentia-tf-backend"
    key    = ""
    region = "eu-west-2"
  }
}

provider "aws"{
  region = "eu-west-2"
  default_tags {
    tags={
      ProjectName   = ""
      Team          = ""
      DeployedFrom  = ""
      Repository    = ""
      Environment   = ""
      RetentionDate = ""
    }
  }
}
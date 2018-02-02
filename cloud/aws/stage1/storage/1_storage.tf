variable "deploy_name" {}
variable "deploy_stack" {}
variable "deploy_prefix" {}

#####################################
# S3 buckets

resource "aws_s3_bucket" "kops-state" {
    bucket = "${var.deploy_prefix}-kops-state"
    acl = "private"

    versioning {
        enabled = true
    }
}

# Tensorflow and ML container registries.

resource "aws_ecr_repository" "universe-dev" {
  name = "universe-dev"
}

resource "aws_ecr_repository" "ml-lmc" {
  name = "ml-lmc"
}

resource "aws_ecr_repository" "ml-trews" {
  name = "ml-trews"
}

resource "aws_ecr_repository" "ml-trews-dev" {
  name = "ml-trews-dev"
}

resource "aws_ecr_repository" "ml-trews-stage" {
  name = "ml-trews-stage"
}

resource "aws_ecr_repository" "utilities" {
  name = "utilities"
}

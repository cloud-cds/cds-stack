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
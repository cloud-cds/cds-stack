variable "aws_id" {}
variable "aws_region" {}
variable "local_shell" {}

variable "domain_zone_id" {}
variable "web_dns_name" {}
variable "web_hostname_file" {}

variable "deploy_name" {}
variable "deploy_stack" {}
variable "deploy_prefix" {}

## Up
resource "null_resource" "start-web-service-prod" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/web/start.sh app=nginx"
  }
}

## Down
resource "null_resource" "stop-web-service-prod" {
  count = 1

  provisioner "local-exec" {
    command = "${var.local_shell} services/web/stop.sh"
  }
}

# DNS
resource "aws_route53_record" "prod_rest_api" {
  zone_id = "${var.domain_zone_id}"
  name    = "${var.web_dns_name}"
  type    = "CNAME"
  ttl     = "300"
  records = ["${trimspace(file("${var.web_hostname_file}"))}"]
}

#############################
# Application logging.

variable "web_logs_name" { default = "opsdx-web-logs-prod" }

# Cloudwatch group for application logging
resource "aws_cloudwatch_log_group" "web_logs_prod" {
  name = "${var.web_logs_name}"
  retention_in_days = "30"
  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Web Logs"
  }
}

# Production log bucket
resource "aws_s3_bucket" "web_logs_prod" {
  bucket = "${var.web_logs_name}"
  force_destroy = true

  policy = <<POLICY
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "s3:GetBucketAcl",
            "Effect": "Allow",
            "Principal": {
              "Service": "logs.${var.aws_region}.amazonaws.com"
            },
            "Resource": "arn:aws:s3:::${var.web_logs_name}"
        },
        {
            "Action": "s3:PutObject",
            "Effect": "Allow",
            "Principal": {
              "Service": "logs.${var.aws_region}.amazonaws.com"
            },
            "Resource": "arn:aws:s3:::${var.web_logs_name}/*",
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-acl": "bucket-owner-full-control"
                }
            }
        }
    ]
}
POLICY

  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Web Logs"
  }
}


# AWS IAM Role for Kinesis Firehose => S3 push.
resource "aws_iam_role" "web_prod_logs_fs3_push" {
  name = "${var.web_logs_name}-role-fs3-push"
  assume_role_policy = <<POLICY
{
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "firehose.amazonaws.com"
      },
      "Effect": "Allow",
      "Condition" : {
        "StringEquals": { "sts:ExternalId":"${var.aws_id}" }
      }
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy" "web_prod_logs_fs3_push" {
  name = "${var.web_logs_name}-policy-fs3-push"
  role = "${aws_iam_role.web_prod_logs_fs3_push.id}"
  policy = <<POLICY
{
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [ "s3:AbortMultipartUpload",
                  "s3:GetBucketLocation",
                  "s3:GetObject",
                  "s3:ListBucket",
                  "s3:ListBucketMultipartUploads",
                  "s3:PutObject" ],
      "Resource": [
        "arn:aws:s3:::${var.web_logs_name}",
        "arn:aws:s3:::${var.web_logs_name}/*"
      ]
    }
  ]
}
POLICY
}

# Kinesis Firehose stream
resource "aws_kinesis_firehose_delivery_stream" "web_prod_log_stream" {
    name          = "${var.web_logs_name}-stream"
    destination   = "s3"
    s3_configuration {
      role_arn   = "${aws_iam_role.web_prod_logs_fs3_push.arn}"
      bucket_arn = "${aws_s3_bucket.web_logs_prod.arn}"
      # TODO: after testing, enable:
      #
      #s3_data_compression = "GZIP"
      #
      #kms_key_arn = ""
      #
      # Log failures (i.e., delivery errors) to a cloudwatch logs group
      #cloudwatch_logging_options = {}
    }
}

# AWS IAM Role for CloudWatch Logs => Kinesis Firehose push.
resource "aws_iam_role" "web_prod_logs_cwf_push" {
  name = "${var.web_logs_name}-role-cwf-push"
  assume_role_policy = <<POLICY
{
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "logs.${var.aws_region}.amazonaws.com"
      },
      "Effect": "Allow"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy" "web_prod_logs_cwf_push" {
  name = "${var.web_logs_name}-policy-cwf-push"
  role = "${aws_iam_role.web_prod_logs_cwf_push.id}"
  policy = <<POLICY
{
  "Statement": [
    {
      "Effect":"Allow",
      "Action":["firehose:*"],
      "Resource":["${aws_kinesis_firehose_delivery_stream.web_prod_log_stream.arn}"]
    },
    {
      "Effect":"Allow",
      "Action":["iam:PassRole"],
      "Resource":["${aws_iam_role.web_prod_logs_cwf_push.arn}"]
    }
  ]
}
POLICY
}

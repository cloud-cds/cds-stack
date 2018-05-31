variable "deploy_name" {}
variable "deploy_stack" {}
variable "deploy_prefix" {}

variable "aws_id" {}
variable "aws_region" {}

variable "audit_sns_protocol" {}
variable "audit_sns_endpoint" {}


#############################
# Auditing via Cloudtrail.

# Terraform construction of KMS log encryption key.
resource "aws_kms_key" "audit_log" {
    description         = "Log encryption key for MC"
    enable_key_rotation = true
    policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "Enable IAM User Permissions",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${var.aws_id}:root"
      },
      "Action": "kms:*",
      "Resource": "*"
    },
    {
      "Sid": "Allow access for Key Administrators",
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "arn:aws:iam::${var.aws_id}:user/zad",
        ]
      },
      "Action": [
        "kms:Create*",
        "kms:Describe*",
        "kms:Enable*",
        "kms:List*",
        "kms:Put*",
        "kms:Update*",
        "kms:Revoke*",
        "kms:Disable*",
        "kms:Get*",
        "kms:Delete*",
        "kms:ScheduleKeyDeletion",
        "kms:CancelKeyDeletion"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Allow use of the key",
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "arn:aws:iam::${var.aws_id}:user/zad",
        ]
      },
      "Action": [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Allow attachment of persistent resources",
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "arn:aws:iam::${var.aws_id}:user/zad",
        ]
      },
      "Action": [
        "kms:CreateGrant",
        "kms:ListGrants",
        "kms:RevokeGrant"
      ],
      "Resource": "*",
      "Condition": {
        "Bool": {
          "kms:GrantIsForAWSResource": "true"
        }
      }
    },
    {
      "Sid": "Allow CloudTrail to encrypt logs",
      "Effect": "Allow",
      "Principal": {
        "Service": "cloudtrail.amazonaws.com"
      },
      "Action": "kms:GenerateDataKey*",
      "Resource": "*",
      "Condition": {
        "StringLike": {
          "kms:EncryptionContext:aws:cloudtrail:arn": "arn:aws:cloudtrail:*:${var.aws_id}:trail/*"
        }
      }
    },
    {
      "Sid": "Enable CloudTrail log decrypt permissions",
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "arn:aws:iam::${var.aws_id}:user/zad",
        ]
      },
      "Action": "kms:Decrypt",
      "Resource": "*",
      "Condition": {
        "Null": {
          "kms:EncryptionContext:aws:cloudtrail:arn": "false"
        }
      }
    },
    {
      "Sid": "Allow CloudTrail access",
      "Effect": "Allow",
      "Principal": {
        "Service": "cloudtrail.amazonaws.com"
      },
      "Action": "kms:DescribeKey",
      "Resource": "*"
    }
  ]
}
POLICY
}

resource "aws_kms_alias" "audit_log" {
    name = "alias/${var.deploy_prefix}-trail"
    target_key_id = "${aws_kms_key.audit_log.key_id}"
}


# Log notification topic
resource "aws_sns_topic" "audit_log_ready" {
  name = "${var.deploy_prefix}-audit-log-ready"
  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AWSCloudTrailSNSPolicy20131101",
    "Effect": "Allow",
    "Principal": {
      "Service": "cloudtrail.amazonaws.com"
    },
    "Action": "SNS:Publish",
    "Resource": "arn:aws:sns:*:*:${var.deploy_prefix}-audit-log-ready"
  }]
}
POLICY
}

# Log notification topic subscription.
resource "aws_sns_topic_subscription" "user_updates_sqs_target" {
  topic_arn = "${aws_sns_topic.audit_log_ready.arn}"
  protocol  = "${var.audit_sns_protocol}"
  endpoint  = "${var.audit_sns_endpoint}"
}

# Cloudwatch group for CloudTrail audit
resource "aws_cloudwatch_log_group" "audit_log" {
  name = "${var.deploy_prefix}-audit-log"
  retention_in_days = "30"
}

# AWS IAM Role for CloudTrail => CloudWatch push.
resource "aws_iam_role" "cloudtrail_push" {
  name = "${var.deploy_prefix}-role-ctpush"
  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "cloudtrail.amazonaws.com"
      },
      "Effect": "Allow"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy" "ctpush_policy" {
  name = "${var.deploy_prefix}-policy-ctpush"
  role = "${aws_iam_role.cloudtrail_push.id}"
  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AWSCloudTrailCreateLogStream2014110",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream"
      ],
      "Resource": [
        "arn:aws:logs:${var.aws_region}:${var.aws_id}:log-group:${var.deploy_prefix}-audit-log:log-stream:${var.aws_id}_CloudTrail_${var.aws_region}*"
      ]
    },
    {
      "Sid": "AWSCloudTrailPutLogEvents20141101",
      "Effect": "Allow",
      "Action": [
        "logs:PutLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:${var.aws_region}:${var.aws_id}:log-group:${var.deploy_prefix}-audit-log:log-stream:${var.aws_id}_CloudTrail_${var.aws_region}*"
      ]
    }
  ]
}
POLICY
}

# Audit trail
resource "aws_cloudtrail" "audit_log" {
  name = "${var.deploy_prefix}-trail"
  s3_bucket_name             = "${aws_s3_bucket.audit_log.id}"
  sns_topic_name             = "${aws_sns_topic.audit_log_ready.id}"
  kms_key_id                 = "${aws_kms_key.audit_log.arn}"
  enable_log_file_validation = true
  is_multi_region_trail      = true

  cloud_watch_logs_group_arn = "${aws_cloudwatch_log_group.audit_log.arn}"
  cloud_watch_logs_role_arn  = "${aws_iam_role.cloudtrail_push.arn}"

  tags {
    Name = "${var.deploy_name}"
    Stack = "${var.deploy_stack}"
    Component = "Audit Logs"
  }
}

# Audit log bucket
resource "aws_s3_bucket" "audit_log" {
    bucket = "${var.deploy_prefix}-audit-log"
    force_destroy = true
    policy = <<POLICY
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AWSCloudTrailAclCheck",
            "Effect": "Allow",
            "Principal": {
              "Service": "cloudtrail.amazonaws.com"
            },
            "Action": "s3:GetBucketAcl",
            "Resource": "arn:aws:s3:::${var.deploy_prefix}-audit-log"
        },
        {
            "Sid": "AWSCloudTrailWrite",
            "Effect": "Allow",
            "Principal": {
              "Service": "cloudtrail.amazonaws.com"
            },
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::${var.deploy_prefix}-audit-log/*",
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
    Component = "Audit Logs"
  }
}

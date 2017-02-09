variable "aws_id" {}
variable "aws_region" {}
variable "local_shell" {}

variable "audit_sns_protocol" {}
variable "audit_sns_endpoint" {}

#############################
# Auditing

# Terraform construction of KMS log encryption key.
resource "aws_kms_key" "aws_prod_log" {
    description         = "Log encryption key for OpsDX"
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
          "arn:aws:iam::${var.aws_id}:user/andong",
          "arn:aws:iam::${var.aws_id}:user/yanif"
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
          "arn:aws:iam::${var.aws_id}:user/andong",
          "arn:aws:iam::${var.aws_id}:user/yanif"
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
          "arn:aws:iam::${var.aws_id}:user/andong",
          "arn:aws:iam::${var.aws_id}:user/yanif"
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
          "arn:aws:iam::${var.aws_id}:user/andong",
          "arn:aws:iam::${var.aws_id}:user/yanif"
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

resource "aws_kms_alias" "aws_prod_log" {
    name = "alias/opsdx-prod-trail"
    target_key_id = "${aws_kms_key.aws_prod_log.key_id}"
}

# Log notification topic
resource "aws_sns_topic" "prod_log_ready" {
  name = "opsdx-prod-log-ready"
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
    "Resource": "arn:aws:sns:*:*:opsdx-prod-log-ready"
  }]
}
POLICY
}

# Terraform construction of sns topic subscription
# Currently, terraform does not support email subscriptions since emails must be validated.
# See: https://www.terraform.io/docs/providers/aws/r/sns_topic_subscription.html
#
# For now, we subscribe here, and must manually confirm the subscription back to SNS.
resource "null_resource" "subscribe_audit_log_sns" {
  provisioner "local-exec" {
    command = "${var.local_shell} audit/subscribe_audit_log_sns.sh ${aws_sns_topic.log_ready.arn} ${var.audit_sns_protocol} ${var.audit_sns_endpoint}"
  }
}

# Cloudwatch group for CloudTrail audit
resource "aws_cloudwatch_log_group" "prod_audit_logs" {
  name = "opsdx-prod-log-audit"
  retention_in_days = "30"
}

# AWS IAM Role for CloudTrail => CloudWatch push.
resource "aws_iam_role" "prod_cloudtrail_push" {
  name = "opsdx-prod-role-ctpush"
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

resource "aws_iam_role_policy" "prod_ctpush_policy" {
  name = "opsdx-prod-policy-ctpush"
  role = "${aws_iam_role.prod_cloudtrail_push.id}"
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
        "arn:aws:logs:${var.aws_region}:${var.aws_id}:log-group:opsdx-prod-log-audit:log-stream:${var.aws_id}_CloudTrail_${var.aws_region}*"
      ]
    },
    {
      "Sid": "AWSCloudTrailPutLogEvents20141101",
      "Effect": "Allow",
      "Action": [
        "logs:PutLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:${var.aws_region}:${var.aws_id}:log-group:opsdx-prod-log-audit:log-stream:${var.aws_id}_CloudTrail_${var.aws_region}*"
      ]
    }
  ]
}
POLICY
}

# Audit trail
resource "aws_cloudtrail" "audit_prod" {
    name = "opsdx-prod-trail"
    s3_bucket_name             = "${aws_s3_bucket.prod_audit_logs.id}"
    sns_topic_name             = "${aws_sns_topic.prod_log_ready.id}"
    kms_key_id                 = "${aws_kms_key.aws_prod_log.arn}"
    enable_log_file_validation = true
    is_multi_region_trail      = true

    cloud_watch_logs_group_arn = "${aws_cloudwatch_log_group.prod_audit_logs.arn}"
    cloud_watch_logs_role_arn  = "${aws_iam_role.prod_cloudtrail_push.arn}"
}

# Audit log bucket
resource "aws_s3_bucket" "prod_audit_logs" {
    bucket = "opsdx-prod-audit-logs"
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
            "Resource": "arn:aws:s3:::opsdx-prod-audit-logs"
        },
        {
            "Sid": "AWSCloudTrailWrite",
            "Effect": "Allow",
            "Principal": {
              "Service": "cloudtrail.amazonaws.com"
            },
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::opsdx-prod-audit-logs/*",
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-acl": "bucket-owner-full-control"
                }
            }
        }
    ]
}
POLICY
}

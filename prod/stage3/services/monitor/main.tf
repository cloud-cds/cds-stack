variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_alarm2slack_package" {}
variable "alarm2slack_kms_key_arn" {}

variable "slack_hook" {}
variable "slack_channel" {}
variable "slack_watchers" {}

## SNS Topic
resource "aws_sns_topic" "alarm_topic" {
  name = "${var.deploy_prefix}-cw-alarms"
}

## Cloudwatch to Slack lambda.
resource "aws_iam_role" "alarm2slack_lambda_role" {
    name = "${var.deploy_prefix}-role-alarm2slack-lambda"
    assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy" "alarm2slack_lambda_policy" {
  name = "${var.deploy_prefix}-policy-alarm2slack-lambda"
  role = "${aws_iam_role.alarm2slack_lambda_role.id}"
  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [ "lambda:InvokeFunction",
                  "logs:CreateLogGroup",
                  "logs:CreateLogStream",
                  "logs:PutLogEvents",
                  "ec2:CreateNetworkInterface",
                  "ec2:DescribeNetworkInterfaces",
                  "ec2:DeleteNetworkInterface",
                  "kms:Decrypt",
                  "kms:DescribeKey",
                  "kms:GetKeyPolicy"
                  ],
      "Resource": [
        "*"
      ]
    }
  ]
}
POLICY
}

resource "aws_lambda_function" "alarm2slack_lambda" {
    function_name    = "${var.deploy_prefix}-alarm2slack-lambda"
    handler          = "lib/index.handler"

    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_alarm2slack_package}"

    role             = "${aws_iam_role.alarm2slack_lambda_role.arn}"
    runtime          = "nodejs4.3"
    timeout          = 300

    kms_key_arn      = "${var.alarm2slack_kms_key_arn}"

    environment {
      variables {
        SLACK_HOOK     = "${var.slack_hook}"
        SLACK_CHANNEL  = "${var.slack_channel}"
        SLACK_WATCHERS = "${var.slack_watchers}"
      }
    }
}

resource "aws_sns_topic_subscription" "alarm2slack_subscription" {
  depends_on = ["aws_lambda_function.alarm2slack_lambda"]
  topic_arn = "${aws_sns_topic.alarm_topic.arn}"
  protocol = "lambda"
  endpoint = "${aws_lambda_function.alarm2slack_lambda.arn}"
}

resource "aws_lambda_permission" "alarm2slack_from_sns" {
  statement_id  = "AllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = "${aws_lambda_function.alarm2slack_lambda.arn}"
  principal     = "sns.amazonaws.com"
  source_arn    = "${aws_sns_topic.alarm_topic.arn}"
}

## Alarms

# Node failures
resource "aws_cloudwatch_metric_alarm" "nodes_inservice" {
  alarm_name                = "${var.deploy_prefix}-nodes-inservice"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "1"
  metric_name               = "GroupInServiceInstances"
  namespace                 = "AWS/AutoScaling"
  period                    = "60"
  statistic                 = "Minimum"
  threshold                 = "5"
  alarm_description         = "Prod nodes in service for the last minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    AutoScalingGroupName = "nodes.cluster.prod.opsdx.io"
  }
}

# ETL failures
resource "aws_cloudwatch_metric_alarm" "etl_up" {
  alarm_name                = "${var.deploy_prefix}-etl-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "1"
  metric_name               = "ExTrLoTime"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "Prod ETL invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    ETL = "opsdx-prod"
  }
}

# Ping failures
resource "aws_cloudwatch_metric_alarm" "ping_up" {
  alarm_name                = "${var.deploy_prefix}-ping-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "1"
  metric_name               = "ExternalLatency"
  namespace                 = "OpsDX"
  period                    = "300"
  statistic                 = "SampleCount"
  threshold                 = "90"
  alarm_description         = "Ping throughput in the past 5 minutes"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    Source = "damsl.cs.jhu.edu",
    Stack  = "Prod"
  }
}
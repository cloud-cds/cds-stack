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

resource "aws_cloudwatch_metric_alarm" "nodes_inservice" {
  alarm_name                = "${var.deploy_prefix}-nodes-inservice"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "1"
  metric_name               = "GroupInServiceInstances"
  namespace                 = "AWS/AutoScaling"
  period                    = "60"
  statistic                 = "Minimum"
  threshold                 = "2"
  alarm_description         = "General purpose dev nodes in service for the last minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    AutoScalingGroupName = "nodes.cluster.dev.opsdx.io"
  }
}

resource "aws_cloudwatch_metric_alarm" "web_nodes_inservice" {
  alarm_name                = "${var.deploy_prefix}-web-nodes-inservice"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "1"
  metric_name               = "GroupInServiceInstances"
  namespace                 = "AWS/AutoScaling"
  period                    = "60"
  statistic                 = "Minimum"
  threshold                 = "2"
  alarm_description         = "Webservers in service for the last minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    AutoScalingGroupName = "web.cluster.dev.opsdx.io"
  }
}

resource "aws_cloudwatch_metric_alarm" "etl_nodes_inservice" {
  alarm_name                = "${var.deploy_prefix}-etl-nodes-inservice"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "1"
  metric_name               = "GroupInServiceInstances"
  namespace                 = "AWS/AutoScaling"
  period                    = "60"
  statistic                 = "Minimum"
  threshold                 = "1"
  alarm_description         = "ETL nodes in service for the last minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    AutoScalingGroupName = "etl.cluster.dev.opsdx.io"
  }
}

# ETL failures
resource "aws_cloudwatch_metric_alarm" "hcgh_etl_up" {
  alarm_name                = "${var.deploy_prefix}-hcgh-etl-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "HCGH_NumBeddedPatients"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "HCGH Dev ETL invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    ETL = "opsdx_dev"
  }
}

resource "aws_cloudwatch_metric_alarm" "bmc_etl_up" {
  alarm_name                = "${var.deploy_prefix}-bmc-etl-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "BMC_NumBeddedPatients"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "BMC Dev ETL invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    ETL = "opsdx_dev"
  }
}

resource "aws_cloudwatch_metric_alarm" "jhh_etl_up" {
  alarm_name                = "${var.deploy_prefix}-jhh-etl-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "JHH_NumBeddedPatients"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "JHH Dev ETL invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    ETL = "opsdx_dev"
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
  threshold                 = "45"
  alarm_description         = "Ping throughput in the past 5 minutes"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    PingStack  = "Dev"
  }
}

# High webservice latency
resource "aws_cloudwatch_metric_alarm" "high_webservice_latency" {
  alarm_name                = "${var.deploy_prefix}-high-webservice-latency"
  comparison_operator       = "GreaterThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "LatencyAvg"
  namespace                 = "OpsDX"
  period                    = "60"
  statistic                 = "Average"
  threshold                 = "300"
  alarm_description         = "Average webservice latency in the past minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    Route = "/api"
    API   = "opsdx-dev"
    MetricStreamId = "0d249909-8586-45d2-9920-85338b93aa10"
  }
}

# High browser latency
resource "aws_cloudwatch_metric_alarm" "high_browser_latency" {
  alarm_name                = "${var.deploy_prefix}-high-browser-latency"
  comparison_operator       = "GreaterThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "UserLatencyAvg"
  namespace                 = "OpsDX"
  period                    = "60"
  statistic                 = "Average"
  threshold                 = "1000"
  alarm_description         = "Average browser-side latency in the past minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    Browser = "opsdx-dev"
  }
}


# Real-time database overload alarms.
resource "aws_cloudwatch_metric_alarm" "db_overload" {
  alarm_name                = "${var.deploy_prefix}-db-overload"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "72"
  metric_name               = "CPUUtilization"
  namespace                 = "AWS/RDS"
  period                    = "300"
  statistic                 = "Average"
  threshold                 = "85"
  alarm_description         = "DB CPU utilization in the past 6 hours"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    DBInstanceIdentifier = "opsdx-dev"
  }
}

# Database free space alarms.
resource "aws_cloudwatch_metric_alarm" "db_low_space" {
  alarm_name                = "${var.deploy_prefix}-db-low-space"
  comparison_operator       = "LessThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "FreeStorageSpace"
  namespace                 = "AWS/RDS"
  period                    = "60"
  statistic                 = "Average"
  threshold                 = "20000000000"
  alarm_description         = "DB free space in the past minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    DBInstanceIdentifier = "opsdx-dev"
  }
}

resource "aws_cloudwatch_metric_alarm" "dw_low_space" {
  alarm_name                = "${var.deploy_prefix}-dw-low-space"
  comparison_operator       = "LessThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "FreeStorageSpace"
  namespace                 = "AWS/RDS"
  period                    = "60"
  statistic                 = "Average"
  threshold                 = "50000000000"
  alarm_description         = "DW free space in the past minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    DBInstanceIdentifier = "opsdx-dev-dw"
  }
}

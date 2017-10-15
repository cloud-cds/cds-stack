variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_alarm2slack_package" {}
variable "alarm2slack_kms_key_arn" {}

variable "slack_hook" {}
variable "slack_channel" {}
variable "slack_watchers" {}

## SNS Topic
resource "aws_sns_topic" "alarm_topic" {
  name = "${var.deploy_prefix}-dev-cw-alarms"
}

## Cloudwatch to Slack lambda.
resource "aws_iam_role" "alarm2slack_lambda_role" {
    name = "${var.deploy_prefix}-dev-role-alarm2slack-lambda"
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
  name = "${var.deploy_prefix}-dev-policy-alarm2slack-lambda"
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
    function_name    = "${var.deploy_prefix}-dev-alarm2slack-lambda"
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
  alarm_name                = "${var.deploy_prefix}-dev-nodes-inservice"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "1"
  metric_name               = "GroupInServiceInstances"
  namespace                 = "AWS/AutoScaling"
  period                    = "60"
  statistic                 = "Minimum"
  threshold                 = "1"
  alarm_description         = "General purpose dev nodes in service for the last minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    AutoScalingGroupName = "nodes.cluster-dev.jh.opsdx.io"
  }
}

resource "aws_cloudwatch_metric_alarm" "spot_nodes_inservice" {
  alarm_name                = "${var.deploy_prefix}-dev-spot-nodes-inservice"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "1"
  metric_name               = "GroupInServiceInstances"
  namespace                 = "AWS/AutoScaling"
  period                    = "60"
  statistic                 = "Minimum"
  threshold                 = "2"
  alarm_description         = "Dev spot instances in service for the last minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    AutoScalingGroupName = "spot-nodes.cluster-dev.jh.opsdx.io"
  }
}


# ETL failures
resource "aws_cloudwatch_metric_alarm" "hcgh_etl_up" {
  alarm_name                = "${var.deploy_prefix}-dev-hcgh-etl-up"
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
  alarm_name                = "${var.deploy_prefix}-dev-bmc-etl-up"
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
  alarm_name                = "${var.deploy_prefix}-dev-jhh-etl-up"
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

# Alert server/predictor failures
resource "aws_cloudwatch_metric_alarm" "hcgh_alertserver_up" {
  alarm_name                = "${var.deploy_prefix}-dev-hcgh-alertserver-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "e2e_time_HCGH"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "HCGH Dev Alert Server and Predictor invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    "Alert Server" = "dev"
  }
}

# TODO: switch to e2e_time_{JHH/BMC} when predictions are enabled.
resource "aws_cloudwatch_metric_alarm" "bmc_alertserver_up" {
  alarm_name                = "${var.deploy_prefix}-dev-bmc-alertserver-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "etl_done_BMC"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "BMC Dev Alert Server ETL-Done messages in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    "Alert Server" = "dev"
  }
}

resource "aws_cloudwatch_metric_alarm" "jhh_alertserver_up" {
  alarm_name                = "${var.deploy_prefix}-dev-jhh-alertserver-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "etl_done_JHH"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "JHH Dev Alert Server ETL-Done messages in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    "Alert Server" = "dev"
  }
}

# Ping failures
resource "aws_cloudwatch_metric_alarm" "ping_up" {
  alarm_name                = "${var.deploy_prefix}-dev-ping-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "1"
  metric_name               = "ExternalLatency"
  namespace                 = "OpsDX"
  period                    = "300"
  statistic                 = "SampleCount"
  threshold                 = "45"
  treat_missing_data        = "breaching"
  alarm_description         = "Ping throughput in the past 5 minutes"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]
  dimensions {
    PingStack  = "Dev"
  }
}

# High webservice latency
resource "aws_cloudwatch_metric_alarm" "high_webservice_latency" {
  alarm_name                = "${var.deploy_prefix}-dev-high-webservice-latency"
  comparison_operator       = "GreaterThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "LatencyAvg"
  namespace                 = "OpsDX"
  period                    = "60"
  statistic                 = "Average"
  threshold                 = "1000"
  treat_missing_data        = "notBreaching"
  alarm_description         = "Average webservice latency in the past minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]
  dimensions {
    API   = "opsdx-dev"
  }
}

# High browser latency
resource "aws_cloudwatch_metric_alarm" "high_browser_latency" {
  alarm_name                = "${var.deploy_prefix}-dev-high-browser-latency"
  comparison_operator       = "GreaterThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "UserLatencyAvg"
  namespace                 = "OpsDX"
  period                    = "60"
  statistic                 = "Average"
  threshold                 = "1000"
  treat_missing_data        = "notBreaching"
  alarm_description         = "Average browser-side latency in the past minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]
  dimensions {
    Browser = "opsdx-dev"
  }
}


# Real-time database overload alarms.
resource "aws_cloudwatch_metric_alarm" "db_overload" {
  alarm_name                = "${var.deploy_prefix}-dev-db-overload"
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
    DBInstanceIdentifier = "${var.deploy_prefix}-dev"
  }
}

# Database free space alarms.
resource "aws_cloudwatch_metric_alarm" "db_low_space" {
  alarm_name                = "${var.deploy_prefix}-dev-db-low-space"
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
    DBInstanceIdentifier = "${var.deploy_prefix}-dev"
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
    DBInstanceIdentifier = "${var.deploy_prefix}-dw"
  }
}

# Predictor and alert server alarms.
resource "aws_cloudwatch_metric_alarm" "hcgh_predictor_up" {
  alarm_name                = "${var.deploy_prefix}-dev-hcgh-predictor-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "total_time_long"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "5"
  alarm_description         = "HCGH Dev Predictor invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    LMCPredictor = "dev"
  }
}

resource "aws_cloudwatch_metric_alarm" "hcgh_e2e_up" {
  alarm_name                = "${var.deploy_prefix}-dev-hcgh-e2e-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "e2e_time_HCGH"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "HCGH Dev Predictor invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    AlertServer = "dev"
  }
}

# JH API request alarms.
resource "aws_cloudwatch_metric_alarm" "jh_api_request_error" {
  alarm_name                = "${var.deploy_prefix}-dev-jh-api-request-error"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "jh_api_request_error"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "Maximum"
  threshold                 = "1"
  alarm_description         = "Dev JH API request error in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    ETL = "dev"
  }
}
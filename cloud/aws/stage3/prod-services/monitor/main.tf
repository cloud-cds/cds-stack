variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_alarm2slack_package" {}
variable "alarm2slack_kms_key_arn" {}

variable "slack_hook" {}
variable "slack_channel" {}
variable "slack_watchers" {}

variable "info_slack_hook" {}
variable "info_slack_channel" {}
variable "info_slack_watchers" {}

## SNS Topic
resource "aws_sns_topic" "alarm_topic" { # Critical channel
  name = "${var.deploy_prefix}-prod-cw-alarms"
}

resource "aws_sns_topic" "info_alarm_topic" {
  name = "${var.deploy_prefix}-prod-info-cw-alarms"
}

## Cloudwatch to Slack lambda.
resource "aws_iam_role" "alarm2slack_lambda_role" {
    name = "${var.deploy_prefix}-prod-role-alarm2slack-lambda"
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
  name = "${var.deploy_prefix}-prod-policy-alarm2slack-lambda"
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
    function_name    = "${var.deploy_prefix}-prod-alarm2slack-lambda"
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

resource "aws_lambda_function" "info_alarm2slack_lambda" {
    function_name    = "${var.deploy_prefix}-prod-info-alarm2slack-lambda"
    handler          = "lib/index.handler"

    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_alarm2slack_package}"

    role             = "${aws_iam_role.alarm2slack_lambda_role.arn}"
    runtime          = "nodejs4.3"
    timeout          = 300

    kms_key_arn      = "${var.alarm2slack_kms_key_arn}"

    environment {
      variables {
        SLACK_HOOK     = "${var.info_slack_hook}"
        SLACK_CHANNEL  = "${var.info_slack_channel}"
        SLACK_WATCHERS = "${var.info_slack_watchers}"
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

resource "aws_sns_topic_subscription" "info_alarm2slack_subscription" {
  depends_on = ["aws_lambda_function.info_alarm2slack_lambda"]
  topic_arn = "${aws_sns_topic.info_alarm_topic.arn}"
  protocol = "lambda"
  endpoint = "${aws_lambda_function.info_alarm2slack_lambda.arn}"
}

resource "aws_lambda_permission" "info_alarm2slack_from_sns" {
  statement_id  = "AllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = "${aws_lambda_function.info_alarm2slack_lambda.arn}"
  principal     = "sns.amazonaws.com"
  source_arn    = "${aws_sns_topic.info_alarm_topic.arn}"
}


## Alarms

# Node failures
resource "aws_cloudwatch_metric_alarm" "nodes_inservice" {
  alarm_name                = "${var.deploy_prefix}-prod-nodes-inservice"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "1"
  metric_name               = "GroupInServiceInstances"
  namespace                 = "AWS/AutoScaling"
  period                    = "60"
  statistic                 = "Minimum"
  threshold                 = "0"
  alarm_description         = "Prod nodes in service for the last minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    AutoScalingGroupName = "nodes.cluster-prod.jh.opsdx.io"
  }
}

resource "aws_cloudwatch_metric_alarm" "web_nodes_inservice" {
  alarm_name                = "${var.deploy_prefix}-prod-web-nodes-inservice"
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
    AutoScalingGroupName = "web.cluster-prod.jh.opsdx.io"
  }
}

resource "aws_cloudwatch_metric_alarm" "etl_nodes_inservice" {
  alarm_name                = "${var.deploy_prefix}-prod-etl-nodes-inservice"
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
    AutoScalingGroupName = "etl.cluster-prod.jh.opsdx.io"
  }
}

# ETL failures
resource "aws_cloudwatch_metric_alarm" "hcgh_etl_up" {
  alarm_name                = "${var.deploy_prefix}-prod-hcgh-etl-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "HCGH_NumBeddedPatients"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "HCGH Prod ETL invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    ETL = "opsdx_prod"
  }
}

resource "aws_cloudwatch_metric_alarm" "bmc_etl_up" {
  alarm_name                = "${var.deploy_prefix}-prod-bmc-etl-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "BMC_NumBeddedPatients"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "BMC Prod ETL invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    ETL = "opsdx_prod"
  }
}

resource "aws_cloudwatch_metric_alarm" "jhh_etl_up" {
  alarm_name                = "${var.deploy_prefix}-prod-jhh-etl-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "JHH_NumBeddedPatients"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "JHH Prod ETL invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    ETL = "opsdx_prod"
  }
}

# Ping failures
resource "aws_cloudwatch_metric_alarm" "ping_up" {
  alarm_name                = "${var.deploy_prefix}-prod-ping-up"
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
    PingStack  = "Prod"
  }
}

# # High webservice latency
resource "aws_cloudwatch_metric_alarm" "high_webservice_latency" {
  alarm_name                = "${var.deploy_prefix}-prod-high-webservice-latency"
  comparison_operator       = "GreaterThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "LatencyAvg"
  namespace                 = "OpsDX"
  period                    = "60"
  statistic                 = "Average"
  threshold                 = "300"
  treat_missing_data        = "notBreaching"
  alarm_description         = "Average webservice latency in the past minute"
  alarm_actions             = ["${aws_sns_topic.info_alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.info_alarm_topic.arn}"]
  dimensions {
    API   = "opsdx-prod"
  }
}

# High browser latency
resource "aws_cloudwatch_metric_alarm" "high_browser_latency" {
  alarm_name                = "${var.deploy_prefix}-prod-high-browser-latency"
  comparison_operator       = "GreaterThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "UserLatencyAvg"
  namespace                 = "OpsDX"
  period                    = "60"
  statistic                 = "Average"
  threshold                 = "1000"
  treat_missing_data        = "notBreaching"
  alarm_description         = "Average browser-side latency in the past minute"
  alarm_actions             = ["${aws_sns_topic.info_alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.info_alarm_topic.arn}"]
  dimensions {
    Browser = "opsdx-prod"
  }
}

# ELB Backend connection failures per zone.
resource "aws_cloudwatch_metric_alarm" "elb_connfail_1a" {
  alarm_name                = "${var.deploy_prefix}-prod-elb-connections-failed-1a"
  comparison_operator       = "GreaterThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "BackendConnectionErrors"
  namespace                 = "AWS/ELB"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "0"
  treat_missing_data        = "notBreaching"
  alarm_description         = "Number of ELB connection failures in the past minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    LoadBalancerName = "a1dd29d6490c511e7ac9d0aea3e03fc4"
    AvailabilityZone = "us-east-1a"
  }
}

resource "aws_cloudwatch_metric_alarm" "elb_connfail_1c" {
  alarm_name                = "${var.deploy_prefix}-prod-elb-connections-failed-1c"
  comparison_operator       = "GreaterThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "BackendConnectionErrors"
  namespace                 = "AWS/ELB"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "0"
  treat_missing_data        = "notBreaching"
  alarm_description         = "Number of ELB connection failures in the past minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    LoadBalancerName = "a1dd29d6490c511e7ac9d0aea3e03fc4"
    AvailabilityZone = "us-east-1c"
  }
}

resource "aws_cloudwatch_metric_alarm" "elb_connfail_1d" {
  alarm_name                = "${var.deploy_prefix}-prod-elb-connections-failed-1d"
  comparison_operator       = "GreaterThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "BackendConnectionErrors"
  namespace                 = "AWS/ELB"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "0"
  treat_missing_data        = "notBreaching"
  alarm_description         = "Number of ELB connection failures in the past minute"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    LoadBalancerName = "a1dd29d6490c511e7ac9d0aea3e03fc4"
    AvailabilityZone = "us-east-1d"
  }
}

# Real-time database overload alarms.
resource "aws_cloudwatch_metric_alarm" "db_overload" {
  alarm_name                = "${var.deploy_prefix}-prod-db-overload"
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
    DBInstanceIdentifier = "${var.deploy_prefix}-prod"
  }
}

# Database free space alarms.
resource "aws_cloudwatch_metric_alarm" "db_low_space" {
  alarm_name                = "${var.deploy_prefix}-prod-db-low-space"
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
    DBInstanceIdentifier = "${var.deploy_prefix}-prod"
  }
}

# Predictor and alert server alarms.
resource "aws_cloudwatch_metric_alarm" "hcgh_predictor_up" {
  alarm_name                = "${var.deploy_prefix}-prod-hcgh-predictor-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "total_time_long"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "HCGH Prod Predictor invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    LMCPredictor = "prod"
  }
}

resource "aws_cloudwatch_metric_alarm" "hcgh_e2e_up" {
  alarm_name                = "${var.deploy_prefix}-prod-hcgh-e2e-up"
  comparison_operator       = "LessThanThreshold"
  evaluation_periods        = "2"
  metric_name               = "e2e_time_HCGH"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "SampleCount"
  threshold                 = "3"
  alarm_description         = "HCGH Prod Predictor invocations in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    AlertServer = "prod"
  }
}

# JH API request alarms.
resource "aws_cloudwatch_metric_alarm" "jh_api_request_error" {
  alarm_name                = "${var.deploy_prefix}-prod-jh-api-request-error"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "jh_api_request_error"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "Maximum"
  threshold                 = "1"
  alarm_description         = "Prod JH API request error in the past hour"
  alarm_actions             = ["${aws_sns_topic.info_alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.info_alarm_topic.arn}"]

  dimensions {
    ETL = "prod"
  }
}


resource "aws_cloudwatch_metric_alarm" "trews_alert_count_on_hcgh_ed" {
  alarm_name                = "${var.deploy_prefix}-prod-trews-alert-count-hcgh-ed"
  comparison_operator       = "LessThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "alert_count_any_trews_HCGH_EMERGENCY-ADULTS"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "Minimum"
  threshold                 = "12"
  alarm_description         = "The number of Trews alerts fired at HCGH ED in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    analysis = "opsdx-jh-prod"
  }
}

#resource "aws_cloudwatch_metric_alarm" "cms_alert_count_on_hcgh_ed" {
#  alarm_name                = "${var.deploy_prefix}-prod-cms-alert-count-hcgh-ed"
#  comparison_operator       = "LessThanOrEqualToThreshold"
#  evaluation_periods        = "2"
#  metric_name               = "alert_count_any_cms_HCGH_EMERGENCY-ADULTS"
#  namespace                 = "OpsDX"
#  period                    = "3600"
#  statistic                 = "Minimum"
#  threshold                 = "4"
#  alarm_description         = "The number of CMS alerts fired at HCGH ED in the past hour"
#  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
#  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]
#  dimensions {
#    analysis = "opsdx-jh-prod"
#  }
#}

resource "aws_cloudwatch_metric_alarm" "trews_alert_count_on_hcgh_3c_icu" {
  alarm_name                = "${var.deploy_prefix}-prod-trews-alert-count-hcgh-3c-icu"
  comparison_operator       = "LessThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "alert_count_any_trews_HCGH_3C_ICU"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "Minimum"
  threshold                 = "4"
  alarm_description         = "The number of Trews alerts fired at HCGH 3C ICU in the past hour"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    analysis = "opsdx-jh-prod"
  }
}

#resource "aws_cloudwatch_metric_alarm" "cms_alert_count_on_hcgh_3c_icu" {
#  alarm_name                = "${var.deploy_prefix}-prod-cms-alert-count-hcgh-3c-icu"
#  comparison_operator       = "LessThanOrEqualToThreshold"
#  evaluation_periods        = "2"
#  metric_name               = "alert_count_any_cms_HCGH_3C_ICU"
#  namespace                 = "OpsDX"
#  period                    = "3600"
#  statistic                 = "Minimum"
#  threshold                 = "4"
#  alarm_description         = "The number of CMS alerts fired at HCGH 3C ICU in the past hour"
#  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
#  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]
#  dimensions {
#    analysis = "opsdx-jh-prod"
#  }
#}

#resource "aws_cloudwatch_metric_alarm" "cms_alert_count_8hr" {
#  alarm_name                = "${var.deploy_prefix}-prod-cms-alert-count-8hr"
#  comparison_operator       = "LessThanOrEqualToThreshold"
#  evaluation_periods        = "2"
#  metric_name               = "alert_count_cms_8hr"
#  namespace                 = "OpsDX"
#  period                    = "3600"
#  statistic                 = "Minimum"
#  threshold                 = "1"
#  alarm_description         = "The number of CMS alerts fired in the past 8 hours"
#  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
#  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]
#  dimensions {
#    analysis = "opsdx-jh-prod"
#  }
#}

resource "aws_cloudwatch_metric_alarm" "trews_alert_count_8hr" {
  alarm_name                = "${var.deploy_prefix}-prod-trews-alert-count-8hr"
  comparison_operator       = "LessThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "alert_count_trews_8hr"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "Minimum"
  threshold                 = "1"
  alarm_description         = "The number of Trews alerts fired in the past 8 hours"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    analysis = "opsdx-jh-prod"
  }
}

resource "aws_cloudwatch_metric_alarm" "epic_push_notification_failure" {
  alarm_name                = "${var.deploy_prefix}-prod-epic-push-notification-failure"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  metric_name               = "FSPushCountFailures"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "Minimum"
  threshold                 = "1"
  alarm_description         = "The number of notification counts failed to push to Epic"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    API = "opsdx-prod"
  }
}

resource "aws_cloudwatch_metric_alarm" "epic_push_notification_success" {
  alarm_name                = "${var.deploy_prefix}-prod-epic-push-notification-success"
  comparison_operator       = "LessThanOrEqualToThreshold"
  evaluation_periods        = "8"
  metric_name               = "FSPushCountFailures"
  namespace                 = "OpsDX"
  period                    = "3600"
  statistic                 = "Minimum"
  threshold                 = "0"
  alarm_description         = "The number of notification counts succeed to push to Epic"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]

  dimensions {
    API = "opsdx-prod"
  }
}


resource "aws_cloudwatch_metric_alarm" "event_count_prod" {
  alarm_name                = "${var.deploy_prefix}-event-count-prod"
  comparison_operator       = "LessThanOrEqualToThreshold"
  evaluation_periods        = "5"
  metric_name               = "EventCount"
  namespace                 = "OpsDX"
  period                    = "60"
  statistic                 = "SampleCount"
  threshold                 = "400"
  alarm_description         = "The number of event counts from prod fired in the past 5 minutes"
  alarm_actions             = ["${aws_sns_topic.alarm_topic.arn}"]
  ok_actions                = ["${aws_sns_topic.alarm_topic.arn}"]
  treat_missing_data        = "breaching"
  dimensions {
    API = "opsdx-prod"
  }
}
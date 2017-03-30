variable "aws_region" {}

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_behamon_lambda_package" {}
variable "aws_behamon_lambda_role_arn" {}
variable "behavior_monitors_timeseries_firing_rate_min" {}

variable "behamon_log_group_name" {}
variable "behamon_log_group_arn" {}

variable "db_host" {}
variable "db_port" { default = 5432 }
variable "db_name" {}
variable "db_username" {}
variable "db_password" {}

variable "db_subnet1_id" {}
variable "db_subnet2_id" {}
variable "db_sg_id" {}

data "aws_subnet" "db_subnet1" {
  id = "${var.db_subnet1_id}"
}

data "aws_subnet" "db_subnet2" {
  id = "${var.db_subnet2_id}"
}

data "aws_security_group" "db_sg" {
  id = "${var.db_sg_id}"
}

resource "aws_lambda_function" "behamon_lambda_watcher" {

    function_name    = "${var.deploy_prefix}-behamon_lambda_watcher"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_behamon_lambda_package}"
    role             = "${var.aws_behamon_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300

    vpc_config {
      subnet_ids         = ["${data.aws_subnet.db_subnet1.id}", "${data.aws_subnet.db_subnet2.id}"]
      security_group_ids = ["${data.aws_security_group.db_sg.id}"]
    }

    environment {
      variables {
        db_host     = "${var.db_host}"
        db_port     = "${var.db_port}"
        db_name     = "${var.db_name}"
        db_user     = "${var.db_username}"
        db_password = "${var.db_password}"
      }
    }
}

resource "aws_cloudwatch_log_subscription_filter" "behamon_lambda_watcher_logfilter" {
  name            = "${var.deploy_prefix}-behamon_lambda_watcher_logfilter"
  log_group_name  = "${var.behamon_log_group_name}"
  filter_pattern  = "{ $.req.url = \"*USERID*\" }"
  destination_arn = "${aws_lambda_function.behamon_lambda_watcher.arn}"
}

resource "aws_lambda_permission" "behamon_lambda_watcher_permissions" {
    statement_id  = "LogBasedExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.behamon_lambda_watcher.function_name}"
    principal     = "logs.${var.aws_region}.amazonaws.com"
    source_arn    = "${var.behamon_log_group_arn}"
}

resource "aws_lambda_function" "behamon_lambda_time_series" {

    function_name    = "${var.deploy_prefix}-behamon_lambda_time_series"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_behamon_lambda_package}"
    role             = "${var.aws_behamon_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300

    vpc_config {
      subnet_ids         = ["${data.aws_subnet.db_subnet1.id}", "${data.aws_subnet.db_subnet2.id}"]
      security_group_ids = ["${data.aws_security_group.db_sg.id}"]
    }

    environment {
      variables {
        db_host     = "${var.db_host}"
        db_port     = "${var.db_port}"
        db_name     = "${var.db_name}"
        db_user     = "${var.db_username}"
        db_password = "${var.db_password}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "behamon_lambda_time_series_rule" {
    name = "${var.deploy_prefix}-behamon_lambda_time_series_rule"
    description = "Fires every ${var.behavior_monitors_timeseries_firing_rate_min} minutes"
    schedule_expression = "rate(${var.behavior_monitors_timeseries_firing_rate_min} minutes)"
}

resource "aws_cloudwatch_event_target" "behamon_lambda_time_series_target" {
    rule      = "${aws_cloudwatch_event_rule.behamon_lambda_time_series_rule.name}"
    target_id = "${var.deploy_prefix}-behamon_lambda_time_series_target"
    arn       = "${aws_lambda_function.behamon_lambda_time_series.arn}"
}

resource "aws_lambda_permission" "behamon_lambda_time_series_permissions" {
    statement_id  = "behamon_ts_period"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.behamon_lambda_time_series.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.behamon_lambda_time_series_rule.arn}"
}


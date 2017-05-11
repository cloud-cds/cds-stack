variable "aws_region" {}

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_behamon_lambda_package" {}
variable "aws_behamon_lambda_role_arn" {}

variable "behavior_monitors_timeseries_firing_rate_min" {}
variable "behavior_monitors_reports_firing_rate_min" {}
variable "behavior_monitors_reports_firing_rate_expr" {}

variable "behamon_stack"              {}
variable "behamon_log_group_name"     {}
variable "behamon_log_group_arn"      {}
variable "behamon_web_filt_str"       {}
variable "behamon_web_log_stream_str" {}

variable "db_host" {}
variable "db_port" { default = 5432 }
variable "db_name" {}
variable "db_username" {}
variable "db_password" {}

variable "lambda_subnet1_id" {}
variable "lambda_subnet2_id" {}
variable "lambda_sg_id" {}

data "aws_subnet" "lambda_subnet1" {
  id = "${var.lambda_subnet1_id}"
}

data "aws_subnet" "lambda_subnet2" {
  id = "${var.lambda_subnet2_id}"
}

data "aws_security_group" "lambda_sg" {
  id = "${var.lambda_sg_id}"
}


## Behavior Monitoring Watcher

resource "aws_lambda_function" "behamon_lambda_watcher" {

    function_name    = "${var.deploy_prefix}-behamon_lambda_watcher"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_behamon_lambda_package}"
    role             = "${var.aws_behamon_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300

    vpc_config {
      subnet_ids         = ["${data.aws_subnet.lambda_subnet1.id}", "${data.aws_subnet.lambda_subnet2.id}"]
      security_group_ids = ["${data.aws_security_group.lambda_sg.id}"]
    }

    environment {
      variables {
        db_host     = "${var.db_host}"
        db_port     = "${var.db_port}"
        db_name     = "${var.db_name}"
        db_user     = "${var.db_username}"
        db_password = "${var.db_password}"

        BEHAMON_MODE                       = "watcher"
        BEHAMON_STACK                      = "${var.behamon_stack}"
        BEHAMON_WEB_LOG_LISTEN             = "${var.behamon_log_group_name}"
        BEHAMON_WEB_FILT_STR               = "${var.behamon_web_filt_str}"
        BEHAMON_WEB_LOG_STREAM_STR         = "${var.behamon_web_log_stream_str}"
        BEHAMON_TS_RULE_PERIOD_MINUTES     = "${var.behavior_monitors_timeseries_firing_rate_min}"
        BEHAMON_REPORT_RULE_PERIOD_MINUTES = "${var.behavior_monitors_reports_firing_rate_min}"
      }
    }
}

resource "aws_lambda_permission" "behamon_lambda_watcher_permissions" {
    statement_id  = "LogBasedExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.behamon_lambda_watcher.function_name}"
    principal     = "logs.${var.aws_region}.amazonaws.com"
    source_arn    = "${var.behamon_log_group_arn}"
}

resource "aws_cloudwatch_log_subscription_filter" "behamon_lambda_watcher_logfilter" {
  depends_on      = ["aws_lambda_permission.behamon_lambda_watcher_permissions"]
  name            = "${var.deploy_prefix}-behamon_lambda_watcher_logfilter"
  log_group_name  = "${var.behamon_log_group_name}"
  filter_pattern  = "{ $.req.url = \"*USERID*\" }"
  destination_arn = "${aws_lambda_function.behamon_lambda_watcher.arn}"
}

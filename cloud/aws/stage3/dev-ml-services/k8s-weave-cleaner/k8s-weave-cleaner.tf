############################
# Epic2Op ETL via AWS Lambda

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_lambda_package" {}
variable "aws_lambda_role_arn" {}

variable "firing_rate_mins" {}

variable "k8s_dev_ml_name" {}
variable "k8s_dev_ml_server" {}
variable "k8s_dev_ml_cert_auth" {}
variable "k8s_dev_ml_user" {}
variable "k8s_dev_ml_pass" {}


# A Lambda function for periodic weave-net cleaning.

resource "aws_lambda_function" "dev_ml_weave_cleaner" {

    function_name    = "${var.deploy_prefix}-dev-ml-weave-cleaner"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_lambda_package}"
    role             = "${var.aws_lambda_role_arn}"
    runtime          = "python3.6"
    timeout          = 300

    environment {
      variables {
        kube_name      = "${var.k8s_dev_ml_name}"
        kube_server    = "${var.k8s_dev_ml_server}"
        kube_cert_auth = "${var.k8s_dev_ml_cert_auth}"
        kube_user      = "${var.k8s_dev_ml_user}"
        kube_pass      = "${var.k8s_dev_ml_pass}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "dev_ml_weave_cleaner_schedule_rule" {
    name = "${var.deploy_prefix}-dev-ml-weave-cleaner-schedule-rule"
    description = "Fires every ${var.firing_rate_mins} minutes"
    schedule_expression = "rate(${var.firing_rate_mins} minutes)"
}

resource "aws_cloudwatch_event_target" "dev_ml_weave_cleaner_schedule_target" {
    rule      = "${aws_cloudwatch_event_rule.dev_ml_weave_cleaner_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_dev_ml_weave_cleaner"
    arn       = "${aws_lambda_function.dev_ml_weave_cleaner.arn}"
}

resource "aws_lambda_permission" "dev_ml_weave_cleaner_cloudwatch_permissions" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.dev_ml_weave_cleaner.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.dev_ml_weave_cleaner_schedule_rule.arn}"
}

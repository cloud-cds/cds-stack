############################
# ETL via AWS Lambda

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_klaunch_lambda_package" {}
variable "aws_klaunch_lambda_role_arn" {}

variable "k8s_server_host" {}
variable "k8s_server_port" {}

variable "k8s_name" {}
variable "k8s_server" {}
variable "k8s_user" {}
variable "k8s_pass" {}
variable "k8s_cert_auth" {}
variable "k8s_cert" {}
variable "k8s_key" {}
variable "k8s_token" {}
variable "k8s_image" {}

variable "db_host" {}
variable "db_port" { default = 5432 }
variable "db_name" {}
variable "db_username" {}
variable "db_password" {}
variable "op2dw_etl_remote_server" {}
variable "op2dw_dataset_id" {}
variable "op2dw_model_id" {}
variable "op2dw_etl_lambda_firing_rate_mins" {}

# ETL Lambda functions for production and development databases.

resource "aws_lambda_function" "op2dw_etl_lambda" {

    function_name    = "${var.deploy_prefix}-op2dw-etl-lambda"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_klaunch_lambda_package}"
    role             = "${var.aws_klaunch_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300

    environment {
      variables {
        PYKUBE_KUBERNETES_SERVICE_HOST = "${var.k8s_server_host}"
        PYKUBE_KUBERNETES_SERVICE_PORT = "${var.k8s_server_port}"

        kube_job_name  = "op2dw-dev"
        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        kube_image     = "${var.k8s_image}"
        #kube_cert      = "${var.k8s_cert}"
        #kube_key       = "${var.k8s_key}"
        #kube_token     = "${var.k8s_token}"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /dashan-etl/etl/op2dw/engine.py"

        k8s_job_db_host           = "${var.db_host}"
        k8s_job_db_port           = "${var.db_port}"
        k8s_job_db_name           = "${var.db_name}"
        k8s_job_db_user           = "${var.db_username}"
        k8s_job_db_password       = "${var.db_password}"
        k8s_job_etl_remote_server = "${var.op2dw_etl_remote_server}"
        k8s_job_OP2DW_DATASET_ID  = "${var.op2dw_dataset_id}"
        k8s_job_OP2DW_MODEL_ID    = "${var.op2dw_model_id}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "op2dw_etl_schedule_rule" {
    name = "${var.deploy_prefix}-op2dw-etl-schedule-rule"
    description = "Fires every ${var.op2dw_etl_lambda_firing_rate_mins} minutes"
    schedule_expression = "rate(${var.op2dw_etl_lambda_firing_rate_mins} minutes)"
}

resource "aws_cloudwatch_event_target" "op2dw_etl_schedule_target" {
    rule      = "${aws_cloudwatch_event_rule.op2dw_etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_op2dw_etl_lambda"
    arn       = "${aws_lambda_function.op2dw_etl_lambda.arn}"
}

resource "aws_lambda_permission" "op2dw_etl_cloudwatch_permissions" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.op2dw_etl_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.op2dw_etl_schedule_rule.arn}"
}

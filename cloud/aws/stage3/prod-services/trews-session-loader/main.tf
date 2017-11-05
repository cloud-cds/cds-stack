variable "aws_region" {}

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_klaunch_lambda_package"  {}
variable "aws_klaunch_lambda_role_arn" {}

variable "db_host" {}
variable "db_port" { default = 5432 }
variable "db_name" {}
variable "db_username" {}
variable "db_password" {}

variable prod_jhapi_client_id {}
variable prod_jhapi_client_secret {}

variable "lambda_subnet1_id" {}
variable "lambda_subnet2_id" {}
variable "lambda_sg_id" {}

variable "k8s_server_host" {}
variable "k8s_server_port" {}

variable "k8s_name" {}
variable "k8s_server" {}
variable "k8s_user" {}
variable "k8s_pass" {}
variable "k8s_cert_auth" {}

variable "session_loader_firing_rate_min" {}
variable "session_loader_firing_rate_expr" {}
variable "k8s_session_loader_image" {}


data "aws_subnet" "lambda_subnet1" {
  id = "${var.lambda_subnet1_id}"
}

data "aws_subnet" "lambda_subnet2" {
  id = "${var.lambda_subnet2_id}"
}

data "aws_security_group" "lambda_sg" {
  id = "${var.lambda_sg_id}"
}



#########################################
# Session loader job runner.
#
resource "aws_lambda_function" "session_loader_lambda" {

    function_name    = "${var.deploy_prefix}_prod_session_loader_lambda"
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

        kube_job_name  = "session-loader-prod"
        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        kube_image     = "${var.k8s_session_loader_image}"
        kube_active_deadline_seconds = "300"
        kube_nodegroup = "spot-nodes"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /jobs/dev/session-loader/session_loader.py 10"

        # ETL Environment Variables
        k8s_job_epic_server                      = "prod"
        k8s_job_drop_if_empty                    = "false"
        k8s_job_push_to_epic                     = "false"
        k8s_job_db_host                          = "${var.db_host}"
        k8s_job_db_port                          = "${var.db_port}"
        k8s_job_db_name                          = "${var.db_name}"
        k8s_job_db_user                          = "${var.db_username}"
        k8s_job_db_password                      = "${var.db_password}"
        k8s_job_jhapi_client_id                  = "${var.prod_jhapi_client_id}"
        k8s_job_jhapi_client_secret              = "${var.prod_jhapi_client_secret}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "session_loader_lambda_schedule_rule" {
    name = "${var.deploy_prefix}_prod_session_loader_schedule_rule"
    description = "Fires every ${var.session_loader_firing_rate_min} minutes"
    schedule_expression = "rate(${var.session_loader_firing_rate_expr})"
}

resource "aws_cloudwatch_event_target" "session_loader_lambda_schedule_rule_target" {
    rule      = "${aws_cloudwatch_event_rule.session_loader_lambda_schedule_rule.name}"
    target_id = "${var.deploy_prefix}_prod_session_loader_lambda"
    arn       = "${aws_lambda_function.session_loader_lambda.arn}"
}

resource "aws_lambda_permission" "session_loader_cloudwatch_permissions" {
    statement_id  = "SessionLoaderSchedule"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.session_loader_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.session_loader_lambda_schedule_rule.arn}"
}

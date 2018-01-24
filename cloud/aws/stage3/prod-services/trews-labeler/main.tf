variable "aws_access_key_id" {}
variable "aws_secret_access_key" {}
variable "aws_region" {}

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_klaunch_lambda_package"  {}
variable "aws_klaunch_lambda_role_arn" {}

variable "db_password" {}

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

variable "trews_labeler_firing_rate_min" {}
variable "trews_labeler_firing_rate_expr" {}
variable "k8s_trews_labeler_image" {}


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
# TREWS labeler job runner.
#
resource "aws_lambda_function" "trews_labeler_lambda" {

    function_name    = "${var.deploy_prefix}_prod_trews_labeler_lambda"
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

        kube_job_name   = "trews-labeler-prod"
        kube_name       = "${var.k8s_name}"
        kube_server     = "${var.k8s_server}"
        kube_cert_auth  = "${var.k8s_cert_auth}"
        kube_user       = "${var.k8s_user}"
        kube_pass       = "${var.k8s_pass}"
        kube_image      = "${var.k8s_trews_labeler_image}"
        kube_nodegroup  = "spot-nodes"
        kube_privileged = "true"
        kube_active_deadline_seconds = "300"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "cd /lmc/LMC-Based-On-GPflow && ./sep2_labels_k8s.sh"

        # Job Environment Variables

        k8s_job_NODE_INDEX                = "1"
        k8s_job_K8S_DEPLOYMENT            = "True"
        k8s_job_NUM_WORKERS               = "1"
        k8s_job_NO_CULTURE_NAME_AVAILABLE = "True"
        k8s_job_HOSPITAL_REGEX            = "HCGH"
        k8s_job_DATASET_NAME              = "HCGH"
        k8s_job_DATASET_ID                = "0"
        k8s_job_LABEL_ID                  = "-1"
        k8s_job_LMCHOME                   = "/lmc/LMC-Based-On-GPflow/"
        k8s_job_DESCRIP                   = "predict"
        k8s_job_DATASOURCE                = "SQL"
        k8s_job_DATASET                   = "1"
        k8s_job_DATABASE                  = "prod"
        k8s_job_DB_PASS                   = "${var.db_password}"
        k8s_job_AWS_ACCESS_KEY_ID         = "${var.aws_access_key_id}"
        k8s_job_AWS_SECRET_ACCESS_KEY     = "${var.aws_secret_access_key}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "trews_labeler_lambda_schedule_rule" {
    name = "${var.deploy_prefix}_prod_trews_labeler_schedule_rule"
    description = "Fires every ${var.trews_labeler_firing_rate_min} minutes"
    schedule_expression = "rate(${var.trews_labeler_firing_rate_expr})"
}

resource "aws_cloudwatch_event_target" "trews_labeler_lambda_schedule_rule_target" {
    rule      = "${aws_cloudwatch_event_rule.trews_labeler_lambda_schedule_rule.name}"
    target_id = "${var.deploy_prefix}_prod_trews_labeler_lambda"
    arn       = "${aws_lambda_function.trews_labeler_lambda.arn}"
}

resource "aws_lambda_permission" "trews_labeler_cloudwatch_permissions" {
    statement_id  = "ScorecardReportSchedule"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.trews_labeler_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.trews_labeler_lambda_schedule_rule.arn}"
}

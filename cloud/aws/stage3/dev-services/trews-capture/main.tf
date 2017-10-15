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

variable "trews_capture_url" {}
variable "trews_capture_firing_rate_min" {}
variable "trews_capture_firing_rate_expr" {}
variable "k8s_trews_capture_image" {}


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
# TREWS capture job runner.
#
resource "aws_lambda_function" "trews_capture_lambda" {

    function_name    = "${var.deploy_prefix}_dev_trews_capture_lambda"
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

        kube_job_name   = "trews-capture-dev"
        kube_name       = "${var.k8s_name}"
        kube_server     = "${var.k8s_server}"
        kube_cert_auth  = "${var.k8s_cert_auth}"
        kube_user       = "${var.k8s_user}"
        kube_pass       = "${var.k8s_pass}"
        kube_image      = "${var.k8s_trews_capture_image}"
        kube_nodegroup  = "spot-nodes"
        kube_privileged = "true"
        kube_active_deadline_seconds = "300"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "aws s3 cp s3://opsdx-deployment/scripts/trews-capture.sh /tmp && chmod 755 /tmp/trews-capture.sh && /tmp/trews-capture.sh"

        # ETL Environment Variables
        k8s_job_capture_query = "select distinct p.pat_id from ( select C.enc_id, count(*) filter (where name like 'trews_subalert' and is_met) as trews_subalert, count(*) filter (where name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and is_met) as sirs, count(*) filter (where name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and is_met) as orgdf from get_criteria(null::int) C inner join ( select distinct enc_id from get_latest_enc_ids('HCGH') ) R on C.enc_id = R.enc_id group by C.enc_id ) c inner join pat_enc p on c.enc_id = p.enc_id where trews_subalert > 0 or ( sirs > 1 and orgdf > 0 )"
        k8s_job_query_id      = "dev.capture.test"
        k8s_job_output_bucket = "opsdx-clarity-etl-stage"
        k8s_job_output_dir    = "trews-capture"
        k8s_job_trews_url     = "${var.trews_capture_url}"
        k8s_job_db_host       = "${var.db_host}"
        k8s_job_db_port       = "${var.db_port}"
        k8s_job_db_name       = "${var.db_name}"
        k8s_job_db_user       = "${var.db_username}"
        k8s_job_db_password   = "${var.db_password}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "trews_capture_lambda_schedule_rule" {
    name = "${var.deploy_prefix}_dev_trews_capture_schedule_rule"
    description = "Fires every ${var.trews_capture_firing_rate_min} minutes"
    schedule_expression = "rate(${var.trews_capture_firing_rate_expr})"
}

resource "aws_cloudwatch_event_target" "trews_capture_lambda_schedule_rule_target" {
    rule      = "${aws_cloudwatch_event_rule.trews_capture_lambda_schedule_rule.name}"
    target_id = "${var.deploy_prefix}_dev_trews_capture_lambda"
    arn       = "${aws_lambda_function.trews_capture_lambda.arn}"
}

resource "aws_lambda_permission" "trews_capture_cloudwatch_permissions" {
    statement_id  = "ScorecardReportSchedule"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.trews_capture_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.trews_capture_lambda_schedule_rule.arn}"
}

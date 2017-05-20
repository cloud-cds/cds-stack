############################
# C2DW ETL via AWS Lambda

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_klaunch_lambda_package" {}
variable "aws_klaunch_lambda_role_arn" {}

variable "c2dw_etl_lambda_cron" {}

variable "k8s_server_host" {}
variable "k8s_server_port" {}

variable "k8s_name" {}
variable "k8s_server" {}
variable "k8s_user" {}
variable "k8s_pass" {}
variable "k8s_cert_auth" {}
variable "k8s_image" {}
variable "k8s_privileged" {}

variable "db_host" {}
variable "db_port" { default = 5432 }
variable "db_name" {}
variable "db_username" {}
variable "db_password" {}

variable "clarity_stage_mnt" {}
variable "dataset_id"        {}
variable "incremental"       {}
variable "remove_pat_enc"    {}
variable "remove_data"       {}
variable "start_enc_id"      {}
variable "clarity_workspace" {}
variable "nprocs"            {}
variable "num_derive_groups" {}
variable "vacuum_temp_table" {}

variable "local_shell" {}

# A Lambda function for periodic ETL.

resource "aws_lambda_function" "c2dw_etl_lambda" {

    function_name    = "${var.deploy_prefix}-c2dw-etl-lambda"
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

        kube_job_name   = "c2dw-dev-daily"
        kube_name       = "${var.k8s_name}"
        kube_server     = "${var.k8s_server}"
        kube_cert_auth  = "${var.k8s_cert_auth}"
        kube_user       = "${var.k8s_user}"
        kube_pass       = "${var.k8s_pass}"
        kube_image      = "${var.k8s_image}"
        kube_privileged = "${var.k8s_privileged}"
        #kube_cert      = "${var.k8s_cert}"
        #kube_key       = "${var.k8s_key}"
        #kube_token     = "${var.k8s_token}"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "./bin/run_c2dw.sh"

        k8s_job_db_host     = "${var.db_host}"
        k8s_job_db_port     = "${var.db_port}"
        k8s_job_db_name     = "${var.db_name}"
        k8s_job_db_user     = "${var.db_username}"
        k8s_job_db_password = "${var.db_password}"

        k8s_job_clarity_stage_mnt = "${var.clarity_stage_mnt}"
        k8s_job_dataset_id        = "${var.dataset_id}"
        k8s_job_incremental       = "${var.incremental}"
        k8s_job_remove_pat_enc    = "${var.remove_pat_enc}"
        k8s_job_remove_data       = "${var.remove_data}"
        k8s_job_start_enc_id      = "${var.start_enc_id}"
        k8s_job_clarity_workspace = "${var.clarity_workspace}"
        k8s_job_nprocs            = "${var.nprocs}"
        k8s_job_num_derive_groups = "${var.num_derive_groups}"
        k8s_job_vacuum_temp_table = "${var.vacuum_temp_table}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "c2dw_etl_schedule_rule" {
    name = "${var.deploy_prefix}-c2dw-etl-schedule-rule"
    description = "Cron of ${var.c2dw_etl_lambda_cron}"
    schedule_expression = "cron(${var.c2dw_etl_lambda_cron})"
}

resource "aws_cloudwatch_event_target" "c2dw_etl_schedule_target" {
    rule      = "${aws_cloudwatch_event_rule.c2dw_etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_c2dw_etl_lambda"
    arn       = "${aws_lambda_function.c2dw_etl_lambda.arn}"
}

resource "aws_lambda_permission" "c2dw_etl_cloudwatch_permissions" {
    statement_id  = "C2DWETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.c2dw_etl_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.c2dw_etl_schedule_rule.arn}"
}

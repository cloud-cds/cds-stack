############################
# Epic2Op ETL via AWS Lambda

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

variable "jhapi_client_id" {}
variable "jhapi_client_secret" {}
variable "etl_channel" {}

variable "etl_lambda_firing_rate_mins" {}

variable "TREWS_ETL_SERVER" {}
variable "TREWS_ETL_HOSPITAL" {}
variable "TREWS_ETL_HOURS" {}
variable "TREWS_ETL_ARCHIVE" {}
variable "TREWS_ETL_MODE" {}
variable "TREWS_ETL_DEMO_MODE" {}
variable "TREWS_ETL_STREAM_HOURS" {}
variable "TREWS_ETL_STREAM_SLICES" {}
variable "TREWS_ETL_STREAM_SLEEP_SECS" {}
variable "TREWS_ETL_EPIC_NOTIFICATIONS" {}
variable "TREWS_ETL_SUPPRESSION" {}
variable "local_shell" {}


# A Lambda function for periodic ETL.

resource "aws_lambda_function" "etl_lambda_HCGH" {

    function_name    = "${var.deploy_prefix}-etl-lambda-HCGH"
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

        kube_job_name  = "epic2op-hcgh-dev"
        kube_nodegroup = "etl"
        kube_cpu_requests = "250m"
        kube_mem_requests = "1Gi"
        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        kube_image     = "${var.k8s_image}"
        kube_active_deadline_seconds = "300"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /dashan-etl/etl/epic2op/engine.py --hospital=HCGH"

        k8s_job_db_host     = "${var.db_host}"
        k8s_job_db_port     = "${var.db_port}"
        k8s_job_db_name     = "${var.db_name}"
        k8s_job_db_user     = "${var.db_username}"
        k8s_job_db_password = "${var.db_password}"

        k8s_job_jhapi_client_id     = "${var.jhapi_client_id}"
        k8s_job_jhapi_client_secret = "${var.jhapi_client_secret}"
        k8s_job_etl_channel         = "${var.etl_channel}"

        k8s_job_TREWS_ETL_SERVER             = "${var.TREWS_ETL_SERVER}"
        k8s_job_TREWS_ETL_HOSPITAL           = "HCGH"
        k8s_job_TREWS_ETL_HOURS              = "${var.TREWS_ETL_HOURS}"
        k8s_job_TREWS_ETL_ARCHIVE            = "${var.TREWS_ETL_ARCHIVE}"
        k8s_job_TREWS_ETL_MODE               = "${var.TREWS_ETL_MODE}"
        k8s_job_TREWS_ETL_STREAM_HOURS       = "${var.TREWS_ETL_STREAM_HOURS}"
        k8s_job_TREWS_ETL_STREAM_SLICES      = "${var.TREWS_ETL_STREAM_SLICES}"
        k8s_job_TREWS_ETL_STREAM_SLEEP_SECS  = "${var.TREWS_ETL_STREAM_SLEEP_SECS}"
        k8s_job_TREWS_ETL_EPIC_NOTIFICATIONS = "${var.TREWS_ETL_EPIC_NOTIFICATIONS}"
        k8s_job_TREWS_ETL_SUPPRESSION = "${var.TREWS_ETL_SUPPRESSION}"
      }
    }
}

resource "aws_lambda_function" "etl_lambda_JHH" {

    function_name    = "${var.deploy_prefix}-etl-lambda-JHH"
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

        kube_job_name  = "epic2op-jhh-dev"
        kube_nodegroup = "etl"
        kube_cpu_requests = "500m"
        kube_mem_requests = "1Gi"
        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        kube_image     = "${var.k8s_image}"
        kube_active_deadline_seconds = "300"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /dashan-etl/etl/epic2op/engine.py --hospital=JHH"

        k8s_job_db_host     = "${var.db_host}"
        k8s_job_db_port     = "${var.db_port}"
        k8s_job_db_name     = "${var.db_name}"
        k8s_job_db_user     = "${var.db_username}"
        k8s_job_db_password = "${var.db_password}"

        k8s_job_jhapi_client_id     = "${var.jhapi_client_id}"
        k8s_job_jhapi_client_secret = "${var.jhapi_client_secret}"
        k8s_job_etl_channel         = "${var.etl_channel}"

        k8s_job_TREWS_ETL_SERVER             = "${var.TREWS_ETL_SERVER}"
        k8s_job_TREWS_ETL_HOSPITAL           = "JHH"
        k8s_job_TREWS_ETL_HOURS              = "${var.TREWS_ETL_HOURS}"
        k8s_job_TREWS_ETL_ARCHIVE            = "${var.TREWS_ETL_ARCHIVE}"
        k8s_job_TREWS_ETL_MODE               = "${var.TREWS_ETL_MODE}"
        k8s_job_TREWS_ETL_STREAM_HOURS       = "${var.TREWS_ETL_STREAM_HOURS}"
        k8s_job_TREWS_ETL_STREAM_SLICES      = "${var.TREWS_ETL_STREAM_SLICES}"
        k8s_job_TREWS_ETL_STREAM_SLEEP_SECS  = "${var.TREWS_ETL_STREAM_SLEEP_SECS}"
        k8s_job_TREWS_ETL_EPIC_NOTIFICATIONS = "${var.TREWS_ETL_EPIC_NOTIFICATIONS}"
        k8s_job_TREWS_ETL_SUPPRESSION = "${var.TREWS_ETL_SUPPRESSION}"
      }
    }
}

resource "aws_lambda_function" "etl_lambda_BMC" {

    function_name    = "${var.deploy_prefix}-etl-lambda-BMC"
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

        kube_job_name  = "epic2op-bmc-dev"
        kube_nodegroup = "etl"
        kube_cpu_requests = "250m"
        kube_mem_requests = "1Gi"
        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        kube_image     = "${var.k8s_image}"
        kube_active_deadline_seconds = "300"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /dashan-etl/etl/epic2op/engine.py --hospital=BMC"

        k8s_job_db_host     = "${var.db_host}"
        k8s_job_db_port     = "${var.db_port}"
        k8s_job_db_name     = "${var.db_name}"
        k8s_job_db_user     = "${var.db_username}"
        k8s_job_db_password = "${var.db_password}"

        k8s_job_jhapi_client_id     = "${var.jhapi_client_id}"
        k8s_job_jhapi_client_secret = "${var.jhapi_client_secret}"
        k8s_job_etl_channel         = "${var.etl_channel}"

        k8s_job_TREWS_ETL_SERVER             = "${var.TREWS_ETL_SERVER}"
        k8s_job_TREWS_ETL_HOSPITAL           = "BMC"
        k8s_job_TREWS_ETL_HOURS              = "${var.TREWS_ETL_HOURS}"
        k8s_job_TREWS_ETL_ARCHIVE            = "${var.TREWS_ETL_ARCHIVE}"
        k8s_job_TREWS_ETL_MODE               = "${var.TREWS_ETL_MODE}"
        k8s_job_TREWS_ETL_STREAM_HOURS       = "${var.TREWS_ETL_STREAM_HOURS}"
        k8s_job_TREWS_ETL_STREAM_SLICES      = "${var.TREWS_ETL_STREAM_SLICES}"
        k8s_job_TREWS_ETL_STREAM_SLEEP_SECS  = "${var.TREWS_ETL_STREAM_SLEEP_SECS}"
        k8s_job_TREWS_ETL_EPIC_NOTIFICATIONS = "${var.TREWS_ETL_EPIC_NOTIFICATIONS}"
        k8s_job_TREWS_ETL_SUPPRESSION = "${var.TREWS_ETL_SUPPRESSION}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "etl_schedule_rule" {
    name = "${var.deploy_prefix}-etl-schedule-rule"
    description = "Fires every ${var.etl_lambda_firing_rate_mins} minutes"
    schedule_expression = "rate(${var.etl_lambda_firing_rate_mins} minutes)"
}

resource "aws_cloudwatch_event_target" "etl_schedule_target_HCGH" {
    rule      = "${aws_cloudwatch_event_rule.etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_etl_lambda_HCGH"
    arn       = "${aws_lambda_function.etl_lambda_HCGH.arn}"
}

resource "aws_cloudwatch_event_target" "etl_schedule_target_JHH" {
    rule      = "${aws_cloudwatch_event_rule.etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_etl_lambda_JHH"
    arn       = "${aws_lambda_function.etl_lambda_JHH.arn}"
}

resource "aws_cloudwatch_event_target" "etl_schedule_target_BMC" {
    rule      = "${aws_cloudwatch_event_rule.etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_etl_lambda_BMC"
    arn       = "${aws_lambda_function.etl_lambda_BMC.arn}"
}

resource "aws_lambda_permission" "etl_cloudwatch_permissions_HCGH" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.etl_lambda_HCGH.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.etl_schedule_rule.arn}"
}

resource "aws_lambda_permission" "etl_cloudwatch_permissions_JHH" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.etl_lambda_JHH.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.etl_schedule_rule.arn}"
}

resource "aws_lambda_permission" "etl_cloudwatch_permissions_BMC" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.etl_lambda_BMC.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.etl_schedule_rule.arn}"
}

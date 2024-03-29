############################
# Epic2Op ETL via AWS Lambda

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_klaunch_lambda_package" {}
variable "aws_klaunch_lambda_role_arn" {}

variable "k8s_prod_server_host" {}
variable "k8s_prod_server_port" {}

variable "k8s_prod_name" {}
variable "k8s_prod_server" {}
variable "k8s_prod_user" {}
variable "k8s_prod_pass" {}
variable "k8s_prod_cert_auth" {}
variable "k8s_prod_cert" {}
variable "k8s_prod_key" {}
variable "k8s_prod_image" {}

variable "prod_db_host" {}
variable "prod_db_port" { default = 5432 }
variable "prod_db_name" {}
variable "prod_db_username" {}
variable "prod_db_password" {}

variable "prod_jhapi_client_id" {}
variable "prod_jhapi_client_secret" {}
variable "prod_etl_channel" {}

variable "prod_etl_lambda_firing_rate_mins" {}

variable "PROD_ETL_SERVER" {}
variable "PROD_ETL_HOSPITAL" {}
variable "PROD_ETL_HOURS" {}
variable "PROD_ETL_ARCHIVE" {}
variable "PROD_ETL_MODE" {}
variable "PROD_ETL_DEMO_MODE" {}
variable "PROD_ETL_STREAM_HOURS" {}
variable "PROD_ETL_STREAM_SLICES" {}
variable "PROD_ETL_STREAM_SLEEP_SECS" {}

variable "local_shell" {}


# A Lambda function for periodic ETL.

resource "aws_lambda_function" "prod_etl_lambda_HCGH" {

    function_name    = "${var.deploy_prefix}-prod-etl-lambda-HCGH"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_klaunch_lambda_package}"
    role             = "${var.aws_klaunch_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300

    environment {
      variables {
        PYKUBE_KUBERNETES_SERVICE_HOST = "${var.k8s_prod_server_host}"
        PYKUBE_KUBERNETES_SERVICE_PORT = "${var.k8s_prod_server_port}"

        kube_job_name  = "epic2op-hcgh-prod"
        kube_nodegroup = "etl"
        kube_cpu_requests = "250m"
        kube_mem_requests = "1Gi"
        kube_name      = "${var.k8s_prod_name}"
        kube_server    = "${var.k8s_prod_server}"
        kube_cert_auth = "${var.k8s_prod_cert_auth}"
        kube_user      = "${var.k8s_prod_user}"
        kube_pass      = "${var.k8s_prod_pass}"
        kube_image     = "${var.k8s_prod_image}"
        kube_active_deadline_seconds = "300"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /etl/epic2op/engine.py --hospital=HCGH"

        k8s_job_db_host     = "${var.prod_db_host}"
        k8s_job_db_port     = "${var.prod_db_port}"
        k8s_job_db_name     = "${var.prod_db_name}"
        k8s_job_db_user     = "${var.prod_db_username}"
        k8s_job_db_password = "${var.prod_db_password}"

        k8s_job_jhapi_client_id     = "${var.prod_jhapi_client_id}"
        k8s_job_jhapi_client_secret = "${var.prod_jhapi_client_secret}"
        k8s_job_etl_channel         = "${var.prod_etl_channel}"

        k8s_job_TREWS_ETL_SERVER             = "${var.PROD_ETL_SERVER}"
        k8s_job_TREWS_ETL_HOSPITAL           = "HCGH"
        k8s_job_TREWS_ETL_HOURS              = "${var.PROD_ETL_HOURS}"
        k8s_job_TREWS_ETL_ARCHIVE            = "${var.PROD_ETL_ARCHIVE}"
        k8s_job_TREWS_ETL_MODE               = "${var.PROD_ETL_MODE}"
        k8s_job_TREWS_ETL_STREAM_HOURS       = "${var.PROD_ETL_STREAM_HOURS}"
        k8s_job_TREWS_ETL_STREAM_SLICES      = "${var.PROD_ETL_STREAM_SLICES}"
        k8s_job_TREWS_ETL_STREAM_SLEEP_SECS  = "${var.PROD_ETL_STREAM_SLEEP_SECS}"
        k8s_job_TREWS_ETL_EPIC_NOTIFICATIONS = "1" # disabled when suppression <> 0
        k8s_job_TREWS_ETL_SUPPRESSION = "2"
        k8s_job_TREWS_ALERT_SERVER_IP = "jit-alerts.default.svc.cluster.local"
      }
    }
}

resource "aws_lambda_function" "prod_etl_lambda_JHH" {
    function_name    = "${var.deploy_prefix}-prod-etl-lambda-JHH"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_klaunch_lambda_package}"
    role             = "${var.aws_klaunch_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300
    environment {
      variables {
        PYKUBE_KUBERNETES_SERVICE_HOST = "${var.k8s_prod_server_host}"
        PYKUBE_KUBERNETES_SERVICE_PORT = "${var.k8s_prod_server_port}"
        kube_job_name  = "epic2op-jhh-prod"
        kube_nodegroup = "etl"
        kube_cpu_requests = "500m"
        kube_mem_requests = "1Gi"
        kube_name      = "${var.k8s_prod_name}"
        kube_server    = "${var.k8s_prod_server}"
        kube_cert_auth = "${var.k8s_prod_cert_auth}"
        kube_user      = "${var.k8s_prod_user}"
        kube_pass      = "${var.k8s_prod_pass}"
        kube_image     = "${var.k8s_prod_image}"
        kube_active_deadline_seconds = "300"
        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /etl/epic2op/engine.py --hospital=JHH"
        k8s_job_db_host     = "${var.prod_db_host}"
        k8s_job_db_port     = "${var.prod_db_port}"
        k8s_job_db_name     = "${var.prod_db_name}"
        k8s_job_db_user     = "${var.prod_db_username}"
        k8s_job_db_password = "${var.prod_db_password}"
        k8s_job_jhapi_client_id     = "${var.prod_jhapi_client_id}"
        k8s_job_jhapi_client_secret = "${var.prod_jhapi_client_secret}"
        k8s_job_etl_channel         = "${var.prod_etl_channel}"
        k8s_job_TREWS_ETL_SERVER             = "${var.PROD_ETL_SERVER}"
        k8s_job_TREWS_ETL_HOSPITAL           = "JHH"
        k8s_job_TREWS_ETL_HOURS              = "${var.PROD_ETL_HOURS}"
        k8s_job_TREWS_ETL_ARCHIVE            = "${var.PROD_ETL_ARCHIVE}"
        k8s_job_TREWS_ETL_MODE               = "${var.PROD_ETL_MODE}"
        k8s_job_TREWS_ETL_STREAM_HOURS       = "${var.PROD_ETL_STREAM_HOURS}"
        k8s_job_TREWS_ETL_STREAM_SLICES      = "${var.PROD_ETL_STREAM_SLICES}"
        k8s_job_TREWS_ETL_STREAM_SLEEP_SECS  = "${var.PROD_ETL_STREAM_SLEEP_SECS}"
        k8s_job_TREWS_ETL_EPIC_NOTIFICATIONS = "0"
        k8s_job_TREWS_ETL_SUPPRESSION = "2"
        k8s_job_TREWS_ALERT_SERVER_IP = "jit-alerts.default.svc.cluster.local"
      }
    }
}

resource "aws_lambda_function" "prod_etl_lambda_BMC" {
    function_name    = "${var.deploy_prefix}-prod-etl-lambda-BMC"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_klaunch_lambda_package}"
    role             = "${var.aws_klaunch_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300
    environment {
      variables {
        PYKUBE_KUBERNETES_SERVICE_HOST = "${var.k8s_prod_server_host}"
        PYKUBE_KUBERNETES_SERVICE_PORT = "${var.k8s_prod_server_port}"
        kube_job_name  = "epic2op-bmc-prod"
        kube_nodegroup = "etl"
        kube_cpu_requests = "250m"
        kube_mem_requests = "1Gi"
        kube_name      = "${var.k8s_prod_name}"
        kube_server    = "${var.k8s_prod_server}"
        kube_cert_auth = "${var.k8s_prod_cert_auth}"
        kube_user      = "${var.k8s_prod_user}"
        kube_pass      = "${var.k8s_prod_pass}"
        kube_image     = "${var.k8s_prod_image}"
        kube_active_deadline_seconds = "300"
        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /etl/epic2op/engine.py --hospital=BMC"
        k8s_job_db_host     = "${var.prod_db_host}"
        k8s_job_db_port     = "${var.prod_db_port}"
        k8s_job_db_name     = "${var.prod_db_name}"
        k8s_job_db_user     = "${var.prod_db_username}"
        k8s_job_db_password = "${var.prod_db_password}"
        k8s_job_jhapi_client_id     = "${var.prod_jhapi_client_id}"
        k8s_job_jhapi_client_secret = "${var.prod_jhapi_client_secret}"
        k8s_job_etl_channel         = "${var.prod_etl_channel}"
        k8s_job_TREWS_ETL_SERVER             = "${var.PROD_ETL_SERVER}"
        k8s_job_TREWS_ETL_HOSPITAL           = "BMC"
        k8s_job_TREWS_ETL_HOURS              = "${var.PROD_ETL_HOURS}"
        k8s_job_TREWS_ETL_ARCHIVE            = "${var.PROD_ETL_ARCHIVE}"
        k8s_job_TREWS_ETL_MODE               = "${var.PROD_ETL_MODE}"
        k8s_job_TREWS_ETL_STREAM_HOURS       = "${var.PROD_ETL_STREAM_HOURS}"
        k8s_job_TREWS_ETL_STREAM_SLICES      = "${var.PROD_ETL_STREAM_SLICES}"
        k8s_job_TREWS_ETL_STREAM_SLEEP_SECS  = "${var.PROD_ETL_STREAM_SLEEP_SECS}"
        k8s_job_TREWS_ETL_EPIC_NOTIFICATIONS = "0"
        k8s_job_TREWS_ETL_SUPPRESSION = "2"
        k8s_job_TREWS_ALERT_SERVER_IP = "jit-alerts.default.svc.cluster.local"
      }
    }
}

resource "aws_cloudwatch_event_rule" "etl_schedule_rule" {
    name = "${var.deploy_prefix}-etl-schedule-rule"
    description = "Fires every ${var.prod_etl_lambda_firing_rate_mins} minutes"
    schedule_expression = "rate(${var.prod_etl_lambda_firing_rate_mins} minutes)"
}

resource "aws_cloudwatch_event_target" "etl_schedule_target_HCGH" {
    rule      = "${aws_cloudwatch_event_rule.etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_prod_etl_lambda_HCGH"
    arn       = "${aws_lambda_function.prod_etl_lambda_HCGH.arn}"
}

resource "aws_cloudwatch_event_target" "etl_schedule_target_JHH" {
    rule      = "${aws_cloudwatch_event_rule.etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_prod_etl_lambda_JHH"
    arn       = "${aws_lambda_function.prod_etl_lambda_JHH.arn}"
}

resource "aws_cloudwatch_event_target" "etl_schedule_target_BMC" {
    rule      = "${aws_cloudwatch_event_rule.etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_prod_etl_lambda_BMC"
    arn       = "${aws_lambda_function.prod_etl_lambda_BMC.arn}"
}

resource "aws_lambda_permission" "etl_cloudwatch_permissions_HCGH" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.prod_etl_lambda_HCGH.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.etl_schedule_rule.arn}"
}

resource "aws_lambda_permission" "etl_cloudwatch_permissions_JHH" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.prod_etl_lambda_JHH.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.etl_schedule_rule.arn}"
}

resource "aws_lambda_permission" "etl_cloudwatch_permissions_BMC" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.prod_etl_lambda_BMC.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.etl_schedule_rule.arn}"
}

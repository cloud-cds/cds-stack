############################
# Epic2Op ETL via AWS Lambda

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_klaunch_lambda_package" {}
variable "aws_klaunch_lambda_role_arn" {}

variable "k8s_dev_server_host" {}
variable "k8s_dev_server_port" {}

variable "k8s_dev_name" {}
variable "k8s_dev_server" {}
variable "k8s_dev_user" {}
variable "k8s_dev_pass" {}
variable "k8s_dev_cert_auth" {}
variable "k8s_dev_cert" {}
variable "k8s_dev_key" {}
variable "k8s_dev_image" {}
variable "k8s_dev_image_dev" {}

variable "dev_db_host" {}
variable "dev_db_port" { default = 5432 }
variable "dev_db_name" {}
variable "dev_db_username" {}
variable "dev_db_password" {}

variable "dev_jhapi_client_id" {}
variable "dev_jhapi_client_secret" {}
variable "dev_etl_channel" {}

variable "dev_etl_lambda_firing_rate_mins" {}

variable "DEV_ETL_SERVER" {}
variable "DEV_ETL_HOSPITAL" {}
variable "DEV_ETL_HOURS" {}
variable "DEV_ETL_ARCHIVE" {}
variable "DEV_ETL_MODE" {}
variable "DEV_ETL_DEMO_MODE" {}
variable "DEV_ETL_STREAM_HOURS" {}
variable "DEV_ETL_STREAM_SLICES" {}
variable "DEV_ETL_STREAM_SLEEP_SECS" {}
variable "DEV_ETL_EPIC_NOTIFICATIONS" {}
variable "local_shell" {}


# A Lambda function for periodic ETL.
resource "aws_lambda_function" "test_etl_lambda" {
    function_name    = "${var.deploy_prefix}-test-etl-lambda"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_mc_lambda}"
    s3_key           = "${var.aws_klaunch_lambda_package}"
    role             = "${var.aws_klaunch_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300
    environment {
      variables {
        PYKUBE_KUBERNETES_SERVICE_HOST = "${var.k8s_dev_server_host}"
        PYKUBE_KUBERNETES_SERVICE_PORT = "${var.k8s_dev_server_port}"
        kube_job_name  = "epic2op-test"
        # kube_nodegroup = "spot-nodes"
        kube_cpu_requests = "250m"
        kube_mem_requests = "1Gi"
        kube_name      = "${var.k8s_dev_name}"
        kube_server    = "${var.k8s_dev_server}"
        kube_cert_auth = "${var.k8s_dev_cert_auth}"
        kube_user      = "${var.k8s_dev_user}"
        kube_pass      = "${var.k8s_dev_pass}"
        kube_image     = "${var.k8s_dev_image}"
        kube_active_deadline_seconds = "300"
        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /etl/epic2op/engine_jh_epic.py"
        k8s_job_db_host     = "${var.dev_db_host}"
        k8s_job_db_port     = "${var.dev_db_port}"
        k8s_job_db_name     = "metabolic_compass"
        k8s_job_db_user     = "${var.dev_db_username}"
        k8s_job_db_password = "${var.dev_db_password}"
        k8s_job_jhapi_client_id     = "${var.dev_jhapi_client_id}"
        k8s_job_jhapi_client_secret = "${var.dev_jhapi_client_secret}"
        k8s_job_etl_channel         = "${var.dev_etl_channel}"
        k8s_job_ETL_SERVER             = "epic-test"
        k8s_job_ETL_HOURS              = "${var.DEV_ETL_HOURS}"
        k8s_job_ETL_ARCHIVE            = "${var.DEV_ETL_ARCHIVE}"
        k8s_job_ETL_MODE               = "${var.DEV_ETL_MODE}"
        k8s_job_ETL_STREAM_HOURS       = "${var.DEV_ETL_STREAM_HOURS}"
        k8s_job_ETL_STREAM_SLICES      = "${var.DEV_ETL_STREAM_SLICES}"
        k8s_job_ETL_STREAM_SLEEP_SECS  = "${var.DEV_ETL_STREAM_SLEEP_SECS}"
        k8s_job_ETL_EPIC_NOTIFICATIONS = "${var.DEV_ETL_EPIC_NOTIFICATIONS}" # disabled when suppression <> 0
        k8s_job_ETL_SUPPRESSION = "2"
        k8s_job_ALERT_SERVER_IP = "push-alerts-tst.default.svc.cluster.local"
        k8s_job_ETL_WORKSPACE           = "workspace"
        k8s_job_ETL_DEPT_ID             = "110300100"
        k8s_job_ETL_DEPT_ID_TYPE        = "EXTERNAL"
        k8s_job_ETL_USER_ID             = "EDIHDAI"
        k8s_job_ETL_USER_ID_TYPE        = "EXTERNAL"
        k8s_job_ETL_SYSTEMLIST_ID       = "5956"
        k8s_job_ETL_SYSTEMLIST_ID_TYPE  = "EXTERNAL"
      }
    }
}

resource "aws_lambda_function" "dev_etl_lambda_HCGH" {

    function_name    = "${var.deploy_prefix}-dev-etl-lambda-HCGH"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_klaunch_lambda_package}"
    role             = "${var.aws_klaunch_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300

    environment {
      variables {
        PYKUBE_KUBERNETES_SERVICE_HOST = "${var.k8s_dev_server_host}"
        PYKUBE_KUBERNETES_SERVICE_PORT = "${var.k8s_dev_server_port}"

        kube_job_name  = "epic2op-hcgh-dev"
        kube_nodegroup = "spot-nodes"
        kube_cpu_requests = "250m"
        kube_mem_requests = "1Gi"
        kube_name      = "${var.k8s_dev_name}"
        kube_server    = "${var.k8s_dev_server}"
        kube_cert_auth = "${var.k8s_dev_cert_auth}"
        kube_user      = "${var.k8s_dev_user}"
        kube_pass      = "${var.k8s_dev_pass}"
        kube_image     = "${var.k8s_dev_image_dev}"
        kube_active_deadline_seconds = "300"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /etl/epic2op/engine_pats_only.py --hospital=HCGH"

        k8s_job_db_host     = "${var.dev_db_host}"
        k8s_job_db_port     = "${var.dev_db_port}"
        k8s_job_db_name     = "${var.dev_db_name}"
        k8s_job_db_user     = "${var.dev_db_username}"
        k8s_job_db_password = "${var.dev_db_password}"

        k8s_job_jhapi_client_id     = "${var.dev_jhapi_client_id}"
        k8s_job_jhapi_client_secret = "${var.dev_jhapi_client_secret}"
        k8s_job_etl_channel         = "${var.dev_etl_channel}"

        k8s_job_TREWS_ETL_SERVER             = "${var.DEV_ETL_SERVER}"
        k8s_job_TREWS_ETL_HOSPITAL           = "HCGH"
        k8s_job_TREWS_ETL_HOURS              = "${var.DEV_ETL_HOURS}"
        k8s_job_TREWS_ETL_ARCHIVE            = "${var.DEV_ETL_ARCHIVE}"
        k8s_job_TREWS_ETL_MODE               = "${var.DEV_ETL_MODE}"
        k8s_job_TREWS_ETL_STREAM_HOURS       = "${var.DEV_ETL_STREAM_HOURS}"
        k8s_job_TREWS_ETL_STREAM_SLICES      = "${var.DEV_ETL_STREAM_SLICES}"
        k8s_job_TREWS_ETL_STREAM_SLEEP_SECS  = "${var.DEV_ETL_STREAM_SLEEP_SECS}"
        k8s_job_TREWS_ETL_EPIC_NOTIFICATIONS = "${var.DEV_ETL_EPIC_NOTIFICATIONS}" # disabled when suppression <> 0
        k8s_job_TREWS_ETL_SUPPRESSION = "2"
        k8s_job_TREWS_ALERT_SERVER_IP = "push-alerts-dev.default.svc.cluster.local"
        k8s_job_TREWS_ETL_WORKSPACE = "workspace"
      }
    }
}

resource "aws_lambda_function" "dev_etl_lambda_JHH" {

    function_name    = "${var.deploy_prefix}-dev-etl-lambda-JHH"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_klaunch_lambda_package}"
    role             = "${var.aws_klaunch_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300

    environment {
      variables {
        PYKUBE_KUBERNETES_SERVICE_HOST = "${var.k8s_dev_server_host}"
        PYKUBE_KUBERNETES_SERVICE_PORT = "${var.k8s_dev_server_port}"

        kube_job_name  = "epic2op-jhh-dev"
        kube_nodegroup = "spot-nodes"
        kube_cpu_requests = "500m"
        kube_mem_requests = "1Gi"
        kube_name      = "${var.k8s_dev_name}"
        kube_server    = "${var.k8s_dev_server}"
        kube_cert_auth = "${var.k8s_dev_cert_auth}"
        kube_user      = "${var.k8s_dev_user}"
        kube_pass      = "${var.k8s_dev_pass}"
        kube_image     = "${var.k8s_dev_image_dev}"
        kube_active_deadline_seconds = "300"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /etl/epic2op/engine_pats_only.py --hospital=JHH"

        k8s_job_db_host     = "${var.dev_db_host}"
        k8s_job_db_port     = "${var.dev_db_port}"
        k8s_job_db_name     = "${var.dev_db_name}"
        k8s_job_db_user     = "${var.dev_db_username}"
        k8s_job_db_password = "${var.dev_db_password}"

        k8s_job_jhapi_client_id     = "${var.dev_jhapi_client_id}"
        k8s_job_jhapi_client_secret = "${var.dev_jhapi_client_secret}"
        k8s_job_etl_channel         = "${var.dev_etl_channel}"

        k8s_job_TREWS_ETL_SERVER             = "${var.DEV_ETL_SERVER}"
        k8s_job_TREWS_ETL_HOSPITAL           = "JHH"
        k8s_job_TREWS_ETL_HOURS              = "${var.DEV_ETL_HOURS}"
        k8s_job_TREWS_ETL_ARCHIVE            = "${var.DEV_ETL_ARCHIVE}"
        k8s_job_TREWS_ETL_MODE               = "${var.DEV_ETL_MODE}"
        k8s_job_TREWS_ETL_STREAM_HOURS       = "${var.DEV_ETL_STREAM_HOURS}"
        k8s_job_TREWS_ETL_STREAM_SLICES      = "${var.DEV_ETL_STREAM_SLICES}"
        k8s_job_TREWS_ETL_STREAM_SLEEP_SECS  = "${var.DEV_ETL_STREAM_SLEEP_SECS}"
        k8s_job_TREWS_ETL_EPIC_NOTIFICATIONS = "${var.DEV_ETL_EPIC_NOTIFICATIONS}"
        k8s_job_TREWS_ETL_SUPPRESSION = "2"
        k8s_job_TREWS_ALERT_SERVER_IP = "push-alerts-dev.default.svc.cluster.local"
        k8s_job_TREWS_ETL_WORKSPACE = "workspace"
      }
    }
}

resource "aws_lambda_function" "dev_etl_lambda_BMC" {

    function_name    = "${var.deploy_prefix}-dev-etl-lambda-BMC"
    handler          = "service.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_klaunch_lambda_package}"
    role             = "${var.aws_klaunch_lambda_role_arn}"
    runtime          = "python2.7"
    timeout          = 300

    environment {
      variables {
        PYKUBE_KUBERNETES_SERVICE_HOST = "${var.k8s_dev_server_host}"
        PYKUBE_KUBERNETES_SERVICE_PORT = "${var.k8s_dev_server_port}"

        kube_job_name  = "epic2op-bmc-dev"
        kube_nodegroup = "spot-nodes"
        kube_cpu_requests = "250m"
        kube_mem_requests = "1Gi"
        kube_name      = "${var.k8s_dev_name}"
        kube_server    = "${var.k8s_dev_server}"
        kube_cert_auth = "${var.k8s_dev_cert_auth}"
        kube_user      = "${var.k8s_dev_user}"
        kube_pass      = "${var.k8s_dev_pass}"
        kube_image     = "${var.k8s_dev_image_dev}"
        kube_active_deadline_seconds = "300"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /etl/epic2op/engine_pats_only.py --hospital=BMC"

        k8s_job_db_host     = "${var.dev_db_host}"
        k8s_job_db_port     = "${var.dev_db_port}"
        k8s_job_db_name     = "${var.dev_db_name}"
        k8s_job_db_user     = "${var.dev_db_username}"
        k8s_job_db_password = "${var.dev_db_password}"

        k8s_job_jhapi_client_id     = "${var.dev_jhapi_client_id}"
        k8s_job_jhapi_client_secret = "${var.dev_jhapi_client_secret}"
        k8s_job_etl_channel         = "${var.dev_etl_channel}"

        k8s_job_TREWS_ETL_SERVER             = "${var.DEV_ETL_SERVER}"
        k8s_job_TREWS_ETL_HOSPITAL           = "BMC"
        k8s_job_TREWS_ETL_HOURS              = "${var.DEV_ETL_HOURS}"
        k8s_job_TREWS_ETL_ARCHIVE            = "${var.DEV_ETL_ARCHIVE}"
        k8s_job_TREWS_ETL_MODE               = "${var.DEV_ETL_MODE}"
        k8s_job_TREWS_ETL_STREAM_HOURS       = "${var.DEV_ETL_STREAM_HOURS}"
        k8s_job_TREWS_ETL_STREAM_SLICES      = "${var.DEV_ETL_STREAM_SLICES}"
        k8s_job_TREWS_ETL_STREAM_SLEEP_SECS  = "${var.DEV_ETL_STREAM_SLEEP_SECS}"
        k8s_job_TREWS_ETL_EPIC_NOTIFICATIONS = "${var.DEV_ETL_EPIC_NOTIFICATIONS}"
        k8s_job_TREWS_ETL_SUPPRESSION = "2"
        k8s_job_TREWS_ALERT_SERVER_IP = "push-alerts-dev.default.svc.cluster.local"
        k8s_job_TREWS_ETL_WORKSPACE = "workspace"
      }
    }
}

resource "aws_cloudwatch_event_rule" "dev_etl_schedule_rule" {
    name = "${var.deploy_prefix}-dev-etl-schedule-rule"
    description = "Fires every ${var.dev_etl_lambda_firing_rate_mins} minutes"
    schedule_expression = "rate(${var.dev_etl_lambda_firing_rate_mins} minutes)"
}

resource "aws_cloudwatch_event_rule" "test_etl_schedule_rule" {
    name = "${var.deploy_prefix}-test-etl-schedule-rule"
    description = "Fires every ${var.dev_etl_lambda_firing_rate_mins} minutes"
    schedule_expression = "rate(${var.dev_etl_lambda_firing_rate_mins} minutes)"
}

resource "aws_cloudwatch_event_target" "test_etl_schedule_target" {
    rule      = "${aws_cloudwatch_event_rule.test_etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_etl_lambda"
    arn       = "${aws_lambda_function.test_etl_lambda_HCGH.arn}"
}

resource "aws_cloudwatch_event_target" "dev_etl_schedule_target_HCGH" {
    rule      = "${aws_cloudwatch_event_rule.dev_etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_etl_lambda_HCGH"
    arn       = "${aws_lambda_function.dev_etl_lambda_HCGH.arn}"
}

resource "aws_cloudwatch_event_target" "dev_etl_schedule_target_JHH" {
    rule      = "${aws_cloudwatch_event_rule.dev_etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_etl_lambda_JHH"
    arn       = "${aws_lambda_function.dev_etl_lambda_JHH.arn}"
}

resource "aws_cloudwatch_event_target" "dev_etl_schedule_target_BMC" {
    rule      = "${aws_cloudwatch_event_rule.dev_etl_schedule_rule.name}"
    target_id = "${replace(var.deploy_prefix, "-", "_")}_etl_lambda_BMC"
    arn       = "${aws_lambda_function.dev_etl_lambda_BMC.arn}"
}

resource "aws_lambda_permission" "test_etl_cloudwatch_permissions" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.test_etl_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.test_etl_schedule_rule.arn}"
}

resource "aws_lambda_permission" "dev_etl_cloudwatch_permissions_HCGH" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.dev_etl_lambda_HCGH.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.dev_etl_schedule_rule.arn}"
}

resource "aws_lambda_permission" "dev_etl_cloudwatch_permissions_JHH" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.dev_etl_lambda_JHH.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.dev_etl_schedule_rule.arn}"
}

resource "aws_lambda_permission" "etl_cloudwatch_permissions_BMC" {
    statement_id  = "ETLPeriodicExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.dev_etl_lambda_BMC.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.dev_etl_schedule_rule.arn}"
}

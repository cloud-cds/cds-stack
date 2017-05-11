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

variable "db_host" {}
variable "db_port" { default = 5432 }
variable "db_name" {}
variable "db_username" {}
variable "db_password" {}

variable "analysis_publishing_timeseries_firing_rate_min" {}
variable "analysis_publishing_reports_firing_rate_min" {}
variable "analysis_publishing_reports_firing_rate_expr" {}
variable "k8s_analysis_publishing_image" {}


#-------------------------------------------
## Metrics (timeseries) ETL
#-------------------------------------------
resource "aws_lambda_function" "analysis_publishing_timeseries_lambda" {

    function_name    = "${var.deploy_prefix}_analysis_publishing_timeseries_lambda"
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

        kube_job_name  = "behavior-timeseries-prod"
        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        kube_image     = "${var.k8s_analysis_publishing_image}"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /dashan-etl/etl/analysis_publishing/engine.py metrics ${var.analysis_publishing_timeseries_firing_rate_min}"

        # ETL Environment Variables
        k8s_job_BEHAMON_STACK                      = "${var.deploy_prefix}"
        k8s_job_db_host                            = "${var.db_host}"
        k8s_job_db_port                            = "${var.db_port}"
        k8s_job_db_name                            = "${var.db_name}"
        k8s_job_db_user                            = "${var.db_username}"
        k8s_job_db_password                        = "${var.db_password}"
      }
    }
}


resource "aws_cloudwatch_event_rule" "analysis_publishing_timeseries_lambda_schedule_rule" {
    name = "${var.deploy_prefix}-analysis_publishing_timeseries_schedule_rule"
    description = "Fires every ${var.analysis_publishing_timeseries_firing_rate_min} minutes"
    schedule_expression = "rate(${var.analysis_publishing_timeseries_firing_rate_min} minutes)"
}

resource "aws_cloudwatch_event_target" "analysis_publishing_timeseries_lambda_schedule_target" {
    rule      = "${aws_cloudwatch_event_rule.analysis_publishing_timeseries_lambda_schedule_rule.name}"
    target_id = "${var.deploy_prefix}_analysis_publishing_timeseries_lambda"
    arn       = "${aws_lambda_function.analysis_publishing_timeseries_lambda.arn}"
}

resource "aws_lambda_permission" "analysis_publishing_timeseries_cloudwatch_permissions" {
    statement_id  = "AnalysisTimeseriesSchedule"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.analysis_publishing_timeseries_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.analysis_publishing_timeseries_lambda_schedule_rule.arn}"
}


#-------------------------------------------
## Reports (email) ETL
#-------------------------------------------
resource "aws_lambda_function" "analysis_publishing_reports_lambda" {

    function_name    = "${var.deploy_prefix}_analysis_publishing_reports_lambda"
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

        kube_job_name  = "behavior-reports-prod"
        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        kube_image     = "${var.k8s_analysis_publishing_image}"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /dashan-etl/etl/analysis_publishing/engine.py reports ${var.analysis_publishing_reports_firing_rate_min}"

        # ETL Environment Variables
        k8s_job_BEHAMON_STACK                      = "${var.deploy_prefix}"
        k8s_job_REPORT_RECEIVING_EMAIL_ADDRESS     = "peterm@opsdx.io"  #trews-jhu@opsdx.io, peterm@opsdx.io
        k8s_job_db_host                            = "${var.db_host}"
        k8s_job_db_port                            = "${var.db_port}"
        k8s_job_db_name                            = "${var.db_name}"
        k8s_job_db_user                            = "${var.db_username}"
        k8s_job_db_password                        = "${var.db_password}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "analysis_publishing_reports_lambda_schedule_rule" {
    name = "${var.deploy_prefix}-analysis_publishing_reports_schedule_rule"
    description = "Fires every ${var.analysis_publishing_reports_firing_rate_min} minutes"
    schedule_expression = "rate(${var.analysis_publishing_reports_firing_rate_expr})"
}

resource "aws_cloudwatch_event_target" "analysis_publishing_reports_lambda_schedule_rule_target" {
    rule      = "${aws_cloudwatch_event_rule.analysis_publishing_reports_lambda_schedule_rule.name}"
    target_id = "${var.deploy_prefix}_analysis_publishing_reports_lambda"
    arn       = "${aws_lambda_function.analysis_publishing_reports_lambda.arn}"
}

resource "aws_lambda_permission" "analysis_publishing_reports_cloudwatch_permissions" {
    statement_id  = "AnalysisReportsSchedule"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.analysis_publishing_reports_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.analysis_publishing_reports_lambda_schedule_rule.arn}"
}

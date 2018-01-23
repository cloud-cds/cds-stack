variable "aws_region" {}

variable "deploy_prefix" {}

variable "s3_opsdx_lambda" {}
variable "aws_behamon_lambda_package"  {}
variable "aws_behamon_lambda_role_arn" {}
variable "aws_klaunch_lambda_package"  {}
variable "aws_klaunch_lambda_role_arn" {}

variable "behamon_log_group_name"     {}
variable "behamon_log_group_arn"      {}

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

variable "scorecard_report_firing_rate_min" {}
variable "scorecard_report_firing_rate_expr" {}
variable "k8s_scorecard_report_image" {}

variable "scorecard_metric_firing_rate_min" {}
variable "scorecard_metric_firing_rate_expr" {}
variable "k8s_scorecard_metric_image" {}

variable "s3_weekly_report_firing_rate_min" {}
variable "s3_weekly_report_firing_rate_expr" {}

data "aws_subnet" "lambda_subnet1" {
  id = "${var.lambda_subnet1_id}"
}

data "aws_subnet" "lambda_subnet2" {
  id = "${var.lambda_subnet2_id}"
}

data "aws_security_group" "lambda_sg" {
  id = "${var.lambda_sg_id}"
}


##
# Behavior Monitor Log Extractor
#

resource "aws_lambda_function" "behamon_log_extractor" {

    function_name    = "${var.deploy_prefix}_dev_behamon_log_extractor"
    handler          = "log_extractor.handler"
    s3_bucket        = "${var.s3_opsdx_lambda}"
    s3_key           = "${var.aws_behamon_lambda_package}"
    role             = "${var.aws_behamon_lambda_role_arn}"
    runtime          = "python3.6"
    timeout          = 300

    vpc_config {
      subnet_ids         = ["${data.aws_subnet.lambda_subnet1.id}", "${data.aws_subnet.lambda_subnet2.id}"]
      security_group_ids = ["${data.aws_security_group.lambda_sg.id}"]
    }

    environment {
      variables {
        db_host     = "${var.db_host}"
        db_port     = "${var.db_port}"
        db_name     = "${var.db_name}"
        db_user     = "${var.db_username}"
        db_password = "${var.db_password}"
      }
    }
}

resource "aws_lambda_permission" "behamon_log_extractor_permissions" {
    statement_id  = "LogBasedExecution"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.behamon_log_extractor.function_name}"
    principal     = "logs.${var.aws_region}.amazonaws.com"
    source_arn    = "${var.behamon_log_group_arn}"
}

resource "aws_cloudwatch_log_subscription_filter" "behamon_log_extractor_filter" {
  depends_on      = ["aws_lambda_permission.behamon_log_extractor_permissions"]
  name            = "${var.deploy_prefix}_dev_behamon_log_extractor_filter"
  log_group_name  = "${var.behamon_log_group_name}"
  filter_pattern  = "{ ( ($.resp.body.q != \"null\" && $.resp.body.s != \"null\" && $.resp.body.u != \"PINGUSER\" && $.resp.body.u != \"LOADTESTUSER\") || $.resp.body.session-close != \"null\") || ($.resp.url = \"*PATID*\" && $.resp.url = \"*TSESSID*\" && $.resp.url != \"*PINGUSER*\" && $.resp.url != \"*LOADTESTUSER*\") }"
  destination_arn = "${aws_lambda_function.behamon_log_extractor.arn}"
}


#########################################
# Reports / scorecard job runner.
#
resource "aws_lambda_function" "scorecard_report_lambda" {

    function_name    = "${var.deploy_prefix}_dev_scorecard_report_lambda"
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

        kube_job_name  = "behavior-reports-dev"
        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        kube_image     = "${var.k8s_scorecard_report_image}"
        kube_active_deadline_seconds = "300"
        kube_nodegroup = "spot-nodes"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        #kube_cmd_2 = "/usr/local/bin/python3 /etl/analysis_publishing/engine.py reports ${var.scorecard_report_firing_rate_min}"
        kube_cmd_2 = "/usr/local/bin/python3 /etl/analysis_publishing/engine.py reports 360"

        # ETL Environment Variables
        k8s_job_BEHAMON_STACK                      = "${var.deploy_prefix}-dev"
        k8s_job_REPORT_RECEIVING_EMAIL_ADDRESS     = "trews-jhu@opsdx.io"
        k8s_job_db_host                            = "${var.db_host}"
        k8s_job_db_port                            = "${var.db_port}"
        k8s_job_db_name                            = "${var.db_name}"
        k8s_job_db_user                            = "${var.db_username}"
        k8s_job_db_password                        = "${var.db_password}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "scorecard_report_lambda_schedule_rule" {
    name = "${var.deploy_prefix}_dev_scorecard_report_schedule_rule"
    description = "Fires every ${var.scorecard_report_firing_rate_min} minutes"
    schedule_expression = "rate(${var.scorecard_report_firing_rate_expr})"
}

resource "aws_cloudwatch_event_target" "scorecard_report_lambda_schedule_rule_target" {
    rule      = "${aws_cloudwatch_event_rule.scorecard_report_lambda_schedule_rule.name}"
    target_id = "${var.deploy_prefix}_dev_scorecard_report_lambda"
    arn       = "${aws_lambda_function.scorecard_report_lambda.arn}"
}

resource "aws_lambda_permission" "scorecard_report_cloudwatch_permissions" {
    statement_id  = "ScorecardReportSchedule"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.scorecard_report_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.scorecard_report_lambda_schedule_rule.arn}"
}

resource "aws_lambda_function" "scorecard_metric_lambda" {

    function_name    = "${var.deploy_prefix}_dev_scorecard_metric_lambda"
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

        kube_job_name  = "behavior-metrics-dev"
        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        kube_image     = "${var.k8s_scorecard_metric_image}"
        kube_active_deadline_seconds = "300"
        kube_nodegroup = "spot-nodes"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        kube_cmd_2 = "/usr/local/bin/python3 /etl/analysis_publishing/engine.py metrics 2880"

        # ETL Environment Variables
        k8s_job_BEHAMON_STACK                      = "${var.deploy_prefix}-dev"
        k8s_job_REPORT_RECEIVING_EMAIL_ADDRESS     = "trews-jhu@opsdx.io"
        k8s_job_db_host                            = "${var.db_host}"
        k8s_job_db_port                            = "${var.db_port}"
        k8s_job_db_name                            = "${var.db_name}"
        k8s_job_db_user                            = "${var.db_username}"
        k8s_job_db_password                        = "${var.db_password}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "scorecard_metric_lambda_schedule_rule" {
    name = "${var.deploy_prefix}_dev_scorecard_metric_schedule_rule"
    description = "Fires every ${var.scorecard_metric_firing_rate_min} minutes"
    schedule_expression = "rate(${var.scorecard_metric_firing_rate_expr})"
}

resource "aws_cloudwatch_event_target" "scorecard_metric_lambda_schedule_rule_target" {
    rule      = "${aws_cloudwatch_event_rule.scorecard_metric_lambda_schedule_rule.name}"
    target_id = "${var.deploy_prefix}_dev_scorecard_metric_lambda"
    arn       = "${aws_lambda_function.scorecard_metric_lambda.arn}"
}

resource "aws_lambda_permission" "scorecard_metric_cloudwatch_permissions" {
    statement_id  = "ScorecardReportSchedule"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.scorecard_metric_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.scorecard_metric_lambda_schedule_rule.arn}"
}

resource "aws_lambda_function" "s3_weekly_report_lambda" {

    function_name    = "${var.deploy_prefix}_dev_s3_weekly_report_lambda"
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

        kube_job_name  = "patient-report-dev"
        kube_name      = "${var.k8s_name}"
        kube_server    = "${var.k8s_server}"
        kube_cert_auth = "${var.k8s_cert_auth}"
        kube_user      = "${var.k8s_user}"
        kube_pass      = "${var.k8s_pass}"
        kube_image     = "${var.k8s_scorecard_metric_image}"
        kube_active_deadline_seconds = "300"
        kube_nodegroup = "spot-nodes"

        kube_cmd_0 = "sh"
        kube_cmd_1 = "-c"
        # kube_cmd_2 = "service rsyslog start && sleep 5 && ./bin/goofys jh-opsdx-report /mnt && sleep 5 && /usr/local/bin/python3 /etl/analysis_publishing/engine.py weekly-report 0"
        kube_cmd_2 = "sleep 100000"
        # ETL Environment Variables
        k8s_job_BEHAMON_STACK                      = "${var.deploy_prefix}-dev"
        k8s_job_REPORT_RECEIVING_EMAIL_ADDRESS     = "trews-jhu@opsdx.io"
        k8s_job_db_host                            = "${var.db_host}"
        k8s_job_db_port                            = "${var.db_port}"
        k8s_job_db_name                            = "${var.db_name}"
        k8s_job_db_user                            = "${var.db_username}"
        k8s_job_db_password                        = "${var.db_password}"
      }
    }
}

resource "aws_cloudwatch_event_rule" "s3_weekly_report_lambda_schedule_rule" {
    name = "${var.deploy_prefix}_dev_s3_weekly_report_schedule_rule"
    description = "Fires every ${var.s3_weekly_report_firing_rate_min} minutes"
    schedule_expression = "rate(${var.s3_weekly_report_firing_rate_expr})"
}

resource "aws_cloudwatch_event_target" "s3_weekly_report_lambda_schedule_rule_target" {
    rule      = "${aws_cloudwatch_event_rule.s3_weekly_report_lambda_schedule_rule.name}"
    target_id = "${var.deploy_prefix}_dev_s3_weekly_report_lambda"
    arn       = "${aws_lambda_function.s3_weekly_report_lambda.arn}"
}

resource "aws_lambda_permission" "s3_weekly_report_cloudwatch_permissions" {
    statement_id  = "S3WeeklyReportSchedule"
    action        = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.s3_weekly_report_lambda.function_name}"
    principal     = "events.amazonaws.com"
    source_arn    = "${aws_cloudwatch_event_rule.s3_weekly_report_lambda_schedule_rule.arn}"
}

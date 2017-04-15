# Specify the provider and access details
provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region     = "${var.aws_region}"
  profile    = "opsdx"
}

data "aws_route53_zone" "main" {
  name = "${var.domain}."
}

module "deis" {
  source = "./services/deis"
  deploy_prefix = "${var.deploy_prefix}"
  local_shell = "${var.local_shell}"
}

module "web" {
  source        = "./services/web"
  deploy_name   = "${var.deploy_name}"
  deploy_stack  = "${var.deploy_stack}"
  deploy_prefix = "${var.deploy_prefix}"
  aws_id        = "${var.aws_id}"
  aws_region    = "${var.aws_region}"

  local_shell       = "${var.local_shell}"
  domain_zone_id    = "${data.aws_route53_zone.main.zone_id}"
  web_dns_name      = "api.${var.domain}"
  web_hostname_file = "services/web/hostname"
}

module "trews_etl" {
  source = "./services/trews_etl"

  deploy_prefix = "${var.deploy_prefix}"
  local_shell   = "${var.local_shell}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_klaunch_lambda_package = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  k8s_server_host = "${var.k8s_server_host}"
  k8s_server_port = "${var.k8s_server_port}"

  k8s_name      = "${var.k8s_name}"
  k8s_server    = "${var.k8s_server}"
  k8s_user      = "${var.k8s_user}"
  k8s_pass      = "${var.k8s_pass}"
  k8s_image     = "${var.k8s_epic2op_image}"
  k8s_cert_auth = "${var.k8s_cert_auth}"
  k8s_cert      = "${var.k8s_cert}"
  k8s_key       = "${var.k8s_key}"
  k8s_token     = "${var.k8s_token}"

  etl_lambda_firing_rate_mins = "15"

  db_host             = "db.${var.domain}"
  db_name             = "${replace(var.deploy_prefix, "-", "_")}"
  db_username         = "${var.db_username}"
  db_password         = "${var.db_password}"

  jhapi_client_id     = "${var.jhapi_client_id}"
  jhapi_client_secret = "${var.jhapi_client_secret}"
  etl_channel         = "on_opsdx_prod_etl"

  TREWS_ETL_SERVER             = "${var.TREWS_ETL_SERVER}"
  TREWS_ETL_HOSPITAL           = "${var.TREWS_ETL_HOSPITAL}"
  TREWS_ETL_HOURS              = "${var.TREWS_ETL_HOURS}"
  TREWS_ETL_ARCHIVE            = "${var.TREWS_ETL_ARCHIVE}"
  TREWS_ETL_MODE               = "${var.TREWS_ETL_MODE}"
  TREWS_ETL_DEMO_MODE          = "${var.TREWS_ETL_DEMO_MODE}"
  TREWS_ETL_STREAM_HOURS       = "${var.TREWS_ETL_STREAM_HOURS}"
  TREWS_ETL_STREAM_SLICES      = "${var.TREWS_ETL_STREAM_SLICES}"
  TREWS_ETL_STREAM_SLEEP_SECS  = "${var.TREWS_ETL_STREAM_SLEEP_SECS}"
  TREWS_ETL_EPIC_NOTIFICATIONS = "${var.TREWS_ETL_EPIC_NOTIFICATIONS}"
}

module "behavior_monitors" {
  source = "./services/behavior-monitors"
  aws_region = "${var.aws_region}"
  deploy_prefix = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_behamon_lambda_package  = "${var.aws_behamon_lambda_package}"
  aws_behamon_lambda_role_arn = "${var.aws_behamon_lambda_role_arn}"

  db_host     = "db.${var.domain}"
  db_name     = "${replace(var.deploy_prefix, "-", "_")}"
  db_username = "${var.db_username}"
  db_password = "${var.db_password}"

  lambda_subnet1_id = "${var.lambda_subnet1_id}"
  lambda_subnet2_id = "${var.lambda_subnet2_id}"
  lambda_sg_id      = "${var.lambda_sg_id}"

  behamon_log_group_name = "${var.behamon_log_group_name}"
  behamon_log_group_arn  = "${var.behamon_log_group_arn}"

  behamon_stack              = "Prod"
  behamon_web_filt_str       = "*USERID*"
  behamon_web_log_stream_str = "monitoring"

  # Must be manually kept in sync with the beahmon service,.py file
  behavior_monitors_timeseries_firing_rate_min = "10"
  behavior_monitors_reports_firing_rate_min = "10"
  behavior_monitors_reports_firing_rate_expr = "10 minutes"
}

module "op2dw_etl" {
  source = "./services/op2dw_etl"

  deploy_prefix = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_klaunch_lambda_package = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  k8s_server_host = "${var.k8s_server_host}"
  k8s_server_port = "${var.k8s_server_port}"

  k8s_name      = "${var.k8s_name}"
  k8s_server    = "${var.k8s_server}"
  k8s_user      = "${var.k8s_user}"
  k8s_pass      = "${var.k8s_pass}"
  k8s_image     = "${var.k8s_op2dw_image}"
  k8s_cert_auth = "${var.k8s_cert_auth}"
  k8s_cert      = "${var.k8s_cert}"
  k8s_key       = "${var.k8s_key}"
  k8s_token     = "${var.k8s_token}"

  op2dw_etl_lambda_firing_rate_mins = "60"
  op2dw_etl_remote_server = "opsdx_prod_srv"
  op2dw_dataset_id = "2"
  op2dw_model_id = "1"

  db_host      = "dw.${var.domain}"
  db_name      = "${replace(var.deploy_prefix, "-", "_")}_dw"
  db_username  = "${var.db_username}"
  db_password  = "${var.db_password}"
}

module "monitor" {
  source = "./services/monitor"
  deploy_prefix = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_alarm2slack_package = "${var.aws_alarm2slack_package}"
  alarm2slack_kms_key_arn = "${var.alarm2slack_kms_key_arn}"

  slack_hook     = "${var.slack_hook}"
  slack_channel  = "${var.slack_channel}"
  slack_watchers = "${var.slack_watchers}"
}

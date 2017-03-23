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

module "ebs" {
  source = "./services/ebs"
  local_shell = "${var.local_shell}"
}

module "zookeeper" {
  source = "./services/zookeeper"
  local_shell = "${var.local_shell}"
}

module "confluent" {
  source = "./services/confluent"
  local_shell = "${var.local_shell}"
}

#module "jupyter" {
#  source = "./services/jupyter"
#  local_shell = "${var.local_shell}"
#}

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
  aws_trews_etl_package = "${var.aws_trews_etl_package}"

  k8s_server_host = "${var.k8s_server_host}"
  k8s_server_port = "${var.k8s_server_port}"

  k8s_name      = "${var.k8s_name}"
  k8s_server    = "${var.k8s_server}"
  k8s_user      = "${var.k8s_user}"
  k8s_pass      = "${var.k8s_pass}"
  k8s_cert_auth = "${var.k8s_cert_auth}"
  k8s_cert      = "${var.k8s_cert}"
  k8s_key       = "${var.k8s_key}"
  k8s_token     = "${var.k8s_token}"
  k8s_image     = "${var.k8s_image}"

  etl_lambda_firing_rate_mins = "15"

  db_host             = "db.${var.domain}"
  db_name             = "${replace(var.deploy_prefix, "-", "_")}"
  db_username         = "${var.db_username}"
  db_password         = "${var.db_password}"
  jhapi_client_id     = "${var.jhapi_client_id}"
  jhapi_client_secret = "${var.jhapi_client_secret}"

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

module "trews_etl_replay" {
  source = "./services/trews_etl_replay"
  deploy_prefix = "${var.deploy_prefix}"

  etl_lambda_role_arn = "${module.trews_etl.etl_lambda_role_arn}"
  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_trews_etl_package = "${var.aws_trews_etl_package}"

  k8s_server_host = "${var.k8s_server_host}"
  k8s_server_port = "${var.k8s_server_port}"

  k8s_name      = "${var.k8s_name}"
  k8s_server    = "${var.k8s_server}"
  k8s_user      = "${var.k8s_user}"
  k8s_pass      = "${var.k8s_pass}"
  k8s_cert_auth = "${var.k8s_cert_auth}"
  k8s_cert      = "${var.k8s_cert}"
  k8s_key       = "${var.k8s_key}"
  k8s_token     = "${var.k8s_token}"
  k8s_image     = "${var.k8s_image}"

  db_host             = "db.${var.domain}"
  db_name             = "${replace(var.deploy_prefix, "-", "_")}"
  db_username         = "${var.db_username}"
  db_password         = "${var.db_password}"
  jhapi_client_id     = "${var.jhapi_client_id}"
  jhapi_client_secret = "${var.jhapi_client_secret}"

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

terraform {
  backend "s3" {
    encrypt = "true"
    bucket  = "mc-terraform"
    key     = "terraform_state/mc"
    region  = "us-east-1"
  }
}

provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region     = "${var.aws_region}"
  profile    = "mc"
}

module "stage1" {
  source = "./stage1"

  aws_region = "${var.aws_region}"

  deploy_name   = "${var.deploy_name}"
  deploy_stack  = "${var.deploy_stack}"
  deploy_prefix = "${var.deploy_prefix}"

  public_key_path   = "${var.public_key_path}"
  private_key_path  = "${var.private_key_path}"

  audit_sns_protocol = "${var.audit_sns_protocol}"
  audit_sns_endpoint = "${var.audit_sns_endpoint}"

  root_domain        = "${var.root_domain}"
  domain             = "${var.domain}"

  dev_db_username    = "${var.dev_db_username}"
  dev_db_password    = "${var.dev_db_password}"
  prod_db_username   = "${var.prod_db_username}"
  prod_db_password   = "${var.prod_db_password}"
  dw_username        = "${var.dw_username}"
  dw_password        = "${var.dw_password}"

  dev_db_snapshot_id      = "${var.dev_db_snapshot_id}"
  dev_db_snapshot_dbname  = "${var.dev_db_snapshot_dbname}"
  prod_db_snapshot_id     = "${var.prod_db_snapshot_id}"
  prod_db_snapshot_dbname = "${var.prod_db_snapshot_dbname}"
  dw_snapshot_id          = "${var.dw_snapshot_id}"
  dw_snapshot_dbname      = "${var.dw_snapshot_dbname}"

}

module "stage2" {
  source = "./stage2"
}

module "stage3" {
  source = "./stage3"

  access_key    = "${var.access_key}"
  secret_key    = "${var.secret_key}"
  aws_region    = "${var.aws_region}"
  domain        = "${var.domain}"
  deploy_prefix = "${var.deploy_prefix}"
  local_shell   = "${var.local_shell}"

  # k8s clusters
  k8s_dev_server_host = "${var.k8s_dev_server_host}"
  k8s_dev_server_port = "${var.k8s_dev_server_port}"

  k8s_dev_name      = "${var.k8s_dev_name}"
  k8s_dev_server    = "${var.k8s_dev_server}"
  k8s_dev_user      = "${var.k8s_dev_user}"
  k8s_dev_pass      = "${var.k8s_dev_pass}"
  k8s_dev_image     = "${var.k8s_dev_image}"
  k8s_dev_image_dev = "${var.k8s_dev_image_dev}"
  k8s_dev_cert_auth = "${var.k8s_dev_cert_auth}"
  k8s_dev_cert      = "${var.k8s_dev_cert}"
  k8s_dev_key       = "${var.k8s_dev_key}"

  k8s_prod_server_host = "${var.k8s_prod_server_host}"
  k8s_prod_server_port = "${var.k8s_prod_server_port}"

  k8s_prod_name      = "${var.k8s_prod_name}"
  k8s_prod_server    = "${var.k8s_prod_server}"
  k8s_prod_user      = "${var.k8s_prod_user}"
  k8s_prod_pass      = "${var.k8s_prod_pass}"
  k8s_prod_image     = "${var.k8s_prod_image}"
  k8s_prod_cert_auth = "${var.k8s_prod_cert_auth}"
  k8s_prod_cert      = "${var.k8s_prod_cert}"
  k8s_prod_key       = "${var.k8s_prod_key}"

  k8s_dev_ml_name      = "${var.k8s_dev_ml_name}"
  k8s_dev_ml_server    = "${var.k8s_dev_ml_server}"
  k8s_dev_ml_user      = "${var.k8s_dev_ml_user}"
  k8s_dev_ml_pass      = "${var.k8s_dev_ml_pass}"
  k8s_dev_ml_cert_auth = "${var.k8s_dev_ml_cert_auth}"

  # databases
  dev_db_host             = "dev.db.${var.domain}"
  dev_db_name             = "opsdx_dev"
  dev_db_username         = "${var.dev_db_username}"
  dev_db_password         = "${var.dev_db_password}"
  dev_etl_channel         = "${var.dev_etl_channel}"

  prod_db_host             = "prod.db.${var.domain}"
  prod_db_name             = "opsdx_prod"
  prod_db_username         = "${var.prod_db_username}"
  prod_db_password         = "${var.prod_db_password}"
  prod_etl_channel         = "${var.prod_etl_channel}"

  dev_etl_lambda_firing_rate_mins  = "${var.dev_etl_lambda_firing_rate_mins}"
  prod_etl_lambda_firing_rate_mins = "${var.prod_etl_lambda_firing_rate_mins}"

  dev_jhapi_client_id     = "${var.dev_jhapi_client_id}"
  dev_jhapi_client_secret = "${var.dev_jhapi_client_secret}"

  prod_jhapi_client_id     = "${var.prod_jhapi_client_id}"
  prod_jhapi_client_secret = "${var.prod_jhapi_client_secret}"

  DEV_ETL_SERVER             = "${var.DEV_ETL_SERVER}"
  DEV_ETL_HOSPITAL           = "${var.DEV_ETL_HOSPITAL}"
  DEV_ETL_HOURS              = "${var.DEV_ETL_HOURS}"
  DEV_ETL_ARCHIVE            = "${var.DEV_ETL_ARCHIVE}"
  DEV_ETL_MODE               = "${var.DEV_ETL_MODE}"
  DEV_ETL_DEMO_MODE          = "${var.DEV_ETL_DEMO_MODE}"
  DEV_ETL_STREAM_HOURS       = "${var.DEV_ETL_STREAM_HOURS}"
  DEV_ETL_STREAM_SLICES      = "${var.DEV_ETL_STREAM_SLICES}"
  DEV_ETL_STREAM_SLEEP_SECS  = "${var.DEV_ETL_STREAM_SLEEP_SECS}"
  DEV_ETL_EPIC_NOTIFICATIONS = "${var.DEV_ETL_EPIC_NOTIFICATIONS}"

  PROD_ETL_SERVER             = "${var.PROD_ETL_SERVER}"
  PROD_ETL_HOSPITAL           = "${var.PROD_ETL_HOSPITAL}"
  PROD_ETL_HOURS              = "${var.PROD_ETL_HOURS}"
  PROD_ETL_ARCHIVE            = "${var.PROD_ETL_ARCHIVE}"
  PROD_ETL_MODE               = "${var.PROD_ETL_MODE}"
  PROD_ETL_DEMO_MODE          = "${var.PROD_ETL_DEMO_MODE}"
  PROD_ETL_STREAM_HOURS       = "${var.PROD_ETL_STREAM_HOURS}"
  PROD_ETL_STREAM_SLICES      = "${var.PROD_ETL_STREAM_SLICES}"
  PROD_ETL_STREAM_SLEEP_SECS  = "${var.PROD_ETL_STREAM_SLEEP_SECS}"

  ######################################
  # Lambda functions.

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"

  lambda_subnet1_id = "${var.lambda_subnet1_id}"
  lambda_subnet2_id = "${var.lambda_subnet2_id}"
  lambda_sg_id      = "${var.lambda_sg_id}"


  ######################################
  # k8s job launcher

  aws_klaunch_lambda_package = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  ######################################
  # k8s weave cleaner

  aws_weave_cleaner_lambda_package = "${var.aws_weave_cleaner_lambda_package}"
  aws_weave_cleaner_lambda_role_arn = "${var.aws_weave_cleaner_lambda_role_arn}"

  weave_cleaner_firing_rate_mins = "${var.weave_cleaner_firing_rate_mins}"

  ######################################
  # Behavior monitors

  aws_behamon_lambda_package  = "${var.aws_behamon_lambda_package}"
  aws_behamon_lambda_role_arn = "${var.aws_behamon_lambda_role_arn}"

  dev_behamon_log_group_name  = "${var.dev_behamon_log_group_name}"
  dev_behamon_log_group_arn   = "${var.dev_behamon_log_group_arn}"

  prod_behamon_log_group_name = "${var.prod_behamon_log_group_name}"
  prod_behamon_log_group_arn  = "${var.prod_behamon_log_group_arn}"

  scorecard_report_firing_rate_min  = "${var.scorecard_report_firing_rate_min}"
  scorecard_report_firing_rate_expr = "${var.scorecard_report_firing_rate_expr}"

  scorecard_metric_firing_rate_min  = "${var.scorecard_metric_firing_rate_min}"
  scorecard_metric_firing_rate_expr = "${var.scorecard_metric_firing_rate_expr}"

  s3_weekly_report_firing_rate_min  = "${var.s3_weekly_report_firing_rate_min}"
  s3_weekly_report_firing_rate_expr = "${var.s3_weekly_report_firing_rate_expr}"

  ######################################
  # Alarm2slack

  aws_alarm2slack_package = "${var.aws_alarm2slack_package}"
  alarm2slack_kms_key_arn = "${var.alarm2slack_kms_key_arn}"

  slack_hook              = "${var.slack_hook}"
  slack_channel           = "${var.slack_channel}"
  slack_watchers          = "${var.slack_watchers}"

  info_slack_hook         = "${var.info_slack_hook}"
  info_slack_channel      = "${var.info_slack_channel}"
  info_slack_watchers     = "${var.info_slack_watchers}"

  ######################################
  # TREWS Capture
  k8s_dev_utilities_image = "${var.k8s_dev_utilities_image}"
  k8s_prod_utilities_image = "${var.k8s_prod_utilities_image}"

  trews_capture_url  = "${var.trews_capture_url}"
  trews_capture_firing_rate_min  = "${var.trews_capture_firing_rate_min}"
  trews_capture_firing_rate_expr = "${var.trews_capture_firing_rate_expr}"

  ######################################
  # TREWS Labeler
  k8s_dev_ml_trews_image = "${var.k8s_dev_ml_trews_image}"
  k8s_prod_ml_trews_image = "${var.k8s_prod_ml_trews_image}"

  trews_labeler_firing_rate_min  = "${var.trews_labeler_firing_rate_min}"
  trews_labeler_firing_rate_expr = "${var.trews_labeler_firing_rate_expr}"

}



##############################
# Outputs

# For shared VPC/Subnets/NAT GW in kops.

output "vpc_id" {
  value = "${module.stage1.vpc_id}"
}

output "vpc_cidr" {
  value = "${module.stage1.vpc_cidr}"
}

output "natgw1_id" {
  value = "${module.stage1.natgw1_id}"
}

output "natgw2_id" {
  value = "${module.stage1.natgw2_id}"
}

output "natgw3_id" {
  value = "${module.stage1.natgw3_id}"
}

output "utility1_subnet_id" {
  value = "${module.stage1.utility1_subnet_id}"
}

output "utility2_subnet_id" {
  value = "${module.stage1.utility2_subnet_id}"
}

output "utility3_subnet_id" {
  value = "${module.stage1.utility3_subnet_id}"
}

output "k8s1_subnet_id" {
  value = "${module.stage1.k8s1_subnet_id}"
}

output "k8s2_subnet_id" {
  value = "${module.stage1.k8s2_subnet_id}"
}

output "k8s3_subnet_id" {
  value = "${module.stage1.k8s3_subnet_id}"
}


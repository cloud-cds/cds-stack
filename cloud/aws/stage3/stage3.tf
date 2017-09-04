module "dev_etl" {
  source = "./dev-services/dev_etl"

  deploy_prefix = "${var.deploy_prefix}"
  local_shell   = "${var.local_shell}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_klaunch_lambda_package = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  k8s_dev_server_host = "${var.k8s_dev_server_host}"
  k8s_dev_server_port = "${var.k8s_dev_server_port}"

  k8s_dev_name      = "${var.k8s_dev_name}"
  k8s_dev_server    = "${var.k8s_dev_server}"
  k8s_dev_user      = "${var.k8s_dev_user}"
  k8s_dev_pass      = "${var.k8s_dev_pass}"
  k8s_dev_image     = "${var.k8s_dev_image}"
  k8s_dev_cert_auth = "${var.k8s_dev_cert_auth}"
  k8s_dev_cert      = "${var.k8s_dev_cert}"
  k8s_dev_key       = "${var.k8s_dev_key}"

  dev_etl_lambda_firing_rate_mins = "${var.dev_etl_lambda_firing_rate_mins}"

  dev_db_host             = "${var.dev_db_host}"
  dev_db_name             = "${var.dev_db_name}"
  dev_db_username         = "${var.dev_db_username}"
  dev_db_password         = "${var.dev_db_password}"
  dev_etl_channel         = "${var.dev_etl_channel}"

  jhapi_client_id     = "${var.jhapi_client_id}"
  jhapi_client_secret = "${var.jhapi_client_secret}"

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
}

module "dev_monitor" {
  source = "./dev-services/monitor"
  deploy_prefix = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_alarm2slack_package = "${var.aws_alarm2slack_package}"
  alarm2slack_kms_key_arn = "${var.alarm2slack_kms_key_arn}"

  slack_hook     = "${var.slack_hook}"
  slack_channel  = "${var.slack_channel}"
  slack_watchers = "${var.slack_watchers}"
}


module "prod_monitor" {
  source = "./prod-services/monitor"
  deploy_prefix = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_alarm2slack_package = "${var.aws_alarm2slack_package}"
  alarm2slack_kms_key_arn = "${var.alarm2slack_kms_key_arn}"

  slack_hook     = "${var.slack_hook}"
  slack_channel  = "${var.slack_channel}"
  slack_watchers = "${var.slack_watchers}"
}

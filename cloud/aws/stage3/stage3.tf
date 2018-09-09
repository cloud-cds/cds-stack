module "stage_etl" {
  source = "./dev-services/stage-etl"
  deploy_prefix = "${var.deploy_prefix}"
  local_shell   = "${var.local_shell}"

  s3_mc_lambda = "${var.s3_mc_lambda}"
  aws_klaunch_lambda_package = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  k8s_dev_server_host = "${var.k8s_dev_server_host}"
  k8s_dev_server_port = "${var.k8s_dev_server_port}"

  k8s_dev_name      = "${var.k8s_dev_name}"
  k8s_dev_server    = "${var.k8s_dev_server}"
  k8s_dev_user      = "${var.k8s_dev_user}"
  k8s_dev_pass      = "${var.k8s_dev_pass}"
  k8s_dev_image     = "${var.k8s_dev_image}"
  k8s_dev_image_dev     = "${var.k8s_dev_image_dev}"
  k8s_dev_cert_auth = "${var.k8s_dev_cert_auth}"
  k8s_dev_cert      = "${var.k8s_dev_cert}"
  k8s_dev_key       = "${var.k8s_dev_key}"

  dev_etl_lambda_firing_rate_mins = "${var.dev_etl_lambda_firing_rate_mins}"

  dev_db_host             = "${var.dev_db_host}"
  dev_db_name             = "${var.dev_db_name}"
  dev_db_username         = "${var.dev_db_username}"
  dev_db_password         = "${var.dev_db_password}"
  dev_etl_channel         = "${var.dev_etl_channel}"

  dev_jhapi_client_id     = "${var.dev_jhapi_client_id}"
  dev_jhapi_client_secret = "${var.dev_jhapi_client_secret}"

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

module "dev_etl" {
  source = "./dev-services/dev-etl"

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
  k8s_dev_image_dev     = "${var.k8s_dev_image_dev}"
  k8s_dev_cert_auth = "${var.k8s_dev_cert_auth}"
  k8s_dev_cert      = "${var.k8s_dev_cert}"
  k8s_dev_key       = "${var.k8s_dev_key}"

  dev_etl_lambda_firing_rate_mins = "${var.dev_etl_lambda_firing_rate_mins}"

  dev_db_host             = "${var.dev_db_host}"
  dev_db_name             = "${var.dev_db_name}"
  dev_db_username         = "${var.dev_db_username}"
  dev_db_password         = "${var.dev_db_password}"
  dev_etl_channel         = "${var.dev_etl_channel}"

  dev_jhapi_client_id     = "${var.dev_jhapi_client_id}"
  dev_jhapi_client_secret = "${var.dev_jhapi_client_secret}"

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

module "prod_etl" {
  source = "./prod-services/prod_etl"

  deploy_prefix = "${var.deploy_prefix}"
  local_shell   = "${var.local_shell}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_klaunch_lambda_package = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

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

  prod_etl_lambda_firing_rate_mins = "${var.prod_etl_lambda_firing_rate_mins}"

  prod_db_host             = "${var.prod_db_host}"
  prod_db_name             = "${var.prod_db_name}"
  prod_db_username         = "${var.prod_db_username}"
  prod_db_password         = "${var.prod_db_password}"
  prod_etl_channel         = "${var.prod_etl_channel}"

  prod_jhapi_client_id     = "${var.prod_jhapi_client_id}"
  prod_jhapi_client_secret = "${var.prod_jhapi_client_secret}"

  PROD_ETL_SERVER             = "${var.PROD_ETL_SERVER}"
  PROD_ETL_HOSPITAL           = "${var.PROD_ETL_HOSPITAL}"
  PROD_ETL_HOURS              = "${var.PROD_ETL_HOURS}"
  PROD_ETL_ARCHIVE            = "${var.PROD_ETL_ARCHIVE}"
  PROD_ETL_MODE               = "${var.PROD_ETL_MODE}"
  PROD_ETL_DEMO_MODE          = "${var.PROD_ETL_DEMO_MODE}"
  PROD_ETL_STREAM_HOURS       = "${var.PROD_ETL_STREAM_HOURS}"
  PROD_ETL_STREAM_SLICES      = "${var.PROD_ETL_STREAM_SLICES}"
  PROD_ETL_STREAM_SLEEP_SECS  = "${var.PROD_ETL_STREAM_SLEEP_SECS}"
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

  info_slack_hook     = "${var.info_slack_hook}"
  info_slack_channel  = "${var.info_slack_channel}"
  info_slack_watchers = "${var.info_slack_watchers}"
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

  info_slack_hook     = "${var.info_slack_hook}"
  info_slack_channel  = "${var.info_slack_channel}"
  info_slack_watchers = "${var.info_slack_watchers}"
}

module "dev_behavior_monitors" {
  source = "./dev-services/behavior-monitors"
  aws_region = "${var.aws_region}"
  deploy_prefix = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_behamon_lambda_package  = "${var.aws_behamon_lambda_package}"
  aws_behamon_lambda_role_arn = "${var.aws_behamon_lambda_role_arn}"
  aws_klaunch_lambda_package  = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  db_host     = "${var.dev_db_host}"
  db_name     = "${var.dev_db_name}"
  db_username = "${var.dev_db_username}"
  db_password = "${var.dev_db_password}"

  lambda_subnet1_id = "${var.lambda_subnet1_id}"
  lambda_subnet2_id = "${var.lambda_subnet2_id}"
  lambda_sg_id      = "${var.lambda_sg_id}"

  behamon_log_group_name = "${var.dev_behamon_log_group_name}"
  behamon_log_group_arn  = "${var.dev_behamon_log_group_arn}"

  k8s_server_host = "${var.k8s_dev_server_host}"
  k8s_server_port = "${var.k8s_dev_server_port}"

  k8s_name      = "${var.k8s_dev_name}"
  k8s_server    = "${var.k8s_dev_server}"
  k8s_user      = "${var.k8s_dev_user}"
  k8s_pass      = "${var.k8s_dev_pass}"
  k8s_cert_auth = "${var.k8s_dev_cert_auth}"

  k8s_scorecard_report_image = "${var.k8s_dev_image}"

  scorecard_report_firing_rate_min  = "${var.scorecard_report_firing_rate_min}"
  scorecard_report_firing_rate_expr = "${var.scorecard_report_firing_rate_expr}"

  k8s_scorecard_metric_image = "${var.k8s_dev_image}"

  scorecard_metric_firing_rate_min  = "${var.scorecard_metric_firing_rate_min}"
  scorecard_metric_firing_rate_expr = "${var.scorecard_metric_firing_rate_expr}"

  s3_weekly_report_firing_rate_min  = "${var.s3_weekly_report_firing_rate_min}"
  s3_weekly_report_firing_rate_expr = "${var.s3_weekly_report_firing_rate_expr}"
}

module "prod_behavior_monitors" {
  source = "./prod-services/behavior-monitors"
  aws_region = "${var.aws_region}"
  deploy_prefix = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_behamon_lambda_package  = "${var.aws_behamon_lambda_package}"
  aws_behamon_lambda_role_arn = "${var.aws_behamon_lambda_role_arn}"
  aws_klaunch_lambda_package  = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  db_host     = "${var.prod_db_host}"
  db_name     = "${var.prod_db_name}"
  db_username = "${var.prod_db_username}"
  db_password = "${var.prod_db_password}"

  lambda_subnet1_id = "${var.lambda_subnet1_id}"
  lambda_subnet2_id = "${var.lambda_subnet2_id}"
  lambda_sg_id      = "${var.lambda_sg_id}"

  behamon_log_group_name = "${var.prod_behamon_log_group_name}"
  behamon_log_group_arn  = "${var.prod_behamon_log_group_arn}"

  k8s_server_host = "${var.k8s_prod_server_host}"
  k8s_server_port = "${var.k8s_prod_server_port}"

  k8s_name      = "${var.k8s_prod_name}"
  k8s_server    = "${var.k8s_prod_server}"
  k8s_user      = "${var.k8s_prod_user}"
  k8s_pass      = "${var.k8s_prod_pass}"
  k8s_cert_auth = "${var.k8s_prod_cert_auth}"

  k8s_scorecard_report_image = "${var.k8s_prod_image}"

  scorecard_report_firing_rate_min  = "${var.scorecard_report_firing_rate_min}"
  scorecard_report_firing_rate_expr = "${var.scorecard_report_firing_rate_expr}"

  k8s_scorecard_metric_image = "${var.k8s_prod_image}"

  scorecard_metric_firing_rate_min  = "${var.scorecard_metric_firing_rate_min}"
  scorecard_metric_firing_rate_expr = "${var.scorecard_metric_firing_rate_expr}"
}


module "dev_ml_weave_cleaner" {
  source = "./dev-ml-services/k8s-weave-cleaner"

  deploy_prefix = "${var.deploy_prefix}"

  s3_opsdx_lambda     = "${var.s3_opsdx_lambda}"
  aws_lambda_package  = "${var.aws_weave_cleaner_lambda_package}"
  aws_lambda_role_arn = "${var.aws_weave_cleaner_lambda_role_arn}"

  firing_rate_mins = "${var.weave_cleaner_firing_rate_mins}"

  k8s_dev_ml_name      = "${var.k8s_dev_ml_name}"
  k8s_dev_ml_server    = "${var.k8s_dev_ml_server}"
  k8s_dev_ml_user      = "${var.k8s_dev_ml_user}"
  k8s_dev_ml_pass      = "${var.k8s_dev_ml_pass}"
  k8s_dev_ml_cert_auth = "${var.k8s_dev_ml_cert_auth}"
}


module "dev_trews_capture" {
  source = "./dev-services/trews-capture"
  aws_region = "${var.aws_region}"
  deploy_prefix = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_klaunch_lambda_package  = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  db_host     = "${var.dev_db_host}"
  db_name     = "${var.dev_db_name}"
  db_username = "${var.dev_db_username}"
  db_password = "${var.dev_db_password}"

  lambda_subnet1_id = "${var.lambda_subnet1_id}"
  lambda_subnet2_id = "${var.lambda_subnet2_id}"
  lambda_sg_id      = "${var.lambda_sg_id}"

  k8s_server_host = "${var.k8s_dev_server_host}"
  k8s_server_port = "${var.k8s_dev_server_port}"

  k8s_name      = "${var.k8s_dev_name}"
  k8s_server    = "${var.k8s_dev_server}"
  k8s_user      = "${var.k8s_dev_user}"
  k8s_pass      = "${var.k8s_dev_pass}"
  k8s_cert_auth = "${var.k8s_dev_cert_auth}"

  k8s_trews_capture_image = "${var.k8s_dev_utilities_image}"

  trews_capture_url = "${var.trews_capture_url}"
  trews_capture_firing_rate_min  = "${var.trews_capture_firing_rate_min}"
  trews_capture_firing_rate_expr = "${var.trews_capture_firing_rate_expr}"
}


module "dev_trews_labeler" {
  source = "./dev-services/trews-labeler"
  aws_access_key_id     = "${var.access_key}"
  aws_secret_access_key = "${var.secret_key}"
  aws_region            = "${var.aws_region}"
  deploy_prefix         = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_klaunch_lambda_package  = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  db_password = "${var.dev_db_password}"

  lambda_subnet1_id = "${var.lambda_subnet1_id}"
  lambda_subnet2_id = "${var.lambda_subnet2_id}"
  lambda_sg_id      = "${var.lambda_sg_id}"

  k8s_server_host = "${var.k8s_dev_server_host}"
  k8s_server_port = "${var.k8s_dev_server_port}"

  k8s_name      = "${var.k8s_dev_name}"
  k8s_server    = "${var.k8s_dev_server}"
  k8s_user      = "${var.k8s_dev_user}"
  k8s_pass      = "${var.k8s_dev_pass}"
  k8s_cert_auth = "${var.k8s_dev_cert_auth}"

  k8s_trews_labeler_image = "${var.k8s_dev_ml_trews_image}"

  trews_labeler_firing_rate_min  = "${var.trews_labeler_firing_rate_min}"
  trews_labeler_firing_rate_expr = "${var.trews_labeler_firing_rate_expr}"
}

module "prod_trews_labeler" {
  source = "./prod-services/trews-labeler"
  aws_access_key_id     = "${var.access_key}"
  aws_secret_access_key = "${var.secret_key}"
  aws_region            = "${var.aws_region}"
  deploy_prefix         = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_klaunch_lambda_package  = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  db_password = "${var.prod_db_password}"

  lambda_subnet1_id = "${var.lambda_subnet1_id}"
  lambda_subnet2_id = "${var.lambda_subnet2_id}"
  lambda_sg_id      = "${var.lambda_sg_id}"

  k8s_server_host = "${var.k8s_prod_server_host}"
  k8s_server_port = "${var.k8s_prod_server_port}"

  k8s_name      = "${var.k8s_prod_name}"
  k8s_server    = "${var.k8s_prod_server}"
  k8s_user      = "${var.k8s_prod_user}"
  k8s_pass      = "${var.k8s_prod_pass}"
  k8s_cert_auth = "${var.k8s_prod_cert_auth}"

  k8s_trews_labeler_image = "${var.k8s_prod_ml_trews_image}"

  trews_labeler_firing_rate_min  = "${var.trews_labeler_firing_rate_min}"
  trews_labeler_firing_rate_expr = "${var.trews_labeler_firing_rate_expr}"
}

module "test_session_loader" {
  source = "./dev-services/trews-session-loader"
  aws_region = "${var.aws_region}"
  deploy_prefix = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_klaunch_lambda_package  = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  db_host     = "${var.dev_db_host}"
  db_name     = "opsdx_test"
  db_username = "${var.dev_db_username}"
  db_password = "${var.dev_db_password}"

  dev_jhapi_client_id     = "${var.dev_jhapi_client_id}"
  dev_jhapi_client_secret = "${var.dev_jhapi_client_secret}"

  lambda_subnet1_id = "${var.lambda_subnet1_id}"
  lambda_subnet2_id = "${var.lambda_subnet2_id}"
  lambda_sg_id      = "${var.lambda_sg_id}"

  k8s_server_host = "${var.k8s_dev_server_host}"
  k8s_server_port = "${var.k8s_dev_server_port}"

  k8s_name      = "${var.k8s_dev_name}"
  k8s_server    = "${var.k8s_dev_server}"
  k8s_user      = "${var.k8s_dev_user}"
  k8s_pass      = "${var.k8s_dev_pass}"
  k8s_cert_auth = "${var.k8s_dev_cert_auth}"

  k8s_session_loader_image = "${var.k8s_dev_image}"

  session_loader_firing_rate_min  = "10"
  session_loader_firing_rate_expr = "10 minutes"
}


module "prod_session_loader" {
  source = "./prod-services/trews-session-loader"
  aws_region = "${var.aws_region}"
  deploy_prefix = "${var.deploy_prefix}"

  s3_opsdx_lambda = "${var.s3_opsdx_lambda}"
  aws_klaunch_lambda_package  = "${var.aws_klaunch_lambda_package}"
  aws_klaunch_lambda_role_arn = "${var.aws_klaunch_lambda_role_arn}"

  db_host     = "${var.prod_db_host}"
  db_name     = "${var.prod_db_name}"
  db_username = "${var.prod_db_username}"
  db_password = "${var.prod_db_password}"

  prod_jhapi_client_id     = "${var.prod_jhapi_client_id}"
  prod_jhapi_client_secret = "${var.prod_jhapi_client_secret}"

  lambda_subnet1_id = "${var.lambda_subnet1_id}"
  lambda_subnet2_id = "${var.lambda_subnet2_id}"
  lambda_sg_id      = "${var.lambda_sg_id}"

  k8s_server_host = "${var.k8s_prod_server_host}"
  k8s_server_port = "${var.k8s_prod_server_port}"

  k8s_name      = "${var.k8s_prod_name}"
  k8s_server    = "${var.k8s_prod_server}"
  k8s_user      = "${var.k8s_prod_user}"
  k8s_pass      = "${var.k8s_prod_pass}"
  k8s_cert_auth = "${var.k8s_prod_cert_auth}"

  k8s_session_loader_image = "${var.k8s_prod_image}"

  session_loader_firing_rate_min  = "10"
  session_loader_firing_rate_expr = "10 minutes"
}

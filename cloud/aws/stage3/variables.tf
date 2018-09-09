variable "access_key" {}
variable "secret_key" {}
variable "aws_region" {
  description = "AWS region to launch servers."
  default = "us-east-1"
}

###################################
# K8s

variable "k8s_dev_server_host" {
  description = "Kubernetes master host"
}

variable "k8s_dev_server_port" {
  description = "Kubernetes master port"
}

variable "k8s_dev_name" {
  description = "Kubernetes context name"
}

variable "k8s_dev_server" {
  description = "Kubernetes master dns name"
}

variable "k8s_dev_user" {
  description = "Kubernetes username"
}

variable "k8s_dev_pass" {
  description = "Kubernetes password"
}

variable "k8s_dev_cert_auth" {
  description = "Kubernetes certificate auth data"
}

variable "k8s_dev_cert" {
  description = "Kubernetes client certificate data"
}

variable "k8s_dev_key" {
  description = "Kubernetes client key data"
}

variable "k8s_dev_image" {}
variable "k8s_dev_image_dev" {}
variable "k8s_dev_utilities_image" {}
variable "k8s_dev_ml_trews_image" {}

variable "k8s_prod_server_host" {
  description = "Kubernetes master host"
}

variable "k8s_prod_server_port" {
  description = "Kubernetes master port"
}

variable "k8s_prod_name" {
  description = "Kubernetes context name"
}

variable "k8s_prod_server" {
  description = "Kubernetes master dns name"
}

variable "k8s_prod_user" {
  description = "Kubernetes username"
}

variable "k8s_prod_pass" {
  description = "Kubernetes password"
}

variable "k8s_prod_cert_auth" {
  description = "Kubernetes certificate auth data"
}

variable "k8s_prod_cert" {
  description = "Kubernetes client certificate data"
}

variable "k8s_prod_key" {
  description = "Kubernetes client key data"
}

variable "k8s_prod_image" {}
variable "k8s_prod_utilities_image" {}
variable "k8s_prod_ml_trews_image" {}

variable "k8s_dev_ml_name" {}
variable "k8s_dev_ml_server" {}
variable "k8s_dev_ml_user" {}
variable "k8s_dev_ml_pass" {}
variable "k8s_dev_ml_cert_auth" {}


####################################
# JH API

variable "dev_jhapi_client_id" {
  description = "EPIC JHAPI Login"
}

variable "dev_jhapi_client_secret" {
  description = "EPIC JHAPI Secret"
}

variable "prod_jhapi_client_id" {
  description = "EPIC JHAPI Login"
}

variable "prod_jhapi_client_secret" {
  description = "EPIC JHAPI Secret"
}

####################################
# Command execution

variable "local_shell" {
  description = "Run a local bash shell (for Windows/MSYS2)"
}

variable "domain" {}
variable "deploy_prefix" {}

####################################
# Trews ETL parameters

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

variable "PROD_ETL_SERVER" {}
variable "PROD_ETL_HOSPITAL" {}
variable "PROD_ETL_HOURS" {}
variable "PROD_ETL_ARCHIVE" {}
variable "PROD_ETL_MODE" {}
variable "PROD_ETL_DEMO_MODE" {}
variable "PROD_ETL_STREAM_HOURS" {}
variable "PROD_ETL_STREAM_SLICES" {}
variable "PROD_ETL_STREAM_SLEEP_SECS" {}

# dev db
variable "dev_db_username" {}
variable "dev_db_password" {}
variable "dev_db_host" {
  default = "mcdb.metaboliccompass.com"
}
variable "dev_db_name" {
  default = "metabolic_compass"
}
variable "dev_etl_channel" {}
variable "dev_etl_lambda_firing_rate_mins" {}


# prod db
variable "prod_db_username" {}
variable "prod_db_password" {}
variable "prod_db_host" {
  default = "mcdb.metaboliccompass.com"
}
variable "prod_db_name" {
  default = "metabolic_compass"
}
variable "prod_etl_channel" {}
variable "prod_etl_lambda_firing_rate_mins" {}


####################################
# Lambda packages

variable "s3_opsdx_lambda" {}
variable "s3_mc_lambda" {}

variable "lambda_subnet1_id" {}
variable "lambda_subnet2_id" {}
variable "lambda_sg_id" {}

variable "aws_klaunch_lambda_role_arn" {}
variable "aws_klaunch_lambda_package" {}

variable "aws_alarm2slack_package" {}

variable "aws_behamon_lambda_package" {}
variable "aws_behamon_lambda_role_arn" {}

variable "aws_weave_cleaner_lambda_package" {}
variable "aws_weave_cleaner_lambda_role_arn" {}


######################################
# Behavior monitors

variable "dev_behamon_log_group_name" {}
variable "dev_behamon_log_group_arn" {}

variable "prod_behamon_log_group_name" {}
variable "prod_behamon_log_group_arn" {}

variable "scorecard_report_firing_rate_min" {}
variable "scorecard_report_firing_rate_expr" {}

variable "scorecard_metric_firing_rate_min" {}
variable "scorecard_metric_firing_rate_expr" {}

variable "s3_weekly_report_firing_rate_min" {}
variable "s3_weekly_report_firing_rate_expr" {}

######################################
# Alarm2Slack

variable "alarm2slack_kms_key_arn" {}

variable "slack_hook" {}
variable "slack_channel" {}
variable "slack_watchers" {}

variable "info_slack_hook" {}
variable "info_slack_channel" {}
variable "info_slack_watchers" {}

######################################
# Weave Cleaner

variable "weave_cleaner_firing_rate_mins" {}


######################################
# TREWS capture

variable "trews_capture_url" {}
variable "trews_capture_firing_rate_min" {}
variable "trews_capture_firing_rate_expr" {}


######################################
# TREWS labeler

variable "trews_labeler_firing_rate_min" {}
variable "trews_labeler_firing_rate_expr" {}


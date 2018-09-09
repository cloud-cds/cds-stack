##################################
# AWS account

variable "aws_id" {
  description = "AWS account ID"
}

variable "access_key" {
  description = "AWS access key ID"
}

variable "secret_key" {
  description = "AWS secret key"
}

######################################
# Deployment

variable "deploy_name" {
  description = "Name Tag for AWS deployments"
}

variable "deploy_stack" {
  description = "Stack Tag for AWS deployments"
}

variable "deploy_prefix" {
  description = "AWS Resource Name Prefix for Deployment"
}

variable "aws_region" {
  description = "AWS region to launch servers."
  default = "us-east-1"
}


######################################
# PKI

variable "public_key_path" {
  description = "OpsDX public key path"
}

variable "private_key_path" {
  description = "OpsDX private key path"
}


########################################
# Auditing & logging.

variable "audit_sns_protocol" {
  description = "Protocol for receiving audit log ready notifications"
  default = "https"
}

variable "audit_sns_endpoint" {
  description = "Endpoint for receiving audit log ready notifications"
}


##################################
# DNS variables

variable "root_domain" {
  description = "Cluster root domain name"
}

variable "domain" {
  description = "Cluster domain name"
}

#######################
# DB

variable "dev_db_username" {
  description = "DB admin account"
}

variable "dev_db_password" {
  description = "DB Password"
}

variable "prod_db_username" {
  description = "DB admin account"
}

variable "prod_db_password" {
  description = "DB Password"
}

variable "dw_username" {
  description = "DW admin account"
}

variable "dw_password" {
  description = "DW Password"
}

# Restoring from backups.
variable "dev_db_snapshot_id" {
  description = "RDS Snapshot Identifier for Restoration"
  default = ""
}

variable "dev_db_snapshot_dbname" {
  description = "Database name in the snapshot"
  default = ""
}

# Restoring from backups.
variable "prod_db_snapshot_id" {
  description = "RDS Snapshot Identifier for Restoration"
  default = ""
}

variable "prod_db_snapshot_dbname" {
  description = "Database name in the snapshot"
  default = ""
}

variable "dw_snapshot_id" {
  description = "DW Snapshot Identifier for Restoration"
  default = ""
}

variable "dw_snapshot_dbname" {
  description = "Database name in the snapshot"
  default = ""
}

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

####################################
# dev_etl parameters

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


####################################
# prod_etl parameters

variable "PROD_ETL_SERVER" {}
variable "PROD_ETL_HOSPITAL" {}
variable "PROD_ETL_HOURS" {}
variable "PROD_ETL_ARCHIVE" {}
variable "PROD_ETL_MODE" {}
variable "PROD_ETL_DEMO_MODE" {}
variable "PROD_ETL_STREAM_HOURS" {}
variable "PROD_ETL_STREAM_SLICES" {}
variable "PROD_ETL_STREAM_SLEEP_SECS" {}

###################################
# K8s dev

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

###################################
# K8s prod

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

###################################
# K8s dev ml

variable "k8s_dev_ml_name" {}
variable "k8s_dev_ml_server" {}
variable "k8s_dev_ml_user" {}
variable "k8s_dev_ml_pass" {}
variable "k8s_dev_ml_cert_auth" {}


###################################
# etl
variable "dev_etl_channel" {}
variable "dev_etl_lambda_firing_rate_mins" {}

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

variable "aws_weave_cleaner_lambda_role_arn" {}
variable "aws_weave_cleaner_lambda_package" {}

variable "aws_behamon_lambda_package" {}
variable "aws_behamon_lambda_role_arn" {}

variable "aws_alarm2slack_package" {}

######################################
# k8s weave cleaner

variable "weave_cleaner_firing_rate_mins" {}

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
# TREWS Capture

variable "trews_capture_url" {}
variable "trews_capture_firing_rate_min" {}
variable "trews_capture_firing_rate_expr" {}

######################################
# TREWS labeler

variable "trews_labeler_firing_rate_min" {}
variable "trews_labeler_firing_rate_expr" {}


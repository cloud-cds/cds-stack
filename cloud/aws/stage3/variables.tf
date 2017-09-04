variable "aws_region" {}

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
####################################
# JH API

variable "jhapi_client_id" {
  description = "EPIC JHAPI Login"
}

variable "jhapi_client_secret" {
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
variable "dev_db_host" {}
variable "dev_db_name" {}

variable "dev_etl_channel" {}
variable "dev_etl_lambda_firing_rate_mins" {}


# prod db
variable "prod_db_username" {}
variable "prod_db_password" {}
variable "prod_db_host" {}
variable "prod_db_name" {}

variable "prod_etl_channel" {}
variable "prod_etl_lambda_firing_rate_mins" {}


####################################
# Lambda packages

variable "s3_opsdx_lambda" {}

variable "lambda_subnet1_id" {}
variable "lambda_subnet2_id" {}
variable "lambda_sg_id" {}

variable "aws_klaunch_lambda_role_arn" {}
variable "aws_klaunch_lambda_package" {}

variable "aws_alarm2slack_package" {}

variable "aws_behamon_lambda_package" {}
variable "aws_behamon_lambda_role_arn" {}


######################################
# Behavior monitors

variable "dev_behamon_log_group_name" {}
variable "dev_behamon_log_group_arn" {}

variable "prod_behamon_log_group_name" {}
variable "prod_behamon_log_group_arn" {}


######################################
# Alarm2Slack

variable "alarm2slack_kms_key_arn" {}
variable "slack_hook" {}
variable "slack_channel" {}
variable "slack_watchers" {}

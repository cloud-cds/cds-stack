terraform {
  backend "s3" {
    encrypt = "true"
    bucket  = "opsdx-terraform"
    key     = "terraform_state/opsdx"
    region  = "us-east-1"
  }
}

provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region     = "${var.aws_region}"
  profile    = "opsdx"
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
  deploy_prefix = "${var.deploy_prefix}"
  local_shell   = "${var.local_shell}"
  domain = "${var.domain}"
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

  dev_ops_etl_lambda_firing_rate_mins = "45"

  dev_db_host             = "dev.db.${var.domain}"
  dev_db_name             = "${replace(var.deploy_prefix, "-", "_")}"
  dev_db_username         = "${var.dev_db_username}"
  dev_db_password         = "${var.dev_db_password}"

  jhapi_client_id     = "${var.jhapi_client_id}"
  jhapi_client_secret = "${var.jhapi_client_secret}"
  dev_etl_channel     = "${var.dev_etl_channel}"

  DEV_OPS_ETL_SERVER             = "${var.DEV_OPS_ETL_SERVER}"
  DEV_OPS_ETL_HOSPITAL           = "${var.DEV_OPS_ETL_HOSPITAL}"
  DEV_OPS_ETL_HOURS              = "${var.DEV_OPS_ETL_HOURS}"
  DEV_OPS_ETL_ARCHIVE            = "${var.DEV_OPS_ETL_ARCHIVE}"
  DEV_OPS_ETL_MODE               = "${var.DEV_OPS_ETL_MODE}"
  DEV_OPS_ETL_DEMO_MODE          = "${var.DEV_OPS_ETL_DEMO_MODE}"
  DEV_OPS_ETL_STREAM_HOURS       = "${var.DEV_OPS_ETL_STREAM_HOURS}"
  DEV_OPS_ETL_STREAM_SLICES      = "${var.DEV_OPS_ETL_STREAM_SLICES}"
  DEV_OPS_ETL_STREAM_SLEEP_SECS  = "${var.DEV_OPS_ETL_STREAM_SLEEP_SECS}"
  DEV_OPS_ETL_EPIC_NOTIFICATIONS = "${var.DEV_OPS_ETL_EPIC_NOTIFICATIONS}"
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


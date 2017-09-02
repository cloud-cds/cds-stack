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

# module "stage2" {
#   source = "./stage2"
#   deploy_name   = "${var.deploy_name}"
#   deploy_stack  = "${var.deploy_stack}"
#   deploy_prefix = "${var.deploy_prefix}"
# }

# module "stage3" {
#   source = "./stage3"
#   deploy_name   = "${var.deploy_name}"
#   deploy_stack  = "${var.deploy_stack}"
#   deploy_prefix = "${var.deploy_prefix}"
# }



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


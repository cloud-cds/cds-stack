# Specify the provider and access details
provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region     = "${var.aws_region}"
  profile    = "opsdx"
}

module "dns" {
  source = "./dns"
  domain = "${var.domain}"
}

module "common" {
  source = "./common"
  deploy_name  = "${var.deploy_name}"
  deploy_stack = "${var.deploy_stack}"
  key_name     = "${var.key_name}"
}

module "core" {
  source = "./core"

  deploy_name  = "${var.deploy_name}"
  deploy_stack = "${var.deploy_stack}"

  auth_key          = "${module.common.auth_key}"
  private_key_path  = "${var.private_key_path}"

  controller_ami      = "${module.common.controller_ami}"
  domain_zone_id      = "${module.dns.zone_id}"
  controller_dns_name = "controller.${var.domain}" 
  trews_dns_name      = "trews.${var.domain}"
}

module "audit" {
  source = "./audit"

  deploy_name  = "${var.deploy_name}"
  deploy_stack = "${var.deploy_stack}"

  aws_id             = "${var.aws_id}"
  aws_region         = "${var.aws_region}"
  local_shell        = "${var.local_shell}"
  audit_sns_protocol = "${var.audit_sns_protocol}"
  audit_sns_endpoint = "${var.audit_sns_endpoint}"
}

module "db" {
  source = "./db"

  deploy_name  = "${var.deploy_name}"
  deploy_stack = "${var.deploy_stack}"

  vpc_id                = "${module.core.vpc_id}"
  db_password           = "${var.db_password}"
  db_subnet1_cidr       = "${var.db_subnet1_cidr}"
  db_availability_zone1 = "${var.db_availability_zone1}"
  db_subnet2_cidr       = "${var.db_subnet2_cidr}"
  db_availability_zone2 = "${var.db_availability_zone2}"
}

module "k8s" {
  source = "./k8s"
  dummy_file = "foo"
}

module "web" {
  source      = "./services/web"
  local_shell = "${var.local_shell}"
}

module "trews_etl" {
  source = "./services/trews_etl"
  aws_trews_etl_package = "${var.aws_trews_etl_package}"
}

######################
# Outputs

output "vpc_id" {
  value = "${module.core.vpc_id}"
}

output "db_ip" {
  value = "${module.db.db_ip}"
}

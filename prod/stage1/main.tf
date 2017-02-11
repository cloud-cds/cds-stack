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
  root_domain = "${var.root_domain}"
}

module "common" {
  source = "./common"
  deploy_name   = "${var.deploy_name}"
  deploy_stack  = "${var.deploy_stack}"
  deploy_prefix = "${var.deploy_prefix}"
  public_key_path = "${var.public_key_path}"
}

module "core" {
  source = "./core"

  deploy_name   = "${var.deploy_name}"
  deploy_stack  = "${var.deploy_stack}"
  deploy_prefix = "${var.deploy_prefix}"

  auth_key          = "${module.common.auth_key}"
  private_key_path  = "${var.private_key_path}"

  controller_ami      = "${module.common.controller_ami}"
  domain_zone_id      = "${module.dns.zone_id}"
  controller_dns_name = "controller.${var.domain}"
  trews_dns_name      = "trews.${var.domain}"
}

module "audit" {
  source = "./audit"

  deploy_name   = "${var.deploy_name}"
  deploy_stack  = "${var.deploy_stack}"
  deploy_prefix = "${var.deploy_prefix}"

  aws_id             = "${var.aws_id}"
  aws_region         = "${var.aws_region}"
  local_shell        = "${var.local_shell}"
  audit_sns_protocol = "${var.audit_sns_protocol}"
  audit_sns_endpoint = "${var.audit_sns_endpoint}"
}

module "db" {
  source = "./db"

  deploy_name   = "${var.deploy_name}"
  deploy_stack  = "${var.deploy_stack}"
  deploy_prefix = "${var.deploy_prefix}"

  vpc_id                = "${module.core.vpc_id}"
  db_identifier         = "${var.deploy_prefix}"
  db_name               = "${replace(var.deploy_prefix, "-", "_")}"
  db_username           = "${var.db_username}"
  db_password           = "${var.db_password}"
  db_subnet1_cidr       = "${var.db_subnet1_cidr}"
  db_availability_zone1 = "${var.db_availability_zone1}"
  db_subnet2_cidr       = "${var.db_subnet2_cidr}"
  db_availability_zone2 = "${var.db_availability_zone2}"

  domain_zone_id  = "${module.dns.zone_id}"
  db_dns_name     = "db.${var.domain}"
}

######################
# Outputs

output "vpc_id" {
  value = "${module.core.vpc_id}"
}

output "db_ip" {
  value = "${module.db.db_ip}"
}

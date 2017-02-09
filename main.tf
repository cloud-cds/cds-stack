# Specify the provider and access details
provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region     = "${var.aws_region}"
  profile    = "opsdx"
}

module "dns" {
  source = "./dns"
  k8s_domain = "${var.k8s_domain}"
  opsdx_domain = "${var.opsdx_domain}"
}

module "common" {
  source = "./common"
}

module "core" {
  source = "./core"

  auth_key          = "${module.common.auth_key}"
  private_key_path  = "${var.private_key_path}"

  controller_ami      = "${module.common.controller_ami}"
  domain_zone_id      = "${module.dns.zone_id}"
  controller_dns_name = "controller.${var.k8s_domain}" 
  trews_dns_name      = "trews.${var.k8s_domain}"
}

module "audit" {
  source = "./audit"
  aws_id             = "${var.aws_id}"
  aws_region         = "${var.aws_region}"
  local_shell        = "${var.local_shell}"
  audit_sns_protocol = "${var.audit_sns_protocol}"
  audit_sns_endpoint = "${var.audit_sns_endpoint}"
}

module "db" {
  source = "./db"
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
  local_shell  = "${var.local_shell}"
  web_instance = "t2.medium"
  gpu_instance = "p2.xlarge"
  cpu_instance = "c4.large"
  jnb_instance = "t2.large"
  enable_nodesets = 1
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

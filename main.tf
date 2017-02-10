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
  deploy_name   = "${var.deploy_name}"
  deploy_stack  = "${var.deploy_stack}"
  deploy_prefix = "${var.deploy_prefix}"
  key_name      = "${var.key_name}"
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
  db_username           = "${var.db_username}"
  db_password           = "${var.db_password}"
  db_subnet1_cidr       = "${var.db_subnet1_cidr}"
  db_availability_zone1 = "${var.db_availability_zone1}"
  db_subnet2_cidr       = "${var.db_subnet2_cidr}"
  db_availability_zone2 = "${var.db_availability_zone2}"

  domain_zone_id  = "${module.dns.zone_id}"
  db_dns_name     = "db.${var.domain}"
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

module "ebs" {
  source = "./services/ebs"
  local_shell = "${var.local_shell}"
}

module "zookeeper" {
  source = "./services/zookeeper"
  local_shell = "${var.local_shell}"
}

module "confluent" {
  source = "./services/confluent"
  local_shell = "${var.local_shell}"
}

module "web" {
  source        = "./services/web"
  deploy_name   = "${var.deploy_name}"
  deploy_stack  = "${var.deploy_stack}"
  deploy_prefix = "${var.deploy_prefix}"
  aws_id        = "${var.aws_id}"
  aws_region    = "${var.aws_region}"

  local_shell       = "${var.local_shell}"
  domain_zone_id    = "${module.dns.zone_id}"
  web_dns_name      = "api.${var.domain}"
  web_hostname_file = "services/web/hostname"
}

module "trews_etl" {
  source = "./services/trews_etl"

  aws_trews_etl_package = "${var.aws_trews_etl_package}"

  k8s_server_host = "${var.k8s_server_host}"
  k8s_server_port = "${var.k8s_server_port}"

  k8s_name      = "${var.k8s_name}"
  k8s_server    = "${var.k8s_server}"
  k8s_user      = "${var.k8s_user}"
  k8s_pass      = "${var.k8s_pass}"
  k8s_cert_auth = "${var.k8s_cert_auth}"
  k8s_cert      = "${var.k8s_cert}"
  k8s_key       = "${var.k8s_key}"
  k8s_token     = "${var.k8s_token}"

  db_host             = "db.${var.domain}"
  db_name             = "${module.db.prod_db_name}"
  db_username         = "${var.db_username}"
  db_password         = "${var.db_password}"
  jhapi_client_id     = "${var.jhapi_client_id}"
  jhapi_client_secret = "${var.jhapi_client_secret}"
}

######################
# Outputs

output "vpc_id" {
  value = "${module.core.vpc_id}"
}

output "db_ip" {
  value = "${module.db.db_ip}"
}

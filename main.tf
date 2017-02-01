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
}

module "core" {
  source = "./core"

  aws_region        = "${var.aws_region}"
  aws_base_ami      = "${var.aws_base_ami}"
  aws_amis          = "${var.aws_amis}"

  aws_id            = "${var.aws_id}"
  access_key        = "${var.access_key}"
  secret_key        = "${var.secret_key}"

  key_name          = "${var.key_name}"
  public_key_path   = "${var.public_key_path}"
  private_key_path  = "${var.private_key_path}"

  domain_zone_id      = "${module.dns.zone_id}"
  controller_dns_name = "controller.${var.k8s_domain}"
}

module "audit" {
  source = "./audit"
  aws_id             = "${var.aws_id}"
  aws_region         = "${var.aws_region}"
  local_shell        = "${var.local_shell}"
  audit_sns_protocol = "${var.audit_sns_protocol}"
  audit_sns_endpoint = "${var.audit_sns_endpoint}"
}

module "storage" {
  source = "./storage"
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

module "db_config" {
  source = "./db_config"
  db_ip       = "${module.db.db_ip}"
  db_password = "${var.db_password}"
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
  source      = "./services/web"
  local_shell = "${var.local_shell}"
}

module "jupyter" {
  source = "./services/jupyter"
  local_shell = "${var.local_shell}"
}

module "deis" {
  source = "./services/deis"
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

output "tensorflow_registry_url" {
  value = "${module.storage.tensorflow_registry_url}"
}

output "deis_registry_url" {
  value = "${module.deis.deis_registry_url}"
}

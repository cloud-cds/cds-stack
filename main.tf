# Specify the provider and access details
provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region     = "${var.aws_region}"
  profile    = "opsdx"
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
}

module "dns" {
  source = "./dns"
  k8s_domain = "${var.k8s_domain}"
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

# module "db_config" {
#   source = "./db_config"
#   db_ip       = "${module.db.db_ip}"
#   db_password = "${var.db_password}"
# }

module "k8s" {
  source = "./k8s"
  dummy_file = "foo"
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


######################
# Outputs

output "vpc_id" {
  value = "${module.core.vpc_id}"
}

output "tensorflow_registry_url" {
  value = "${module.storage.tensorflow_registry_url}"
}
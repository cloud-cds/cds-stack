# Module order:
# - dns
# - core
# - audit
# - db

module "dns" {
  source = "./dns"
  domain = "${var.domain}"
  root_domain = "${var.root_domain}"
}

module "core" {
  source = "./core"

  deploy_name     = "${var.deploy_name}"
  deploy_stack    = "${var.deploy_stack}"
  deploy_prefix   = "${var.deploy_prefix}"

  az1 = "us-east-1a"
  az2 = "us-east-1c"
  az3 = "us-east-1d"

  public_key_path   = "${var.public_key_path}"
  private_key_path  = "${var.private_key_path}"

  domain_zone_id      = "${module.dns.zone_id}"
  controller_dns_name = "controller.${var.domain}"
  windows_dns_name = "windows.${var.domain}"
}

###########
# Commented out since this duplicates JH AWS cloudtrail.
# Enable this at new deployments where we are not linked in
# with an institution's security team.
############
#
# module "audit" {
#   source = "./audit"
#
#   deploy_name   = "${var.deploy_name}"
#   deploy_stack  = "${var.deploy_stack}"
#   deploy_prefix = "${var.deploy_prefix}"
#
#   aws_id             = "${var.aws_id}"
#   aws_region         = "${var.aws_region}"
#   audit_sns_protocol = "${var.audit_sns_protocol}"
#   audit_sns_endpoint = "${var.audit_sns_endpoint}"
# }


module "db" {
  source = "./db"

  deploy_name   = "${var.deploy_name}"
  deploy_stack  = "${var.deploy_stack}"
  deploy_prefix = "${var.deploy_prefix}"

  vpc_id                = "${module.core.vpc_id}"

  dev_db_identifier     = "${var.deploy_prefix}-dev"
  dev_db_name           = "${var.dev_db_snapshot_dbname != "" ? var.dev_db_snapshot_dbname : "${replace(var.deploy_prefix, "-", "_")}_dev"}"
  dev_db_username       = "${var.dev_db_username}"
  dev_db_password       = "${var.dev_db_password}"
  dev_db_snapshot_id    = "${var.dev_db_snapshot_id}"

  prod_db_identifier    = "${var.deploy_prefix}-prod"
  prod_db_name          = "${var.prod_db_snapshot_dbname != "" ? var.prod_db_snapshot_dbname : "${replace(var.deploy_prefix, "-", "_")}_prod"}"
  prod_db_username      = "${var.prod_db_username}"
  prod_db_password      = "${var.prod_db_password}"
  prod_db_snapshot_id   = "${var.prod_db_snapshot_id}"

  dw_identifier         = "${var.deploy_prefix}-dw"
  dw_name               = "${var.dw_snapshot_dbname != "" ? var.dw_snapshot_dbname : "${replace(var.deploy_prefix, "-", "_")}_dw"}"
  dw_username           = "${var.dw_username}"
  dw_password           = "${var.dw_password}"
  dw_snapshot_id        = "${var.dw_snapshot_id}"

  db_parameter_group    = "${var.deploy_prefix}-pgstats96"
  dw_parameter_group    = "${var.deploy_prefix}-pgetl96"
  db_subnet1_cidr       = "${var.db_subnet1_cidr}"
  db_availability_zone1 = "${var.db_availability_zone1}"
  db_subnet2_cidr       = "${var.db_subnet2_cidr}"
  db_availability_zone2 = "${var.db_availability_zone2}"

  domain_zone_id      = "${module.dns.zone_id}"
  dev_db_dns_name     = "dev.db.${var.domain}"
  prod_db_dns_name    = "prod.db.${var.domain}"
  dw_dns_name         = "dw.${var.domain}"
  dwa_dns_name        = "redshift.dw.${var.domain}"
}

module "storage" {
  source = "./storage"

  deploy_name     = "${var.deploy_name}"
  deploy_stack    = "${var.deploy_stack}"
  deploy_prefix   = "${var.deploy_prefix}"
}


##############################
# Outputs

# For shared VPC/Subnets/NAT GW in kops.

output "vpc_id" {
  value = "${module.core.vpc_id}"
}

output "vpc_cidr" {
  value = "${module.core.vpc_cidr}"
}

output "natgw1_id" {
  value = "${module.core.natgw1_id}"
}

output "natgw2_id" {
  value = "${module.core.natgw2_id}"
}

output "natgw3_id" {
  value = "${module.core.natgw3_id}"
}

output "utility1_subnet_id" {
  value = "${module.core.utility1_subnet_id}"
}

output "utility2_subnet_id" {
  value = "${module.core.utility2_subnet_id}"
}

output "utility3_subnet_id" {
  value = "${module.core.utility3_subnet_id}"
}

output "k8s1_subnet_id" {
  value = "${module.core.k8s1_subnet_id}"
}

output "k8s2_subnet_id" {
  value = "${module.core.k8s2_subnet_id}"
}

output "k8s3_subnet_id" {
  value = "${module.core.k8s3_subnet_id}"
}


# DB

output "dev_db_ip" {
  value = "${module.db.dev_db_ip}"
}

output "prod_db_ip" {
  value = "${module.db.prod_db_ip}"
}

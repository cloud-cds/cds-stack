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

  public_key_path   = "${var.public_key_path}"
  private_key_path  = "${var.private_key_path}"

  domain_zone_id      = "${module.dns.zone_id}"
  controller_dns_name = "controller.${var.domain}"
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
  dev_db_name           = "${replace(var.deploy_prefix, "-", "_")}"
  dev_db_username       = "${var.dev_db_username}"
  dev_db_password       = "${var.dev_db_password}"

  prod_db_identifier    = "${var.deploy_prefix}-prod"
  prod_db_name          = "${replace(var.deploy_prefix, "-", "_")}"
  prod_db_username      = "${var.prod_db_username}"
  prod_db_password      = "${var.prod_db_password}"

  dw_identifier         = "${var.deploy_prefix}-dw"
  dw_name               = "${replace(var.deploy_prefix, "-", "_")}_dw"
  dw_username           = "${var.dw_username}"
  dw_password           = "${var.dw_password}"

  db_subnet1_cidr       = "${var.db_subnet1_cidr}"
  db_availability_zone1 = "${var.db_availability_zone1}"
  db_subnet2_cidr       = "${var.db_subnet2_cidr}"
  db_availability_zone2 = "${var.db_availability_zone2}"

  domain_zone_id      = "${module.dns.zone_id}"
  dev_db_dns_name     = "dev.db.${var.domain}"
  prod_db_dns_name    = "prod.db.${var.domain}"
  dw_dns_name         = "dw.${var.domain}"
}

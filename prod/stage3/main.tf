# Specify the provider and access details
provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region     = "${var.aws_region}"
  profile    = "opsdx"
}

data "aws_route53_zone" "main" {
  name = "${var.domain}."
}

module "web" {
  source        = "./services/web"
  deploy_name   = "${var.deploy_name}"
  deploy_stack  = "${var.deploy_stack}"
  deploy_prefix = "${var.deploy_prefix}"
  aws_id        = "${var.aws_id}"
  aws_region    = "${var.aws_region}"

  local_shell       = "${var.local_shell}"
  domain_zone_id    = "${data.aws_route53_zone.main.zone_id}"
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
  db_name             = "${replace(var.deploy_prefix, "-", "_")}"
  db_username         = "${var.db_username}"
  db_password         = "${var.db_password}"
  jhapi_client_id     = "${var.jhapi_client_id}"
  jhapi_client_secret = "${var.jhapi_client_secret}"
}
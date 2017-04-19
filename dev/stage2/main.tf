# Specify the provider and access details
provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region     = "${var.aws_region}"
  profile    = "opsdx"
}

module "k8s" {
  source = "./k8s"
  dummy_file = "foo"
  node_sg_id = ""
  controller_sg_id = ""
}

module "k8s_ml" {
  source = "./k8s-ml"
}

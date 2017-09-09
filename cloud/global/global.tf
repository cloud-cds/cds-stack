terraform {
  backend "s3" {
    encrypt = "true"
    bucket  = "opsdx-terraform"
    key     = "terraform_state/opsdx-common"
    region  = "us-east-1"
  }
}

provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region     = "${var.aws_region}"
  profile    = "opsdx"
}

module "dns" {
  source = "./dns"
  domain = "opsdx.io"

  spf           = "${var.spf}"
  dkim          = "${var.dkim}"
  dkim_selector = "${var.dkim_selector}"
}

module "storage" {
  source = "./storage"
  aws_id = "${var.aws_id}"
}

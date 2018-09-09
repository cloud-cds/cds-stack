terraform {
  backend "s3" {
    encrypt = "true"
    bucket  = "mc-terraform"
    key     = "terraform_state/mc"
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

# Lambda roles and policies.
module "behavior_monitor" {
  source = "./lambda/behavior-monitor"
}

module "clarity_etl_launcher" {
  source = "./lambda/clarity-etl-launcher"
}

module "k8s_launcher" {
  source = "./lambda/k8s-job-launcher"
}

module "k8s_weave_cleaner" {
  source = "./lambda/k8s-weave-cleaner"
}

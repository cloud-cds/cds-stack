variable "deploy_name" {}
variable "deploy_stack" {}
variable "deploy_prefix" {}

######################################
# Deployment

variable "aws_region" {
  description = "AWS region to launch servers."
  default = "us-east-1"
}

variable "az1" {}
variable "az2" {}
variable "az3" {}


# Official Ubuntu 14.04 AMI, from which we make encrypted copies.
variable "aws_base_ami" {
  default = {
    us-east-1 = "ami-d90d92ce"
  }
}


######################################
# DNS

variable "domain_zone_id" {
  description = "OpsDX domain name"
}


######################################
# PKI

variable "public_key_path" {
  description = "OpsDX public key path"
}

variable "private_key_path" {
  description = "OpsDX private key path"
}


#########################################
# Controller instance.

variable "controller_dns_name" {
  description = "OpsDX controller instance dns name"
}

#########################################
# Windows instance.
variable "windows_username" { default = "admin" }
variable "windows_password" { }

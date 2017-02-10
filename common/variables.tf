variable "deploy_name" {
  description = "Name Tag for AWS"
}

variable "deploy_stack" {
  description = "Stack Tag for AWS"
}

variable "deploy_prefix" {
  description = "AWS Resource Name Prefix for Deployment"
}

######################################
# Deployment

variable "aws_region" {
  description = "AWS region to launch servers."
  default = "us-east-1"
}

# Official Ubuntu 14.04 AMI, from which we make encrypted copies.
variable "aws_base_ami" {
  default = {
    us-east-1 = "ami-d90d92ce"
  }
}

variable "aws_amis" {
  default = {
    us-east-1 = "ami-63d7e709"
  }
}

######################################
# PKI

variable "key_name" {
  description = "OpsDX AWS key pair name"
}

variable "public_key_path" {
  description = "OpsDX public key path"
  default = "keys/tf-opsdx.pub"
}


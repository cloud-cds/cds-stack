##################################
# AWS account

variable "aws_id" {
  description = "AWS account ID"
}

variable "access_key" {
  description = "AWS access key ID"
}

variable "secret_key" {
  description = "AWS secret key"
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

variable "private_key_path" {
  description = "OpsDX private key path"
}


#########################################
# Controller instance.

variable "domain_zone_id" {
  description = "OpsDX domain name"
}

variable "controller_dns_name" {
  description = "OpsDX controller instance dns name"
}

# redash.io instance
variable "redash_dns_name" {
  description = "redash.io instance dns name"
}


# TREWS service
variable "trews_dns_name" {
  description = "OpsDX TREWS Rest API and Web Frontend instance dns name"
}

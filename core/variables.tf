variable "deploy_name" {
  description = "Name Tag for AWS"
}

variable "deploy_stack" {
  description = "Stack Tag for AWS"
}

######################################
# PKI

variable "private_key_path" {
  description = "OpsDX private key path"
}

variable "auth_key" {
  description = "AWS Key ID for local machine ssh keys"
}

#########################################
# Controller instance.

variable "controller_ami" {
  description = "OpsDX Controller OS AMI"
}

variable "domain_zone_id" {
  description = "OpsDX domain name"
}

variable "controller_dns_name" {
  description = "OpsDX controller instance dns name"
}

# TREWS service
variable "trews_dns_name" {
  description = "OpsDX TREWS Rest API and Web Frontend instance dns name"
}

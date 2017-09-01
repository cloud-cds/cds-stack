######################################
# Deployment

variable "deploy_name" {
  description = "Name Tag for AWS deployments"
}

variable "deploy_stack" {
  description = "Stack Tag for AWS deployments"
}

variable "deploy_prefix" {
  description = "AWS Resource Name Prefix for Deployment"
}

variable "aws_region" {
  description = "AWS region to launch servers."
  default = "us-east-1"
}


######################################
# PKI

variable "public_key_path" {
  description = "OpsDX public key path"
}

variable "private_key_path" {
  description = "OpsDX private key path"
}


########################################
# Auditing & logging.

variable "audit_sns_protocol" {
  description = "Protocol for receiving audit log ready notifications"
  default = "https"
}

variable "audit_sns_endpoint" {
  description = "Endpoint for receiving audit log ready notifications"
}


##################################
# DNS variables

variable "root_domain" {
  description = "Cluster root domain name"
}

variable "domain" {
  description = "Cluster domain name"
}

#######################
# DB

variable "dev_db_username" {
  description = "DB admin account"
}

variable "dev_db_password" {
  description = "DB Password"
}

variable "prod_db_username" {
  description = "DB admin account"
}

variable "prod_db_password" {
  description = "DB Password"
}

variable "dw_username" {
  description = "DW admin account"
}

variable "dw_password" {
  description = "DW Password"
}

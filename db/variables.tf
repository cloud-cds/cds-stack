variable "deploy_name" {
  description = "Name Tag for AWS deployment"
}

variable "deploy_stack" {
  description = "Stack Tag for AWS deployment"
}

################
# AWS Provider
variable "vpc_id" {}

data "aws_vpc" "prod" {
  id = "${var.vpc_id}"
}

######################################
# RDS DB

variable "db_identifier" {
  description = "Identifier for your DB"
  default = "opsdx-prod"
}

variable "db_storage" {
  description = "Storage size in GB"
  default = "30"
}

variable "db_engine" {
  description = "Postgres-backed DB Engine"
  default = "postgres"
}

variable "db_engine_version" {
  description = "Engine version"
  default = {
    postgres = "9.5.2"
  }
}

variable "db_instance_class" {
  default = "db.t2.large"
  description = "Instance class"
}

variable "db_name_prod" {
  default = "opsdx_prod"
  description = "Production DB name"
}

variable "db_username" {
  default = "opsdx_root"
  description = "User name"
}

variable "db_password" {
  description = "DB Password"
}

#######################
# DB Networking

variable "db_subnet1_cidr" {
  description = "Multi-AZ DB Subnet CIDR block 1"
  default = "10.0.128.0/24"
}

variable "db_availability_zone1" {
  description = "Multi-AZ DB zone 1"
  default = "us-east-1b"
}

variable "db_subnet2_cidr" {
  description = "Multi-AZ DB Subnet CIDR block 2"
  default = "10.0.129.0/24"
}

variable "db_availability_zone2" {
  description = "Multi-AZ DB zone 2"
  default = "us-east-1c"
}


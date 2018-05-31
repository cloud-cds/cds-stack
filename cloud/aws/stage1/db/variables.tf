variable "deploy_name" {}
variable "deploy_stack" {}
variable "deploy_prefix" {}

################
# AWS Provider
variable "vpc_id" {}

data "aws_vpc" "main" {
  id = "${var.vpc_id}"
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

######################
# DNS

variable "domain_zone_id" {}
variable "dev_db_dns_name" {}
variable "prod_db_dns_name" {}
variable "dw_dns_name" {}
variable "dwa_dns_name" {}

######################################
# Common DB parameters

variable "db_storage" {
  description = "Storage size in GB"
  default = "132"
}

variable "db_storage_type" {
  description = "RDS Storage type (e.g., SSD, magnetic)"
  default = "gp2"
}

variable "db_engine" {
  description = "Postgres-backed DB Engine"
  default = "postgres"
}

variable "db_engine_version" {
  description = "Engine version"
  default = {
    postgres = "9.6.2"
  }
}

variable "db_instance_class" {
  default = "db.t2.large"
  description = "Instance class"
}

variable "db_parameter_group" {
  description = "Postgres Parameter Group"
}

#############################################
# Dev-specific DB parameters

variable "dev_db_identifier" {
  description = "RDS Resource Identifier"
}

variable "dev_db_name" {
  description = "Database name"
}

variable "dev_db_username" {
  description = "User name"
}

variable "dev_db_password" {
  description = "Password"
}

# Restoring from backups.
variable "dev_db_snapshot_id" {
  description = "RDS Snapshot Identifier for Restoration"
  default = ""
}


#############################################
# Prod-specific DB parameters

variable "prod_db_identifier" {
  description = "RDS Resource Identifier"
}

variable "prod_db_name" {
  description = "Database name"
}

variable "prod_db_username" {
  description = "User name"
}

variable "prod_db_password" {
  description = "Password"
}

# Restoring from backups.
variable "prod_db_snapshot_id" {
  description = "RDS Snapshot Identifier for Restoration"
  default = ""
}


######################################
# RDS DW

variable "dw_identifier" {
  description = "RDS DW Resource Identifier"
}

variable "dw_name" {
  description = "DW name"
}

variable "dw_username" {
  description = "DW Username"
}

variable "dw_password" {
  description = "DW Password"
}

variable "dw_storage" {
  description = "Storage size in GB"
  default = "900"
}

variable "dw_storage_type" {
  description = "RDS Storage type (e.g., SSD, magnetic)"
  default = "gp2"
}

variable "dw_engine" {
  description = "Postgres-backed DB Engine"
  default = "postgres"
}

variable "dw_engine_version" {
  description = "Engine version"
  default = {
    postgres = "9.6.2"
  }
}

variable "dw_instance_class" {
  default = "db.m4.2xlarge"
  description = "Instance class"
}

variable "dw_parameter_group" {
  description = "DW Parameter Group"
}

variable "dw_snapshot_id" {
  description = "DW Snapshot Identifier for Restoration"
  default = ""
}


######################################
# Redshift DW

variable "dwa_node_type" { default = "dc1.large" }
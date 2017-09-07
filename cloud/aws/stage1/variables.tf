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

# Official Ubuntu 14.04 AMI, from which we make encrypted copies.
variable "aws_base_ami" {
  default = {
    us-east-1 = "ami-d90d92ce"
  }
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

variable "dev_db_snapshot_id" {
  description = "RDS Snapshot Identifier for Restoration"
  default = ""
}

variable "dev_db_snapshot_dbname" {
  description = "Database name in the snapshot"
  default = ""
}

variable "prod_db_snapshot_id" {
  description = "RDS Snapshot Identifier for Restoration"
  default = ""
}

variable "prod_db_snapshot_dbname" {
  description = "Database name in the snapshot"
  default = ""
}

variable "dw_snapshot_id" {
  description = "DW Snapshot Identifier for Restoration"
  default = ""
}

variable "dw_snapshot_dbname" {
  description = "Database name in the snapshot"
  default = ""
}


#######################
# DB networking

variable "db_subnet1_cidr" {
  description = "Multi-AZ DB Subnet CIDR block 1"
  default = "10.0.249.0/24"
}

variable "db_availability_zone1" {
  description = "Multi-AZ DB zone 1"
  default = "us-east-1a"
}

variable "db_subnet2_cidr" {
  description = "Multi-AZ DB Subnet CIDR block 2"
  default = "10.0.250.0/24"
}

variable "db_availability_zone2" {
  description = "Multi-AZ DB zone 2"
  default = "us-east-1c"
}

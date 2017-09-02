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

# Restoring from backups.
variable "dev_db_snapshot_id" {
  description = "RDS Snapshot Identifier for Restoration"
  default = ""
}

variable "dev_db_snapshot_dbname" {
  description = "Database name in the snapshot"
  default = ""
}

# Restoring from backups.
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

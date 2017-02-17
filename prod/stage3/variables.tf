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

variable "public_key_path" {
  description = "OpsDX public key path"
}

variable "private_key_path" {
  description = "OpsDX private key path"
}


########################################
# Emails.
variable "admin_email" {
  description = "System administrator email"
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

variable "db_username" {
  description = "DB admin account"
}

variable "db_password" {
  description = "DB Password"
}

variable "db_subnet1_cidr" {
  description = "Multi-AZ DB Subnet CIDR block 1"
  default = "10.0.128.0/24"
}

variable "db_availability_zone1" {
  description = "Multi-AZ DB zone 1"
  default = "us-east-1a"
}

variable "db_subnet2_cidr" {
  description = "Multi-AZ DB Subnet CIDR block 2"
  default = "10.0.129.0/24"
}

variable "db_availability_zone2" {
  description = "Multi-AZ DB zone 2"
  default = "us-east-1c"
}

###################################
# K8s

variable "k8s_server_host" {
  description = "Kubernetes master host"
}

variable "k8s_server_port" {
  description = "Kubernetes master port"
}

variable "k8s_name" {
  description = "Kubernetes context name"
}

variable "k8s_server" {
  description = "Kubernetes master dns name"
}

variable "k8s_user" {
  description = "Kubernetes username"
}

variable "k8s_pass" {
  description = "Kubernetes password"
}

variable "k8s_cert_auth" {
  description = "Kubernetes certificate auth data"
}

variable "k8s_cert" {
  description = "Kubernetes client certificate data"
}

variable "k8s_key" {
  description = "Kubernetes client key data"
}

variable "k8s_token" {
  description = "Kubernetes service account token"
}

####################################
# JH API

variable "jhapi_client_id" {
  description = "EPIC JHAPI Login"
}

variable "jhapi_client_secret" {
  description = "EPIC JHAPI Secret"
}

####################################
# Files

variable "aws_trews_etl_package" {
  description = "AWS Lambda deployment package"
}

####################################
# Command execution

variable "local_shell" {
  description = "Run a local bash shell (for Windows/MSYS2)"
}

####################################
# Trews ETL parameters

variable "TREWS_ETL_SERVER" {}
variable "TREWS_ETL_HOSPITAL" {}
variable "TREWS_ETL_HOURS" {}

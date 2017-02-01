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

variable "k8s_domain" {
  description = "k8s cluster domain"
}

#######################
# DB

variable "db_password" {
  description = "DB Password"
}

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

####################################
# Files

variable "aws_trews_etl_package" {
  description = "AWS Lambda deployment package"
  default = "services/trews_etl/aws_lambda/2017-02-01-010038-trews-etl-lambda.zip"
}

####################################
# Command execution

variable "local_shell" {
  description = "Run a local bash shell (for Windows/MSYS2)"
}
